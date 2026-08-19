package io.substrait.spark

import io.substrait.spark.utils.DialectGenerator

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import io.substrait.dialect.{Dialect, DialectFunction}
import io.substrait.extension.{DefaultExtensionCatalog, SimpleExtension}

import java.io.File

import scala.io.Source
import scala.jdk.CollectionConverters._

class DialectSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {
  private val dialectPath = "../spark_dialect.yaml"

  private lazy val published: String = {
    val source = Source.fromFile(dialectPath)
    try source.mkString
    finally source.close()
  }

  private def allFunctions(dialect: Dialect): Seq[DialectFunction] =
    dialect.supportedScalarFunctions().asScala.toSeq ++
      dialect.supportedAggregateFunctions().asScala.toSeq ++
      dialect.supportedWindowFunctions().asScala.toSeq

  /** A single-aggregate extension, used to drive the generator with an extension it cannot know. */
  private def sumExtension(urn: String): SimpleExtension.ExtensionCollection =
    SimpleExtension.load(s"""urn: $urn
                            |aggregate_functions:
                            |  - name: "sum"
                            |    impls:
                            |      - args:
                            |          - name: x
                            |            value: i64
                            |        nullability: DECLARED_OUTPUT
                            |        decomposable: MANY
                            |        intermediate: i64?
                            |        return: i64?
                            |""".stripMargin)

  private def generatorWith(extras: SimpleExtension.ExtensionCollection*): DialectGenerator =
    new DialectGenerator(
      SparkExtension.SparkScalarFunctions,
      SparkExtension.SparkAggregateFunctions ++
        extras.flatMap(_.aggregateFunctions().asScala.toSeq),
      SparkExtension.SparkWindowFunctions
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("validate published dialect") {
    val jsonSchemaFactory = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)

    val schema =
      jsonSchemaFactory.getSchema(DialectGenerator.schemaStream(), InputFormat.YAML)
    val errors = schema.validate(published, InputFormat.YAML)
    assertResult(java.util.List.of())(errors)
  }

  test("generate validated YAML") {
    val tempPathName = "build/tmp/test/dialect.yaml"
    val tempFile = new File(tempPathName)
    if (tempFile.exists()) {
      tempFile.delete()
    }
    DialectGenerator.main(Array(tempPathName))
    assertResult(true)(tempFile.exists())
  }

  test("compare generated dialect") {
    val genDialect = DialectGenerator.generate()
    val publishedDialect = Dialect.load(published)
    // The following will fail if the generated dialect differs from the published one.
    // If this is caused by an intentional change, the published dialect should be regenerated using:
    // `./gradlew dialect`
    assertResult(publishedDialect)(genDialect)
  }

  test("published dialect is byte-for-byte what the generator emits") {
    // Dialect equality is order-insensitive for `dependencies`, so comparing the models alone
    // lets the published file's key order drift. Compare the text as well.
    assertResult(published)(DialectGenerator.generateYaml())
  }

  test("dependency aliases are emitted in sorted order") {
    val aliases = DialectGenerator
      .generateYaml()
      .linesIterator
      .dropWhile(_ != "dependencies:")
      .drop(1)
      .takeWhile(_.startsWith("  "))
      .map(_.trim.takeWhile(_ != ':'))
      .toSeq
    assert(aliases.nonEmpty)
    assertResult(aliases.sorted)(aliases)
  }

  test("every source resolves to a declared dependency") {
    val dialect = DialectGenerator.generate()
    val aliases = dialect.dependencies().asScala.keySet
    val functions = allFunctions(dialect)
    assert(functions.nonEmpty)
    // The dialect schema declares `source` as a plain string, so a dangling alias validates
    // against it; nothing but this assertion ties the two sections together. Types are covered as
    // well as functions: `dependencies` is derived from the functions' URNs, so a USER_DEFINED type
    // pointing at an extension no function comes from would dangle.
    val sources = functions.map(_.source()) ++
      dialect.supportedTypes().asScala.toSeq.flatMap(t => Option(t.source().orElse(null)))
    assertResult(Seq.empty)(sources.distinct.filterNot(aliases.contains))
  }

  test("dependency aliases are derived from the extension URN") {
    assertResult("arithmetic")(
      DialectGenerator.dependencyAlias("extension:io.substrait:functions_arithmetic"))
    assertResult("spark")(DialectGenerator.dependencyAlias("extension:substrait:spark"))
    assertResult("extra")(
      DialectGenerator.dependencyAlias("extension:io.substrait:functions_extra"))
  }

  test("an extension the generator has not seen before is given an alias and a dependency") {
    val dialect = generatorWith(sumExtension("extension:io.substrait:functions_extra")).generate()

    assertResult(Some("extension:io.substrait:functions_extra"))(
      dialect.dependencies().asScala.get("extra"))
    assertResult(Seq(Seq("i64")))(
      dialect
        .supportedAggregateFunctions()
        .asScala
        .toSeq
        .filter(_.source() == "extra")
        .map(_.supportedImpls().asScala.toSeq))
  }

  test("two extensions claiming the same alias are rejected") {
    // Aliases are the URN's last segment, so two URNs can derive the same one. Failing loudly
    // beats dropping one of them from a `dependencies` block that is keyed by alias.
    val generator = generatorWith(
      sumExtension("extension:io.substrait:functions_extra"),
      sumExtension("extension:acme:functions_extra"))
    val error = intercept[IllegalStateException](generator.generate())
    assert(error.getMessage.contains("extra"))
  }

  test("no aggregate or window function outside the standard extensions is advertised") {
    // The runtime converters bind aggregates and windows against the standard extensions only, so
    // a dialect derived from a wider set -- the collection merged with spark.yml -- would advertise
    // functions that then fail with "Unable to find binding for call". Sourced from the catalog
    // rather than from SparkExtension so that rewiring the generator to the merged collection is
    // what this fails on. It cannot fail while spark.yml declares no aggregate or window function,
    // which is exactly the condition that makes the bug latent rather than live.
    val standard = DefaultExtensionCatalog.DEFAULT_COLLECTION
    val declared: Set[(String, String)] =
      (standard.aggregateFunctions().asScala.toSeq.map(f => (f.urn(), f.key())) ++
        standard.windowFunctions().asScala.toSeq.map(f => (f.urn(), f.key()))).toSet

    val dialect = DialectGenerator.generate()
    val urns = dialect.dependencies().asScala
    val advertised = (dialect.supportedAggregateFunctions().asScala.toSeq ++
      dialect.supportedWindowFunctions().asScala.toSeq).flatMap {
      f => f.supportedImpls().asScala.toSeq.map(impl => (urns(f.source()), s"${f.name()}:$impl"))
    }

    assert(advertised.nonEmpty)
    assertResult(Seq.empty)(advertised.filterNot(declared.contains))
  }
}
