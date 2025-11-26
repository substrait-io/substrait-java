package io.substrait.spark

import io.substrait.spark.utils.{Dialect, DialectGenerator}
import io.substrait.spark.utils.DialectGenerator.schemaPath

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}

import java.io.{File, FileInputStream}

import scala.io.Source

class DialectSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {
  private val dialectPath = "spark_dialect.yaml"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("validate published dialect") {
    val jsonSchemaFactory = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)

    val schema =
      jsonSchemaFactory.getSchema(new FileInputStream(new File(schemaPath)), InputFormat.YAML)
    val dialect = Source.fromFile(dialectPath).mkString
    val errors = schema.validate(dialect, InputFormat.YAML)
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
    val mapper = new ObjectMapper(new YAMLFactory()).registerModules(DefaultScalaModule)

    val genDialect = DialectGenerator.generate()
    val publishedDialect =
      mapper.readValue(new File(dialectPath), scala.reflect.classTag[Dialect].runtimeClass)
    // The following will fail if the generated dialect differs from the published one.
    // If this is caused by an intentional change, the published dialect should be regenerated using:
    // `./gradlew dialect`
    assertResult(publishedDialect)(genDialect)
  }

}
