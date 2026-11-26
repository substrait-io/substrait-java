package io.substrait.spark.utils

import io.substrait.spark.SparkExtension
import io.substrait.spark.expression.{Enum, FunctionConverter, ToAggregateFunction, ToScalarFunction, ToWindowFunction, UnsupportedCallException}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, Literal}
import org.apache.spark.sql.types.{ByteType, DateType, DayTimeIntervalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType, YearMonthIntervalType}

import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import io.substrait.dialect.{DdlWriteType, Dialect, DialectFunction, ExpressionKind, JoinType, Notation, ReadType, RelationKind, SetOperation, SubqueryType, SupportedExpression, SupportedRelation, SupportedType, SystemFunctionMetadata, SystemTypeMetadata, TypeKind, Variadic}
import io.substrait.extension.SimpleExtension

import java.io.{InputStream, OutputStreamWriter}
import java.nio.file.{Files, Path}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Exists so `dependencies` can be derived from exactly the extensions the functions reference. */
private case class SourcedFunction(urn: String, function: DialectFunction)

/**
 * Generates the Substrait dialect describing what this integration supports.
 *
 * The function collections are the ones the runtime converters bind against: a dialect derived from
 * a wider collection would advertise functions that then fail to bind.
 *
 * Enum arguments are the one place a listed implementation can still fail to convert. A dialect
 * cannot restrict an enum argument's values, so an implementation with one is listed if Spark
 * converts a call with any of its options: extract is listed although Spark converts only some of
 * its components.
 */
class DialectGenerator(
    scalarFunctions: Seq[SimpleExtension.ScalarFunctionVariant],
    aggregateFunctions: Seq[SimpleExtension.AggregateFunctionVariant],
    windowFunctions: Seq[SimpleExtension.WindowFunctionVariant]) {

  // The dialect schema ships on the classpath in the substrait-packaging extensions artifact.
  val schemaResource = "/substrait/text/dialect_schema.yaml"

  def schemaStream(): InputStream = {
    val stream = getClass.getResourceAsStream(schemaResource)
    if (stream == null) {
      throw new IllegalStateException(s"Dialect schema not found on classpath: $schemaResource")
    }
    stream
  }

  def generate(): Dialect = {
    val scalars = supportedFunctions(scalarFunctions, ToScalarFunction(scalarFunctions))
    val aggregates = supportedFunctions(aggregateFunctions, ToAggregateFunction(aggregateFunctions))
    val windows = supportedFunctions(windowFunctions, ToWindowFunction(windowFunctions))

    val builder = Dialect
      .builder()
      .name("Spark Dialect")
      .addAllSupportedTypes(supportedTypes().asJava)
      .addAllSupportedExpressions(supportedExpressions().asJava)
      .addAllSupportedRelations(supportedRelations().asJava)
      .addAllSupportedScalarFunctions(sortedFunctions(scalars).asJava)
      .addAllSupportedAggregateFunctions(sortedFunctions(aggregates).asJava)
      .addAllSupportedWindowFunctions(sortedFunctions(windows).asJava)

    // The builder keeps insertion order, so feeding it a SortedMap emits the dependencies in
    // alias order rather than in an order that depends on how a Scala version hashes Strings.
    dependencies(scalars ++ aggregates ++ windows).foreach {
      case (alias, urn) => builder.putDependencies(alias, urn)
    }

    builder.build()
  }

  def generateYaml(): String = {
    val yaml = Dialect.toYaml(generate())

    // Validate against the substrait dialect schema
    val jsonSchemaFactory = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)

    val schema =
      jsonSchemaFactory.getSchema(schemaStream(), InputFormat.YAML)
    val errors = schema.validate(yaml, InputFormat.YAML)
    if (!errors.isEmpty) {
      throw new Exception(errors.toString)
    }
    yaml
  }

  /**
   * The alias a dialect function's `source` refers to. Derived rather than looked up, so an
   * extension the generator has not seen before cannot produce an empty `source` that dangles
   * against the `dependencies` block.
   */
  private[spark] def dependencyAlias(urn: String): String =
    urn.substring(urn.lastIndexOf(':') + 1).stripPrefix("functions_")

  /**
   * The `dependencies` block, keyed by alias and built from exactly the extensions the emitted
   * functions reference, so no unreferenced alias is published. A `source` on anything other than a
   * function -- a `USER_DEFINED` supported type is the only other place the schema allows one --
   * would have to be folded in here too.
   */
  private def dependencies(functions: Seq[SourcedFunction]): SortedMap[String, String] =
    functions.map(_.urn).distinct.sorted.foldLeft(SortedMap.empty[String, String]) {
      (deps, urn) =>
        val alias = dependencyAlias(urn)
        deps.get(alias) match {
          case Some(other) if other != urn =>
            throw new IllegalStateException(
              s"Dependency alias '$alias' is claimed by both '$other' and '$urn'")
          case _ => deps + (alias -> urn)
        }
    }

  // Ordered by source and name, with the implementations as a tie-breaker, so that the section does
  // not inherit the hash order the entries were grouped in. Two entries share a source and name only
  // when the function's implementations accept different numbers of arguments, and then each lists
  // different implementations, so no two entries tie.
  private def sortedFunctions(functions: Seq[SourcedFunction]): Seq[DialectFunction] =
    functions
      .map(_.function)
      .sortBy(f => (f.source(), f.name(), f.supportedImpls().asScala.mkString(",")))

  private def supportedTypes(): Seq[SupportedType] = {
    Seq(
      supportedType(TypeKind.I8, "ByteType"),
      supportedType(TypeKind.I16, "ShortType"),
      supportedType(TypeKind.I32, "IntegerType"),
      supportedType(TypeKind.I64, "LongType"),
      supportedType(TypeKind.FP32, "FloatType"),
      supportedType(TypeKind.FP64, "DoubleType"),
      supportedType(TypeKind.DECIMAL, "DecimalType"),
      supportedType(TypeKind.DATE, "DateType"),
      supportedType(TypeKind.STRING, "StringType"),
      supportedType(TypeKind.VARCHAR, "StringType"),
      supportedType(TypeKind.FIXED_CHAR, "StringType"),
      supportedType(TypeKind.BINARY, "BinaryType"),
      supportedType(TypeKind.BOOL, "BooleanType"),
      // Spark stores these as microseconds and carries no precision on the type itself, so the
      // maximum it supports is the microsecond one. Reading this from Util keeps the declaration
      // and the conversion guard from drifting apart.
      supportedType(
        TypeKind.PRECISION_TIMESTAMP,
        "TimestampNTZType",
        Some(Util.MICROSECOND_PRECISION)),
      supportedType(
        TypeKind.PRECISION_TIMESTAMP_TZ,
        "TimestampType",
        Some(Util.MICROSECOND_PRECISION)),
      supportedType(TypeKind.INTERVAL_DAY, "DayTimeIntervalType", Some(Util.MICROSECOND_PRECISION)),
      supportedType(TypeKind.INTERVAL_YEAR, "YearMonthIntervalType"),
      supportedType(TypeKind.LIST, "ArrayType"),
      supportedType(TypeKind.MAP, "MapType"),
      supportedType(TypeKind.STRUCT, "StructType")
    )
  }

  private def supportedType(
      kind: TypeKind,
      sparkType: String,
      maxPrecision: Option[Int] = None): SupportedType = {
    val builder = SupportedType
      .builder()
      .`type`(kind)
      .systemMetadata(SystemTypeMetadata.builder().name(sparkType).supportedAsColumn(true).build())
    maxPrecision.foreach(precision => builder.maxPrecision(precision))
    builder.build()
  }

  private def supportedExpressions(): Seq[SupportedExpression] = {
    Seq(
      SupportedExpression.of(ExpressionKind.LITERAL),
      SupportedExpression.of(ExpressionKind.SELECTION),
      SupportedExpression.of(ExpressionKind.SCALAR_FUNCTION),
      SupportedExpression.of(ExpressionKind.IF_THEN),
      SupportedExpression.of(ExpressionKind.SINGULAR_OR_LIST),
      SupportedExpression.of(ExpressionKind.CAST),
      SupportedExpression
        .builder()
        .expression(ExpressionKind.SUBQUERY)
        .addSubqueryTypes(SubqueryType.SCALAR, SubqueryType.IN_PREDICATE)
        .build()
    )
  }

  private def supportedRelations(): Seq[SupportedRelation] = {
    Seq(
      SupportedRelation.of(RelationKind.FILTER),
      SupportedRelation.of(RelationKind.FETCH),
      SupportedRelation.of(RelationKind.AGGREGATE),
      SupportedRelation.of(RelationKind.SORT),
      SupportedRelation.of(RelationKind.PROJECT),
      SupportedRelation.of(RelationKind.CROSS),
      SupportedRelation.of(RelationKind.UPDATE),
      SupportedRelation.of(RelationKind.CONSISTENT_PARTITION_WINDOW),
      SupportedRelation.of(RelationKind.EXPAND),
      SupportedRelation.of(RelationKind.WRITE),
      SupportedRelation
        .builder()
        .relation(RelationKind.READ)
        .addReadTypes(ReadType.VIRTUAL_TABLE, ReadType.LOCAL_FILES, ReadType.NAMED_TABLE)
        .build(),
      SupportedRelation
        .builder()
        .relation(RelationKind.DDL)
        .addDdlWriteTypes(DdlWriteType.NAMED_OBJECT)
        .build(),
      SupportedRelation
        .builder()
        .relation(RelationKind.JOIN)
        .addJoinTypes(
          JoinType.INNER,
          JoinType.OUTER,
          JoinType.LEFT,
          JoinType.RIGHT,
          JoinType.LEFT_SEMI,
          JoinType.LEFT_ANTI)
        .build(),
      SupportedRelation
        .builder()
        .relation(RelationKind.SET)
        .addOperations(SetOperation.UNION_ALL)
        .build()
    )
  }

  // The supported functions sections are generated from the converters the runtime builds calls
  // with. An implementation is listed if its converter builds a call for it -- the call the runtime
  // makes -- that Spark accepts.
  private def supportedFunctions(
      functions: Seq[SimpleExtension.Function],
      converter: FunctionConverter[_, _]): Seq[SourcedFunction] = {
    val probes = functions.flatMap(probe(converter))
    // One entry for each function, extension and range of argument counts.
    probes
      .groupBy(p => (p.variant.urn(), p.variant.name(), p.counts))
      .toSeq
      .map {
        case ((urn, name, counts), group) =>
          val (sqlName, notation) = systemName(converter, name, group.flatMap(_.calls))
          val builder = DialectFunction
            .builder()
            .source(dependencyAlias(urn))
            .name(name)
            .systemMetadata(
              SystemFunctionMetadata.builder().name(sqlName).notation(notation).build())
            .addAllSupportedImpls(group.map(_.signature).distinct.sorted.asJava)
          counts.foreach(c => builder.variadic(c.toVariadic))
          SourcedFunction(urn, builder.build())
      }
  }

  /**
   * The function's name in Spark, from the calls the converter built that are of a Spark expression
   * one of its signatures names. A function that several Spark expressions implement, chosen by its
   * arguments as extract's is by its component, goes by its own name.
   */
  private def systemName(
      converter: FunctionConverter[_, _],
      name: String,
      calls: Seq[Expression]): (String, Notation) = {
    val classes = converter.getSigs.filter(_.name == name).map(_.expClass)
    val names = calls
      .filter(call => classes.exists(_.isInstance(call)))
      .map {
        case bo: BinaryOperator => (bo.sqlOperator, Notation.INFIX)
        case e => (e.prettyName, Notation.FUNCTION)
      }
      .distinct
    if (names.size == 1) names.head else (name, Notation.FUNCTION)
  }

  /** An implementation that the runtime converter can build calls for, and the calls it built. */
  private case class Probe(
      variant: SimpleExtension.Function,
      calls: Seq[Expression],
      counts: Option[ArgumentCounts]) {
    def signature: String = variant.key().split(":", 2).apply(1)
  }

  /**
   * The numbers of arguments that a variadic implementation accepts: `max` empty for any number.
   * The dialect schema counts every argument ("Minimum number of arguments required"), while a
   * declaration's variadic behaviour counts repetitions of its last argument only, so the two
   * differ when a function has arguments before the repeated one.
   */
  private case class ArgumentCounts(min: Int, max: Option[Int]) {
    def toVariadic: Variadic = {
      val builder = Variadic.builder()
      // The schema's minimum is at least 1, so a function that accepts none leaves it out.
      if (min > 0) {
        builder.min(min)
      }
      max.foreach(m => builder.max(m))
      builder.build()
    }
  }

  /**
   * How many numbers of arguments beyond the least it accepts a variadic implementation with no
   * declared maximum must accept to be taken as accepting any number. Spark builds a function from
   * either a fixed set of argument counts or a list of any length, and its fixed counts are all
   * well below this.
   */
  private val probedRepeats = 32

  /**
   * The implementation, if the runtime converter can build a call for it. A variadic one is tried
   * from the least number of arguments its declaration allows to find the least Spark accepts, then
   * with more: with each number up to its declared maximum if it has one, and otherwise with each
   * of the next [[probedRepeats]]. It is listed with the range Spark accepts, or as unbounded if it
   * has no declared maximum and Spark accepts every number tried.
   */
  private def probe(converter: FunctionConverter[_, _])(
      variant: SimpleExtension.Function): Option[Probe] = {
    val declared = variant.args().asScala.toSeq
    val built = mutable.Map.empty[Int, Seq[Expression]]
    def accepted(n: Int): Seq[Expression] =
      built.getOrElseUpdate(n, calls(converter, variant, repeated(declared, n)))

    if (!variant.variadic().isPresent) {
      Some(accepted(declared.size)).filter(_.nonEmpty).map(Probe(variant, _, None))
    } else {
      val behavior = variant.variadic().get()
      // Only the last argument repeats.
      val fixed = declared.size - 1
      val lowest = fixed + behavior.getMin
      val declaredMax =
        if (behavior.getMax.isPresent) Some(fixed + behavior.getMax.getAsInt) else None
      (lowest to declaredMax.getOrElse(lowest + probedRepeats)).find(accepted(_).nonEmpty).flatMap {
        min =>
          val top = declaredMax.getOrElse(min + probedRepeats)
          val counts = (min to top).takeWhile(accepted(_).nonEmpty)
          val max = if (declaredMax.isEmpty && counts.last == top) None else Some(counts.last)
          val calls = counts.flatMap(accepted)
          if (max.contains(0)) {
            // Spark accepts it with no arguments only, which a dialect cannot say: max is at least 1.
            None
          } else if (min == declared.size && max.contains(declared.size)) {
            Some(Probe(variant, calls, None))
          } else {
            Some(Probe(variant, calls, Some(ArgumentCounts(min, max))))
          }
      }
    }
  }

  /** The declared arguments with the last repeated, or left out, to make `n` of them. */
  private def repeated(
      declared: Seq[SimpleExtension.Argument],
      n: Int): Seq[SimpleExtension.Argument] =
    if (n <= declared.size) declared.take(n)
    else declared ++ Seq.fill(n - declared.size)(declared.last)

  /**
   * The calls that the converter builds with sample arguments of the declared types and that Spark
   * accepts. A signature says only that an argument is an enum, so an enum argument is tried with
   * each of the options its declaration gives. The option has to be consumed in building the call:
   * an [[Enum]] left in it is a placeholder that Spark cannot evaluate.
   */
  private def calls(
      converter: FunctionConverter[_, _],
      variant: SimpleExtension.Function,
      args: Seq[SimpleExtension.Argument]): Seq[Expression] = {
    val choices: Seq[Seq[Expression]] = args.map {
      case enum: SimpleExtension.EnumArgument => enum.options().asScala.toSeq.map(Enum(_))
      case value: SimpleExtension.ValueArgument => argValue(value.toTypeString()).toSeq
      // A type argument has no sample value.
      case _ => Seq.empty
    }
    combinations(choices).flatMap {
      sample =>
        unlessRejected(converter.getSparkExpressionFromSubstraitFunc(variant.key(), sample)).flatten
          .filter(call => call.find(_.isInstanceOf[Enum]).isEmpty)
          .filter(call => unlessRejected(call.checkInputDataTypes().isSuccess).getOrElse(false))
    }
  }

  /**
   * The result, or none if Spark rejects the call by throwing: an AnalysisException, as for the
   * wrong number of arguments, or an UnsupportedCallException, from a signature that cannot build
   * it. Any other exception is a fault, and fails the generator rather than leave the function out.
   */
  private def unlessRejected[A](attempt: => A): Option[A] =
    try Some(attempt)
    catch { case _: AnalysisException | _: UnsupportedCallException => None }

  private def combinations(choices: Seq[Seq[Expression]]): Seq[Seq[Expression]] =
    choices.foldRight(Seq(Seq.empty[Expression])) {
      (options, rest) => for (option <- options; tail <- rest) yield option +: tail
    }

  // Generate a type-appropriate sample value, or None when the Substrait type has no
  // Spark equivalent to build a literal from.
  private def argValue(argType: String): Option[Literal] = {
    argType match {
      case "i8" => Some(Literal(Byte.MaxValue, ByteType))
      case "i16" => Some(Literal(Short.MaxValue, ShortType))
      case "i32" => Some(Literal(Integer.MAX_VALUE, IntegerType))
      case "i64" => Some(Literal(Long.MaxValue, LongType))
      case "fp32" => Some(Literal(Float.MaxValue, FloatType))
      case "fp64" => Some(Literal(Double.MaxValue, DoubleType))
      case "dec" => Some(Literal(BigDecimal(1)))
      case "str" => Some(Literal("str"))
      case "vchar" => Some(Literal("str"))
      case "fchar" => Some(Literal("str"))
      case "any" => Some(Literal("any")) // can be any literal type - use string
      case "bool" => Some(Literal(true))
      case "date" => Some(Literal(0, DateType))
      case "ts" => Some(Literal(0L, TimestampNTZType))
      case "tstz" => Some(Literal(0L, TimestampType))
      case "pts" => Some(Literal(0L, TimestampNTZType))
      case "ptstz" => Some(Literal(0L, TimestampType))
      case "iyear" => Some(Literal(0, YearMonthIntervalType()))
      case "iday" => Some(Literal(0L, DayTimeIntervalType()))
      case _ => None
    }
  }
}

object DialectGenerator
  extends DialectGenerator(
    SparkExtension.SparkScalarFunctions,
    SparkExtension.StandardAggregateFunctions,
    SparkExtension.StandardWindowFunctions) {

  def main(args: Array[String]): Unit = {
    val yaml = generateYaml()

    args match {
      case Array(t) => Files.writeString(Path.of(t), yaml)
      case _ =>
        val out = new OutputStreamWriter(System.out)
        out.write(yaml)
        out.flush()
    }
  }
}
