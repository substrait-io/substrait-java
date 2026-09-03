package io.substrait.spark.utils

import io.substrait.spark.SparkExtension
import io.substrait.spark.expression.FunctionMappings.{AGGREGATE_SIGS, SCALAR_SIGS, WINDOW_SIGS}
import io.substrait.spark.expression.Sig

import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, Literal}
import org.apache.spark.sql.types.{ByteType, DateType, DayTimeIntervalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType, YearMonthIntervalType}

import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import io.substrait.dialect.{DdlWriteType, Dialect, DialectFunction, ExpressionKind, JoinType, Notation, ReadType, RelationKind, SetOperation, SubqueryType, SupportedExpression, SupportedRelation, SupportedType, SystemFunctionMetadata, SystemTypeMetadata, TypeKind}
import io.substrait.extension.SimpleExtension

import java.io.{File, FileWriter, InputStream, OutputStreamWriter}

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

/** Exists so `dependencies` can be derived from exactly the extensions the functions reference. */
private case class SourcedFunction(urn: String, function: DialectFunction)

/**
 * Generates the Substrait dialect describing what this integration supports.
 *
 * The function collections are the ones the runtime converters bind against: a dialect derived from
 * a wider collection would advertise functions that then fail to bind.
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
    val scalars = SCALAR_SIGS.flatMap(supportedFunctions(scalarFunctions))
    val aggregates = AGGREGATE_SIGS.flatMap(supportedFunctions(aggregateFunctions))
    val windows = WINDOW_SIGS.flatMap(supportedFunctions(windowFunctions))

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
    functions.map(_.urn).distinct.foldLeft(SortedMap.empty[String, String]) {
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
  // not inherit the hash order the URN groups were collected in. (source, name) is unique within a
  // Sig; a tie needs two Sigs of the same name matching the same extension, where the remaining
  // order is the SIGS list's.
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

  // The supported functions section is generated from the existing FunctionMappings code.
  private def supportedFunctions(functions: Seq[SimpleExtension.Function])(
      sig: Sig): Seq[SourcedFunction] = {
    val expr = if (classOf[Expression].isAssignableFrom(sig.expClass)) {
      val cons = sig.expClass.getDeclaredConstructors.minBy(c => c.getParameterCount)
      val i = cons.getParameterCount
      val inst = i match {
        case 0 => cons.newInstance()
        case 1 => cons.newInstance(null)
        case 2 => cons.newInstance(null, null)
        case 3 => cons.newInstance(null, null, null)
        case _ =>
          throw new UnsupportedOperationException(
            s"${sig.expClass} constructor requires $i parameters")
      }
      inst.asInstanceOf[Expression]
    } else {
      throw new UnsupportedOperationException(s"${sig.expClass} is not an Expression")
    }

    val sqlName = expr match {
      case bo: BinaryOperator => bo.sqlOperator
      case e => e.prettyName
    }

    val notation = expr match {
      case _: BinaryOperator => Notation.INFIX
      case _ => Notation.FUNCTION
    }

    // create a map of function parameter variants grouped by URN
    val variants = functions.filter(_.name() == sig.name)
    val groups: Map[String, Seq[String]] =
      variants
        .map {
          v =>
            {
              val signature = v.key().split(":", 2).apply(1)
              // generate sample arguments for this variant
              val args: Seq[Option[Literal]] = if (signature.isEmpty) {
                Seq.empty
              } else {
                signature.split("_").toSeq.map(argValue)
              }
              // A variant is only supported if every argument type has a Spark
              // equivalent. Probing an unmappable type with a placeholder literal
              // would let permissive type checks pass and report support that
              // Spark does not have.
              if (
                args.forall(_.isDefined) && expr.children != null
                && expr.children.size == args.size
                && expr.withNewChildren(args.flatten).checkInputDataTypes().isSuccess
              ) {
                (v.urn, signature)
              } else {
                ("FAILED", signature)
              }
            }
        }
        .groupBy(_._1) // group by URN
        .filter(_._1 != "FAILED")
        .view
        .map { case (k, v) => (k, v.map(_._2)) }
        .toMap
    groups.map {
      case (urn, sigs) =>
        SourcedFunction(
          urn,
          DialectFunction
            .builder()
            .source(dependencyAlias(urn))
            .name(sig.name)
            .systemMetadata(
              SystemFunctionMetadata.builder().name(sqlName).notation(notation).build())
            .addAllSupportedImpls(sigs.sorted.asJava)
            .build()
        )
    }.toSeq
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
      case "req" => Some(Literal("req"))
      case _ => None
    }
  }
}

object DialectGenerator
  extends DialectGenerator(
    SparkExtension.SparkScalarFunctions,
    SparkExtension.StandardAggregateFunctions,
    SparkExtension.StandardWindowFunctions) {

  def main(args: Array[String]) = {
    val yaml = generateYaml()

    val out = args match {
      case Array(t) =>
        val f = new File(t)
        if (!f.exists()) {
          f.createNewFile()
        }
        new FileWriter(t)
      case _ => new OutputStreamWriter(System.out)
    }

    out.write(yaml)
    out.flush()
  }
}
