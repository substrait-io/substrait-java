package io.substrait.spark.utils

import io.substrait.spark.SparkExtension.{COLLECTION, SparkScalarFunctions}
import io.substrait.spark.expression.FunctionMappings.{AGGREGATE_SIGS, SCALAR_SIGS, WINDOW_SIGS}
import io.substrait.spark.expression.Sig

import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, Literal}
import org.apache.spark.sql.types.{ByteType, DateType, DayTimeIntervalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType, YearMonthIntervalType}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import io.substrait.extension.SimpleExtension

import java.io.{File, FileWriter, InputStream, OutputStreamWriter}

import scala.jdk.CollectionConverters._

case class Dialect(
    name: String,
    supported_types: Seq[SupportedType],
    supported_expressions: Seq[Any],
    supported_relations: Seq[Any],
    dependencies: Map[String, String],
    supported_scalar_functions: Seq[SupportedFunction],
    supported_aggregate_functions: Seq[SupportedFunction],
    supported_window_functions: Seq[SupportedFunction])

// Types section
case class TypeMetadata(name: String, supported_as_column: Boolean)
case class SupportedType(
    `type`: String,
    system_metadata: TypeMetadata,
    max_precision: Option[Integer] = None)

// Functions section
case class FunctionMetadata(name: String, notation: String)
case class SupportedFunction(
    source: String,
    name: String,
    system_metadata: FunctionMetadata,
    supported_impls: Seq[String])

class DialectGenerator {
  // The dialect schema ships on the classpath in the substrait-packaging extensions artifact.
  val schemaResource = "/substrait/text/dialect_schema.yaml"

  def schemaStream(): InputStream = {
    val stream = getClass.getResourceAsStream(schemaResource)
    if (stream == null) {
      throw new IllegalStateException(s"Dialect schema not found on classpath: $schemaResource")
    }
    stream
  }

  private val sourceURNs = Map(
    "extension:io.substrait:functions_aggregate_approx" -> "aggregate_approx",
    "extension:io.substrait:functions_aggregate_generic" -> "aggregate_generic",
    "extension:io.substrait:functions_arithmetic" -> "arithmetic",
    "extension:io.substrait:functions_arithmetic_decimal" -> "arithmetic_decimal",
    "extension:io.substrait:functions_boolean" -> "boolean",
    "extension:io.substrait:functions_comparison" -> "comparison",
    "extension:io.substrait:functions_datetime" -> "datetime",
    "extension:io.substrait:functions_logarithmic" -> "logarithmic",
    "extension:io.substrait:functions_rounding" -> "rounding",
    "extension:io.substrait:functions_rounding_decimal" -> "rounding_decimal",
    "extension:io.substrait:functions_string" -> "string",
    "extension:substrait:spark" -> "spark"
  )

  def generate(): Dialect = {
    val types = supportedTypes()
    val expressions = supportedExpressions()
    val relations = supportedRelations()
    val scalars = SCALAR_SIGS.flatMap(supportedFunctions(SparkScalarFunctions))
    val aggregates =
      AGGREGATE_SIGS
        .flatMap(supportedFunctions(COLLECTION.aggregateFunctions().asScala.toSeq))
    val windows =
      WINDOW_SIGS
        .flatMap(supportedFunctions(COLLECTION.windowFunctions().asScala.toSeq))

    Dialect(
      "Spark Dialect",
      types,
      expressions,
      relations,
      sourceURNs.map(_.swap),
      scalars.sortBy(f => (f.source, f.name)),
      aggregates.sortBy(f => (f.source, f.name)),
      windows.sortBy(f => (f.source, f.name))
    )
  }

  def generateYaml(): String = {
    // Generate the dialect YAML
    val mapper = YAMLMapper
      .builder()
      .defaultPropertyInclusion(JsonInclude.Value.ALL_NON_ABSENT)
      .addModule(DefaultScalaModule)
      .build()
    val yaml = mapper.writeValueAsString(generate())

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

  private def supportedTypes(): Seq[SupportedType] = {
    Seq(
      SupportedType("I8", TypeMetadata("ByteType", true)),
      SupportedType("I16", TypeMetadata("ShortType", true)),
      SupportedType("I32", TypeMetadata("IntegerType", true)),
      SupportedType("I64", TypeMetadata("LongType", true)),
      SupportedType("FP32", TypeMetadata("FloatType", true)),
      SupportedType("FP64", TypeMetadata("DoubleType", true)),
      SupportedType("DECIMAL", TypeMetadata("DecimalType", true)),
      SupportedType("DATE", TypeMetadata("DateType", true)),
      SupportedType("STRING", TypeMetadata("StringType", true)),
      SupportedType("VARCHAR", TypeMetadata("StringType", true)),
      SupportedType("FIXED_CHAR", TypeMetadata("StringType", true)),
      SupportedType("BINARY", TypeMetadata("BinaryType", true)),
      SupportedType("BOOL", TypeMetadata("BooleanType", true)),
      SupportedType(
        "PRECISION_TIMESTAMP",
        TypeMetadata("TimestampNTZType", true),
        max_precision = Some(9)),
      SupportedType(
        "PRECISION_TIMESTAMP_TZ",
        TypeMetadata("TimestampType", true),
        max_precision = Some(9)),
      SupportedType(
        "INTERVAL_DAY",
        TypeMetadata("DayTimeIntervalType", true),
        max_precision = Some(9)),
      SupportedType("INTERVAL_YEAR", TypeMetadata("YearMonthIntervalType", true)),
      SupportedType("LIST", TypeMetadata("ArrayType", true)),
      SupportedType("MAP", TypeMetadata("MapType", true)),
      SupportedType("STRUCT", TypeMetadata("StructType", true))
    )
  }

  private def supportedExpressions(): Seq[Any] = {
    Seq(
      "LITERAL",
      "SELECTION",
      "SCALAR_FUNCTION",
      "IF_THEN",
      "SINGULAR_OR_LIST",
      "CAST",
      Map(
        "expression" -> "SUBQUERY",
        "subquery_types" -> Seq("SCALAR", "IN_PREDICATE")
      )
    )
  }

  private def supportedRelations(): Seq[Any] = {
    Seq(
      "FILTER",
      "FETCH",
      "AGGREGATE",
      "SORT",
      "PROJECT",
      "CROSS",
      "UPDATE",
      "CONSISTENT_PARTITION_WINDOW",
      "EXPAND",
      "WRITE",
      Map(
        "relation" -> "READ",
        "read_types" -> Seq("VIRTUAL_TABLE", "LOCAL_FILES", "NAMED_TABLE")
      ),
      Map(
        "relation" -> "DDL",
        "write_types" -> Seq("NAMED_OBJECT")
      ),
      Map(
        "relation" -> "JOIN",
        "join_types" -> Seq("INNER", "OUTER", "LEFT", "RIGHT", "LEFT_SEMI", "LEFT_ANTI")
      ),
      Map(
        "relation" -> "SET",
        "operations" -> Seq("UNION_ALL")
      )
    )
  }

  // The supported functions section is generated from the existing FunctionMappings code.
  private def supportedFunctions(functions: Seq[SimpleExtension.Function])(
      sig: Sig): Seq[SupportedFunction] = {
    val inst = if (classOf[Expression].isAssignableFrom(sig.expClass)) {
      val cons = sig.expClass.getDeclaredConstructors.minBy(c => c.getParameterCount)
      val i = cons.getParameterCount
      i match {
        case 0 => cons.newInstance()
        case 1 => cons.newInstance(null)
        case 2 => cons.newInstance(null, null)
        case 3 => cons.newInstance(null, null, null)
        case _ =>
          throw new UnsupportedOperationException(
            s"${sig.expClass} constructor requires $i parameters")
      }
    } else {
      throw new UnsupportedOperationException(s"${sig.expClass} is not an Expression")
    }

    val sqlName = inst match {
      case bo: BinaryOperator => bo.sqlOperator
      case e: Expression => e.prettyName
      case _ => "NO NAME"
    }

    val notation = inst match {
      case _: BinaryOperator => "INFIX"
      case _ => "FUNCTION"
    }

    // create a map of function parameter variants grouped by URN
    val variants = functions.filter(_.name() == sig.name)
    val groups: Map[String, Seq[String]] = inst match {
      case expr: Expression =>
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
      case _ =>
        println(s"NO INPUT TYPES")
        Map.empty
    }
    groups.map {
      case (urn, sigs) =>
        SupportedFunction(
          sourceURNs.getOrElse(urn, ""),
          sig.name,
          FunctionMetadata(sqlName, notation),
          sigs.sorted)
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

object DialectGenerator extends DialectGenerator {
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
