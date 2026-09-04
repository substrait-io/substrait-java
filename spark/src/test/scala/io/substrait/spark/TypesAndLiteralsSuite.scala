package io.substrait.spark

import io.substrait.spark.expression.{ToSparkExpression, ToSubstraitLiteral}
import io.substrait.spark.utils.Util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.substrait.SparkTypeUtil
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.`type`.TypeCreator
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.util.EmptyVisitationContext

import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}

class TypesAndLiteralsSuite extends SparkFunSuite {

  val toSparkExpression = new ToSparkExpression(null, null)

  val types: Seq[DataType] = List(
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    BooleanType,
    DecimalType(10, 2),
    TimestampNTZType,
    TimestampType,
    DayTimeIntervalType.DEFAULT,
    YearMonthIntervalType.DEFAULT,
    ArrayType(IntegerType, containsNull = false),
    ArrayType(IntegerType, containsNull = true),
    MapType(IntegerType, StringType, valueContainsNull = false),
    MapType(IntegerType, StringType, valueContainsNull = true),
    StructType(
      Seq( // match automatic naming
        StructField("col1", IntegerType, nullable = false),
        StructField("col2", StringType, nullable = false),
        StructField(
          "col3",
          StructType(Seq(StructField("col1", IntegerType, nullable = false))),
          nullable = false)
      )
    )
  )

  types.foreach(
    t => {
      test(s"test type: $t") {
        // Nullability doesn't matter as in Spark it's not a property of the type
        val substraitType = ToSubstraitType.convert(t, nullable = true).get
        val sparkType = ToSparkType.convert(substraitType)

        println("Before: " + t)
        println("After: " + sparkType)
        println("Substrait: " + substraitType)

        assert(t == sparkType)
      }
    })

  val defaultLiterals: Seq[Literal] = types.map(Literal.default)

  val literals: Seq[Literal] = List(
    Literal(1.toByte),
    Literal(1.toShort),
    Literal(1),
    Literal(1L),
    Literal(1.0f),
    Literal(1.0),
    Literal("1"),
    Literal(Array[Byte](1)),
    Literal(true),
    Literal(BigDecimal("123.4567890")),
    Literal(Instant.now()), // Timestamp
    Literal(LocalDateTime.now()), // TimestampNTZ
    Literal(LocalDate.now()), // Date
    Literal(Duration.ofDays(1)), // DayTimeInterval
    Literal(
      Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4).plusMillis(5)
    ), // DayTimeInterval
    Literal(Period.ofYears(1)), // YearMonthInterval
    Literal(Period.of(1, 2, 0)), // YearMonthInterval, days are ignored
    Literal.create(Array(1, 2, 3), ArrayType(IntegerType, containsNull = false)),
//    Literal.create(Array(1, null, 3), ArrayType(IntegerType, containsNull = true)) // TODO: handle containsNulls
    Literal.create(
      Row(1, "a"),
      StructType(
        Seq( // match automatic naming
          StructField("col1", IntegerType, nullable = false),
          StructField("col2", StringType, nullable = false)
        ) // TODO: handle nullable = true
      )
    )
  )

  (defaultLiterals ++ literals).foreach(
    l => {
      test(s"test literal: $l (${l.dataType})") {
        val substraitLiteral = ToSubstraitLiteral.convert(l).get
        val sparkLiteral = substraitLiteral
          .accept(toSparkExpression, EmptyVisitationContext.INSTANCE)
          .asInstanceOf[Literal]

        println("Before: " + l + " " + l.dataType)
        println("After: " + sparkLiteral + " " + sparkLiteral.dataType)
        println("Substrait: " + substraitLiteral)

        assert(l.dataType == sparkLiteral.dataType) // makes understanding failures easier
        assert(l == sparkLiteral)
      }
    })

  test(s"test map literal") {
    val l = Literal.create(
      Map(1 -> "a", 2 -> "b"),
      MapType(IntegerType, StringType, valueContainsNull = false))

    val substraitLiteral = ToSubstraitLiteral.convert(l).get
    val sparkLiteral = substraitLiteral
      .accept(toSparkExpression, EmptyVisitationContext.INSTANCE)
      .asInstanceOf[Literal]

    println("Before: " + l + " " + l.dataType)
    println("After: " + sparkLiteral + " " + sparkLiteral.dataType)
    println("Substrait: " + substraitLiteral)

    assert(l.dataType == sparkLiteral.dataType) // makes understanding failures easier
    assert(SparkTypeUtil.sameType(l.dataType, sparkLiteral.dataType))

    // MapData doesn't implement equality so we have to compare the arrays manually
    val originalKeys = l.value.asInstanceOf[MapData].keyArray().toIntArray().sorted
    val sparkKeys = sparkLiteral.value.asInstanceOf[MapData].keyArray().toIntArray().sorted
    assert(originalKeys.sameElements(sparkKeys))

    val originalValues = l.value.asInstanceOf[MapData].valueArray().toArray[UTF8String](StringType)
    val sparkValues =
      sparkLiteral.value.asInstanceOf[MapData].valueArray().toArray[UTF8String](StringType)

    assert(originalValues.toSet == sparkValues.toSet)
  }

  test(s"test named struct") {
    // The types test above doesn't cover names so we need to test it separately
    val dt = StructType(
      Seq(
        StructField("integer_col", IntegerType, nullable = false),
        StructField("string_col", StringType, nullable = false),

        // Nested in struct
        StructField(
          "struct_col",
          StructType(
            Seq(
              StructField("nested_integer_col", IntegerType, nullable = false),
              StructField("nested_string_col", StringType, nullable = false)
            )
          ),
          nullable = false
        ),

        // Struct in array
        StructField(
          "array_col",
          ArrayType(
            StructType(
              Seq(
                StructField("array_integer_col", IntegerType, nullable = false),
                StructField("array_string_col", StringType, nullable = false)
              )
            ),
            containsNull = false
          ),
          nullable = false
        ),

        // Struct in map
        StructField(
          "map_col",
          MapType(
            StructType(
              Seq(
                StructField("map_key_integer_col", IntegerType, nullable = false),
                StructField("map_key_string_col", StringType, nullable = false)
              )
            ),
            StructType(
              Seq(
                StructField("map_value_integer_col", IntegerType, nullable = false),
                StructField("map_value_string_col", StringType, nullable = false)
              )
            ),
            valueContainsNull = false
          ),
          nullable = false
        ),

        // Struct in a struct
        StructField(
          "nested_struct_col",
          StructType(Seq(StructField("integer_col", IntegerType, nullable = false))),
          nullable = false)
      )
    )

    val substraitType = ToSubstraitType.toNamedStruct(dt)
    val sparkType = ToSparkType.toStructType(substraitType)

    assert(dt == sparkType)
  }

  test(s"test UnsafeArrayData literal") {
    val originalValues = Array(1, 2, 3)
    val l = Literal.create(
      UnsafeArrayData.fromPrimitiveArray(originalValues),
      ArrayType(IntegerType, containsNull = false))

    val substraitLiteral = ToSubstraitLiteral.convert(l).get
    val sparkLiteral = substraitLiteral
      .accept(toSparkExpression, EmptyVisitationContext.INSTANCE)
      .asInstanceOf[Literal]

    println("Before: " + l + " " + l.dataType)
    println("After: " + sparkLiteral + " " + sparkLiteral.dataType)
    println("Substrait: " + substraitLiteral)

    assert(l.dataType == sparkLiteral.dataType)
    assert(
      sparkLiteral.value
        .asInstanceOf[ArrayData]
        .toArray[Integer](IntegerType)
        .sorted
        .sameElements(originalValues))
  }

  private def sparkLiteral(literal: SExpression.Literal): Literal =
    literal.accept(toSparkExpression, EmptyVisitationContext.INSTANCE).asInstanceOf[Literal]

  private def rejects(precision: Int)(convert: => Any): Unit =
    assert(
      intercept[UnsupportedOperationException](convert).getMessage
        .contains(s"Unsupported precision: $precision"))

  test("a temporal literal coarser than microseconds is rescaled") {
    // Spark has one microsecond-based representation per type, so a coarser precision fits without
    // loss — but only once the value is scaled. Taken verbatim it is wrong by exactly that factor.
    Seq((0, 1L, 1000000L), (1, 15L, 1500000L), (3, 1500L, 1500000L), (6, 1500000L, 1500000L))
      .foreach {
        case (precision, value, micros) =>
          val ntz = sparkLiteral(ExpressionCreator.precisionTimestamp(false, value, precision))
          assert(ntz.value === micros)
          assert(ntz.dataType === TimestampNTZType)

          val tz = sparkLiteral(ExpressionCreator.precisionTimestampTZ(false, value, precision))
          assert(tz.value === micros)
          assert(tz.dataType === TimestampType)
      }
  }

  test("an interval literal scales only its sub-second part") {
    // days and seconds are whole units whatever the precision; only subseconds is expressed in
    // 1e(-P) units, so it is the one field that moves.
    Seq((0, 0L, 0L), (1, 5L, 500000L), (3, 500L, 500000L), (6, 500000L, 500000L)).foreach {
      case (precision, subseconds, micros) =>
        val interval =
          sparkLiteral(ExpressionCreator.intervalDay(false, 1, 2, subseconds, precision))
        assert(interval.value === (Util.SECONDS_PER_DAY + 2) * Util.MICROS_PER_SECOND + micros)
        assert(interval.dataType === DayTimeIntervalType.DEFAULT)
    }
  }

  test("a precision finer than microseconds is rejected for every carrier") {
    rejects(9)(sparkLiteral(ExpressionCreator.precisionTimestamp(false, 1L, 9)))
    rejects(9)(sparkLiteral(ExpressionCreator.precisionTimestampTZ(false, 1L, 9)))
    rejects(9)(sparkLiteral(ExpressionCreator.intervalDay(false, 0, 0, 1L, 9)))

    rejects(9)(ToSparkType.convert(TypeCreator.REQUIRED.precisionTimestamp(9)))
    rejects(9)(ToSparkType.convert(TypeCreator.REQUIRED.precisionTimestampTZ(9)))
    rejects(9)(ToSparkType.convert(TypeCreator.REQUIRED.intervalDay(9)))
  }

  test("a negative precision is rejected, naming the range rather than one end of it") {
    // Reachable from the wire: the literal's precision is a plain int32 in algebra.proto and core
    // does not bound it, so the message has to cover both directions of the range.
    val e = intercept[UnsupportedOperationException](Util.toMicroseconds(1L, -1))
    assert(e.getMessage.contains("Unsupported precision: -1"))
    assert(e.getMessage.contains("between 0 and 6"))
  }

  test("rescaling reports overflow instead of wrapping") {
    // The scaling multiply is not reachable from any instant Spark can represent, but a sentinel
    // value wraps into a plausible one: Long.MaxValue at precision 0 used to come out as
    // -1000000, one second before the epoch.
    intercept[ArithmeticException](Util.toMicroseconds(Long.MaxValue, 0))
    intercept[ArithmeticException](Util.toMicroseconds(Long.MinValue, 0))

    // The same for the interval, whose day count is a whole-unit multiply of its own.
    intercept[ArithmeticException](
      sparkLiteral(ExpressionCreator.intervalDay(false, Int.MaxValue, 0, 0L, 6)))
  }

  test("a year-month interval reports overflow instead of wrapping") {
    // years and months are both int32 and Spark's physical type is a months Int, so the flattened
    // total can outrun the carrier: 178,956,971 years used to come out as -2,147,483,644 months,
    // and Int.MinValue years as exactly 0 — a zero-length interval. This is the carrier's bound,
    // not the spec's much tighter 10,000-year one, so it takes a producer already far past that.
    intercept[ArithmeticException](
      sparkLiteral(ExpressionCreator.intervalYear(false, 178956971, 0)))
    intercept[ArithmeticException](
      sparkLiteral(ExpressionCreator.intervalYear(false, 178956970, 8)))
    intercept[ArithmeticException](
      sparkLiteral(ExpressionCreator.intervalYear(false, Int.MinValue, 0)))

    // Only the total is significant, so an intermediate past Int.MaxValue is not itself an error.
    val mixedSigns = sparkLiteral(ExpressionCreator.intervalYear(false, 178956971, -12))
    assert(mixedSigns.value === 2147483640)

    // The spec's maximum still converts, and the months component carries through.
    val atTheBound = sparkLiteral(ExpressionCreator.intervalYear(false, 9999, 12))
    assert(atTheBound.value === 120000)
    assert(atTheBound.dataType === YearMonthIntervalType.DEFAULT)
  }

  test("a coarser precision on a type is rejected, since a type has no value to rescale") {
    // A Spark type carries no precision of its own, so mapping precision_timestamp<3> onto
    // TimestampNTZType would reinterpret millisecond counts as microsecond ones. Only the literal
    // conversions relax this, because only they hold a value.
    rejects(3)(ToSparkType.convert(TypeCreator.REQUIRED.precisionTimestamp(3)))
    rejects(3)(ToSparkType.convert(TypeCreator.REQUIRED.precisionTimestampTZ(3)))
    rejects(3)(ToSparkType.convert(TypeCreator.REQUIRED.intervalDay(3)))

    // The path that makes this matter: a cast to a coarser precision becomes a cast between two
    // TimestampNTZTypes, which truncates nothing, so the value would keep its finer resolution.
    val cast = SExpression.Cast
      .builder()
      .input(ExpressionCreator.precisionTimestamp(false, 1234567L, 6))
      .`type`(TypeCreator.REQUIRED.precisionTimestamp(3))
      .failureBehavior(SExpression.FailureBehavior.THROW_EXCEPTION)
      .build()
    rejects(3)(cast.accept(toSparkExpression, EmptyVisitationContext.INSTANCE))
  }
}
