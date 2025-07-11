package io.substrait.spark

import io.substrait.spark.expression.{ToSparkExpression, ToSubstraitLiteral}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.substrait.SparkTypeUtil
import org.apache.spark.unsafe.types.UTF8String

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
        val sparkLiteral = substraitLiteral.accept(toSparkExpression, null).asInstanceOf[Literal]

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
    val sparkLiteral = substraitLiteral.accept(toSparkExpression, null).asInstanceOf[Literal]

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
    assert(originalValues.sorted.sameElements(sparkValues.sorted))
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
}
