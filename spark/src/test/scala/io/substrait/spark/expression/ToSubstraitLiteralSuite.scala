package io.substrait.spark.expression

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._

import io.substrait.expression.{Expression => SExpression}

class ToSubstraitLiteralSuite extends SparkFunSuite {

  test("apply without explicit nullability uses literal's inferred nullability") {
    val nonNullLiteral = Literal(42, IntegerType)
    val result = ToSubstraitLiteral.apply(nonNullLiteral)
    assert(!result.getType.nullable(), "Non-null value should create non-nullable type")
    assert(result.isInstanceOf[SExpression.I32Literal])
  }

  test("apply with Some(true) creates nullable type even for non-null value") {
    val nonNullLiteral = Literal(42, IntegerType)
    val result = ToSubstraitLiteral.apply(nonNullLiteral, Some(true))
    assert(result.getType.nullable(), "Explicit nullable=true should create nullable type")
    assert(result.isInstanceOf[SExpression.I32Literal])
  }

  test("apply with Some(false) creates non-nullable type") {
    val literal = Literal(42, IntegerType)
    val result = ToSubstraitLiteral.apply(literal, Some(false))
    assert(!result.getType.nullable(), "Explicit nullable=false should create non-nullable type")
    assert(result.isInstanceOf[SExpression.I32Literal])
  }

  test("apply with null value and Some(true) creates nullable typed null") {
    val nullLiteral = Literal(null, IntegerType)
    val result = ToSubstraitLiteral.apply(nullLiteral, Some(true))
    assert(result.getType.nullable(), "Null value should create nullable type")
    assert(result.isInstanceOf[SExpression.NullLiteral])
  }

  test("apply with null value and Some(false) throws exception") {
    val nullLiteral = Literal(null, IntegerType)
    val thrown = intercept[IllegalArgumentException] {
      ToSubstraitLiteral.apply(nullLiteral, Some(false))
    }
    assert(thrown.getMessage.contains("Cannot create a non-nullable type for a null value"))
  }

  test("apply preserves nullability for nested struct fields") {
    val sparkStructType = StructType(
      Seq(
        StructField("nullable_field", IntegerType, nullable = true),
        StructField("non_nullable_field", IntegerType, nullable = false)
      )
    )

    val rowData = Row(42, 100)
    val structLiteral: Literal = Literal.create(rowData, sparkStructType)

    val result = ToSubstraitLiteral.apply(structLiteral, Some(false))

    assert(!result.getType.nullable(), "Struct itself should be non-nullable")
    val substraitStructType = result.getType.asInstanceOf[io.substrait.`type`.Type.Struct]
    assert(
      substraitStructType.fields().get(0).nullable(),
      "First field should be nullable per schema")
    assert(
      !substraitStructType.fields().get(1).nullable(),
      "Second field should be non-nullable per schema")
  }

  test("apply preserves nullability for array elements") {
    val arrayType = ArrayType(IntegerType, containsNull = true)
    val arrayLiteral = Literal.create(Array(1, 2, 3), arrayType)

    val result = ToSubstraitLiteral.apply(arrayLiteral, Some(false))

    assert(!result.getType.nullable(), "Array itself should be non-nullable")
    val listType = result.getType.asInstanceOf[io.substrait.`type`.Type.ListType]
    assert(listType.elementType().nullable(), "Elements should be nullable per schema")
  }

  test("apply preserves nullability for map values") {
    val mapType = MapType(IntegerType, StringType, valueContainsNull = true)
    val mapLiteral = Literal.create(Map(1 -> "a", 2 -> "b"), mapType)

    val result = ToSubstraitLiteral.apply(mapLiteral, Some(false))

    assert(!result.getType.nullable(), "Map itself should be non-nullable")
    val substraitMapType = result.getType.asInstanceOf[io.substrait.`type`.Type.Map]
    assert(!substraitMapType.key().nullable(), "Map keys should be non-nullable")
    assert(substraitMapType.value().nullable(), "Map values should be nullable per schema")
  }

  test("apply with different data types respects explicit nullability") {
    val testCases = Seq(
      (Literal("hello"), "String"),
      (Literal(42L), "Long"),
      (Literal(3.14), "Double"),
      (Literal(true), "Boolean")
    )

    testCases.foreach {
      case (literal, typeName) =>
        // Test with nullable=true
        val nullableResult = ToSubstraitLiteral.apply(literal, Some(true))
        assert(nullableResult.getType.nullable(), s"$typeName should be nullable with Some(true)")

        // Test with nullable=false
        val nonNullableResult = ToSubstraitLiteral.apply(literal, Some(false))
        assert(
          !nonNullableResult.getType.nullable(),
          s"$typeName should be non-nullable with Some(false)")
    }
  }
}
