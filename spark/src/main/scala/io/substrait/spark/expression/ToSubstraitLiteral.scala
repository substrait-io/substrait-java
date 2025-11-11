/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.substrait.spark.expression

import io.substrait.spark.ToSubstraitType
import io.substrait.spark.utils.Util

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.expression.{Expression => SExpression}
import io.substrait.expression.ExpressionCreator._

import scala.collection.JavaConverters

class ToSubstraitLiteral {

  private def sparkIntervalDay2Substrait(ms: Long, nullable: Boolean): SExpression.Literal = {
    val days = (ms / Util.MICROS_PER_SECOND / Util.SECONDS_PER_DAY).toInt
    val seconds = (ms / Util.MICROS_PER_SECOND % Util.SECONDS_PER_DAY).toInt
    val micros = ms % Util.MICROS_PER_SECOND
    intervalDay(nullable, days, seconds, micros, Util.MICROSECOND_PRECISION)
  }

  private def sparkArray2Substrait(
      arrayData: ArrayData,
      elementType: DataType,
      containsNull: Boolean,
      nullable: Boolean): SExpression.Literal = {
    val elements =
      arrayData
        .toArray(elementType)
        .map((any: Any) => apply(Literal(any, elementType), Some(containsNull)))
    if (elements.isEmpty) {
      emptyList(nullable, ToSubstraitType.convert(elementType, nullable = containsNull).get)
    } else {
      list(nullable, JavaConverters.asJavaIterable(elements))
    }
  }

  private def sparkMap2Substrait(
      mapData: MapData,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean,
      nullable: Boolean): SExpression.Literal = {
    val keys = mapData.keyArray().array.map(any => apply(Literal(any, keyType), Some(false)))
    val values =
      mapData.valueArray().array.map(any => apply(Literal(any, valueType), Some(valueContainsNull)))
    if (keys.isEmpty) {
      emptyMap(
        nullable,
        ToSubstraitType.convert(keyType, nullable = false).get,
        ToSubstraitType.convert(valueType, nullable = valueContainsNull).get)
    } else {
      map(nullable, JavaConverters.mapAsJavaMap(keys.zip(values).toMap))
    }
  }

  private def sparkStruct2Substrait(
      structData: GenericInternalRow,
      fields: Array[StructField],
      nullable: Boolean): SExpression.Literal = {
    struct(
      nullable,
      JavaConverters.asJavaIterable(fields.zip(structData.values).map {
        case (field, any) => apply(Literal(any, field.dataType), Some(field.nullable))
      }))
  }

  private def convertWithValue(literal: Literal, nullable: Boolean): Option[SExpression.Literal] = {
    Option.apply(
      literal match {
        case Literal(b: Boolean, BooleanType) => bool(nullable, b)
        case Literal(b: Byte, ByteType) => i8(nullable, b)
        case Literal(s: Short, ShortType) => i16(nullable, s)
        case Literal(i: Integer, IntegerType) => i32(nullable, i)
        case Literal(l: Long, LongType) => i64(nullable, l)
        case Literal(f: Float, FloatType) => fp32(nullable, f)
        case Literal(d: Double, DoubleType) => fp64(nullable, d)
        case Literal(d: Decimal, dataType: DecimalType) =>
          decimal(nullable, d.toJavaBigDecimal, dataType.precision, dataType.scale)
        case Literal(d: Integer, DateType) => date(nullable, d)
        case Literal(t: Long, TimestampType) =>
          precisionTimestampTZ(nullable, t, Util.MICROSECOND_PRECISION)
        case Literal(t: Long, TimestampNTZType) =>
          precisionTimestamp(nullable, t, Util.MICROSECOND_PRECISION)
        case Literal(d: Long, DayTimeIntervalType.DEFAULT) =>
          sparkIntervalDay2Substrait(d, nullable)
        case Literal(ym: Int, YearMonthIntervalType.DEFAULT) =>
          intervalYear(nullable, ym / 12, ym % 12)
        case Literal(u: UTF8String, StringType) => string(nullable, u.toString)
        case Literal(b: Array[Byte], BinaryType) => binary(nullable, b)
        case Literal(a: ArrayData, ArrayType(et, containsNull)) =>
          sparkArray2Substrait(a, et, containsNull, nullable)
        case Literal(m: MapData, MapType(keyType, valueType, valueContainsNull)) =>
          sparkMap2Substrait(m, keyType, valueType, valueContainsNull, nullable)
        case Literal(s: GenericInternalRow, StructType(fields)) =>
          sparkStruct2Substrait(s, fields, nullable)
        case _ => null
      }
    )
  }

  def convert(literal: Literal): Option[SExpression.Literal] = {
    if (literal.nullable) {
      ToSubstraitType
        .convert(literal.dataType, nullable = true)
        .map(typedNull)
    } else {
      convertWithValue(literal, nullable = false)
    }
  }

  /**
   * Converts a Spark Literal to a Substrait Literal.
   *
   * @param literal
   *   The Spark literal to convert
   * @param nullable
   *   Optional explicit nullability to use for the Substrait literal (typically from the schema).
   *   If None, uses the literal's inferred nullability.
   */
  def apply(literal: Literal, nullable: Option[Boolean] = None): SExpression.Literal = {
    nullable match {
      case Some(n) =>
        // Use explicit nullability
        if (literal.value == null) {
          if (!n) {
            throw new IllegalArgumentException("Cannot create a non-nullable type for a null value")
          }
          ToSubstraitType
            .convert(literal.dataType, nullable = true)
            .map(typedNull)
            .getOrElse(throw new UnsupportedOperationException(
              s"Unable to convert the type ${literal.dataType.typeName}"))
        } else {
          convertWithValue(literal, n)
            .getOrElse(
              throw new UnsupportedOperationException(
                s"Unable to convert the type ${literal.dataType.typeName}"))
        }
      case None =>
        // Use literal's inferred nullability
        convert(literal)
          .getOrElse(
            throw new UnsupportedOperationException(
              s"Unable to convert the type ${literal.dataType.typeName}"))
    }
  }
}

object ToSubstraitLiteral extends ToSubstraitLiteral

object SubstraitLiteral {
  def unapply(literal: Literal): Option[SExpression.Literal] = {
    ToSubstraitLiteral.convert(literal)
  }
}
