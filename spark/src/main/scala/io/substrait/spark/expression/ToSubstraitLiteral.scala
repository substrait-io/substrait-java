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

  object Nonnull {
    private def sparkDecimal2Substrait(
        d: Decimal,
        precision: Int,
        scale: Int): SExpression.Literal =
      decimal(false, d.toJavaBigDecimal, precision, scale)

    private def sparkArray2Substrait(
        arrayData: ArrayData,
        elementType: DataType,
        containsNull: Boolean): SExpression.Literal = {
      val elements =
        arrayData.toArray(elementType).map((any: Any) => apply(Literal(any, elementType)))
      if (elements.isEmpty) {
        return emptyList(false, ToSubstraitType.convert(elementType, nullable = containsNull).get)
      }
      list(false, JavaConverters.asJavaIterable(elements)) // TODO: handle containsNull
    }

    private def sparkMap2Substrait(
        mapData: MapData,
        keyType: DataType,
        valueType: DataType,
        valueContainsNull: Boolean): SExpression.Literal = {
      val keys = mapData.keyArray().array.map(any => apply(Literal(any, keyType)))
      val values = mapData.valueArray().array.map(any => apply(Literal(any, valueType)))
      if (keys.isEmpty) {
        return emptyMap(
          false,
          ToSubstraitType.convert(keyType, nullable = false).get,
          ToSubstraitType.convert(valueType, nullable = valueContainsNull).get)
      }
      // TODO: handle valueContainsNull
      map(false, JavaConverters.mapAsJavaMap(keys.zip(values).toMap))
    }

    private def sparkStruct2Substrait(
        structData: GenericInternalRow,
        fields: Array[StructField]): SExpression.Literal =
      struct(
        false,
        JavaConverters.asJavaIterable(fields.zip(structData.values).map {
          case (field, any) => apply(Literal(any, field.dataType))
        }))

    val _bool: Boolean => SExpression.Literal = bool(false, _)
    val _i8: Byte => SExpression.Literal = i8(false, _)
    val _i16: Short => SExpression.Literal = i16(false, _)
    val _i32: Int => SExpression.Literal = i32(false, _)
    val _i64: Long => SExpression.Literal = i64(false, _)
    val _fp32: Float => SExpression.Literal = fp32(false, _)
    val _fp64: Double => SExpression.Literal = fp64(false, _)
    val _decimal: (Decimal, Int, Int) => SExpression.Literal = sparkDecimal2Substrait
    val _date: Int => SExpression.Literal = date(false, _)
    val _timestamp: Long => SExpression.Literal =
      precisionTimestamp(false, _, Util.MICROSECOND_PRECISION) // Spark ts is in microseconds
    val _timestampTz: Long => SExpression.Literal =
      precisionTimestampTZ(false, _, Util.MICROSECOND_PRECISION) // Spark ts is in microseconds
    val _intervalDay: Long => SExpression.Literal = (ms: Long) => {
      val days = (ms / Util.MICROS_PER_SECOND / Util.SECONDS_PER_DAY).toInt
      val seconds = (ms / Util.MICROS_PER_SECOND % Util.SECONDS_PER_DAY).toInt
      val micros = ms % Util.MICROS_PER_SECOND
      intervalDay(false, days, seconds, micros, Util.MICROSECOND_PRECISION)
    }
    val _intervalYear: Int => SExpression.Literal = (m: Int) => intervalYear(false, m / 12, m % 12)
    val _string: String => SExpression.Literal = string(false, _)
    val _binary: Array[Byte] => SExpression.Literal = binary(false, _)
    val _array: (ArrayData, DataType, Boolean) => SExpression.Literal = sparkArray2Substrait
    val _map: (MapData, DataType, DataType, Boolean) => SExpression.Literal = sparkMap2Substrait
    val _struct: (GenericInternalRow, Array[StructField]) => SExpression.Literal =
      sparkStruct2Substrait
  }

  private def convertWithValue(literal: Literal): Option[SExpression.Literal] = {
    Option.apply(
      literal match {
        case Literal(b: Boolean, BooleanType) => Nonnull._bool(b)
        case Literal(b: Byte, ByteType) => Nonnull._i8(b)
        case Literal(s: Short, ShortType) => Nonnull._i16(s)
        case Literal(i: Integer, IntegerType) => Nonnull._i32(i)
        case Literal(l: Long, LongType) => Nonnull._i64(l)
        case Literal(f: Float, FloatType) => Nonnull._fp32(f)
        case Literal(d: Double, DoubleType) => Nonnull._fp64(d)
        case Literal(d: Decimal, dataType: DecimalType) =>
          Nonnull._decimal(d, dataType.precision, dataType.scale)
        case Literal(d: Integer, DateType) => Nonnull._date(d)
        case Literal(t: Long, TimestampType) => Nonnull._timestampTz(t)
        case Literal(t: Long, TimestampNTZType) => Nonnull._timestamp(t)
        case Literal(d: Long, DayTimeIntervalType.DEFAULT) => Nonnull._intervalDay(d)
        case Literal(ym: Int, YearMonthIntervalType.DEFAULT) => Nonnull._intervalYear(ym)
        case Literal(u: UTF8String, StringType) => Nonnull._string(u.toString)
        case Literal(b: Array[Byte], BinaryType) => Nonnull._binary(b)
        case Literal(a: ArrayData, ArrayType(et, containsNull)) =>
          Nonnull._array(a, et, containsNull)
        case Literal(m: MapData, MapType(keyType, valueType, valueContainsNull)) =>
          Nonnull._map(m, keyType, valueType, valueContainsNull)
        case Literal(s: GenericInternalRow, StructType(fields)) => Nonnull._struct(s, fields)
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
      convertWithValue(literal)
    }
  }

  def apply(literal: Literal): SExpression.Literal = {
    convert(literal)
      .getOrElse(
        throw new UnsupportedOperationException(
          s"Unable to convert the type ${literal.dataType.typeName}"))
  }
}

object ToSubstraitLiteral extends ToSubstraitLiteral

object SubstraitLiteral {
  def unapply(literal: Literal): Option[SExpression.Literal] = {
    ToSubstraitLiteral.convert(literal)
  }
}
