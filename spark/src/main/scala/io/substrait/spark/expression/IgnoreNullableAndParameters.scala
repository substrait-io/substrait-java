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

import io.substrait.`type`.Type
import io.substrait.function.{ParameterizedType, ParameterizedTypeVisitor}

import scala.annotation.nowarn

class IgnoreNullableAndParameters(val typeToMatch: ParameterizedType)
  extends ParameterizedTypeVisitor[Boolean, RuntimeException] {

  override def visit(`type`: Type.Bool): Boolean = typeToMatch.isInstanceOf[Type.Bool]

  override def visit(`type`: Type.I8): Boolean = typeToMatch.isInstanceOf[Type.I8]

  override def visit(`type`: Type.I16): Boolean = typeToMatch.isInstanceOf[Type.I16]

  override def visit(`type`: Type.I32): Boolean = typeToMatch.isInstanceOf[Type.I32]

  override def visit(`type`: Type.I64): Boolean = typeToMatch.isInstanceOf[Type.I64]

  override def visit(`type`: Type.FP32): Boolean = typeToMatch.isInstanceOf[Type.FP32]

  override def visit(`type`: Type.FP64): Boolean = typeToMatch.isInstanceOf[Type.FP64]

  override def visit(`type`: Type.Str): Boolean = typeToMatch.isInstanceOf[Type.Str]

  override def visit(`type`: Type.Binary): Boolean = typeToMatch.isInstanceOf[Type.Binary]

  override def visit(`type`: Type.Date): Boolean = typeToMatch.isInstanceOf[Type.Date]

  override def visit(`type`: Type.Time): Boolean = typeToMatch.isInstanceOf[Type.Time]

  @nowarn
  override def visit(`type`: Type.TimestampTZ): Boolean = typeToMatch.isInstanceOf[Type.TimestampTZ]

  @nowarn
  override def visit(`type`: Type.Timestamp): Boolean = typeToMatch.isInstanceOf[Type.Timestamp]

  override def visit(`type`: Type.IntervalYear): Boolean =
    typeToMatch.isInstanceOf[Type.IntervalYear]

  override def visit(`type`: Type.IntervalDay): Boolean =
    typeToMatch.isInstanceOf[Type.IntervalDay] || typeToMatch
      .isInstanceOf[ParameterizedType.IntervalDay]

  override def visit(`type`: Type.IntervalCompound): Boolean =
    typeToMatch.isInstanceOf[Type.IntervalCompound] || typeToMatch
      .isInstanceOf[ParameterizedType.IntervalCompound]

  override def visit(`type`: Type.UUID): Boolean = typeToMatch.isInstanceOf[Type.UUID]

  override def visit(`type`: Type.FixedChar): Boolean =
    typeToMatch.isInstanceOf[Type.FixedChar] || typeToMatch
      .isInstanceOf[ParameterizedType.FixedChar]

  override def visit(`type`: Type.VarChar): Boolean =
    typeToMatch.isInstanceOf[Type.VarChar] || typeToMatch.isInstanceOf[ParameterizedType.VarChar]

  override def visit(`type`: Type.FixedBinary): Boolean =
    typeToMatch.isInstanceOf[Type.FixedBinary] || typeToMatch
      .isInstanceOf[ParameterizedType.FixedBinary]

  override def visit(`type`: Type.Decimal): Boolean =
    typeToMatch.isInstanceOf[Type.Decimal] || typeToMatch.isInstanceOf[ParameterizedType.Decimal]

  override def visit(`type`: Type.Struct): Boolean =
    typeToMatch.isInstanceOf[Type.Struct] || typeToMatch.isInstanceOf[ParameterizedType.Struct]

  override def visit(`type`: Type.ListType): Boolean =
    typeToMatch.isInstanceOf[Type.ListType] || typeToMatch.isInstanceOf[ParameterizedType.ListType]

  override def visit(`type`: Type.Map): Boolean =
    typeToMatch.isInstanceOf[Type.Map] || typeToMatch.isInstanceOf[ParameterizedType.Map]

  override def visit(`type`: Type.UserDefined): Boolean =
    typeToMatch.isInstanceOf[Type.UserDefined]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.FixedChar): Boolean =
    typeToMatch.isInstanceOf[Type.FixedChar] || typeToMatch
      .isInstanceOf[ParameterizedType.FixedChar]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.VarChar): Boolean =
    typeToMatch.isInstanceOf[Type.VarChar] || typeToMatch.isInstanceOf[ParameterizedType.VarChar]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.FixedBinary): Boolean =
    typeToMatch.isInstanceOf[Type.FixedBinary] || typeToMatch
      .isInstanceOf[ParameterizedType.FixedBinary]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.Decimal): Boolean =
    typeToMatch.isInstanceOf[Type.Decimal] || typeToMatch.isInstanceOf[ParameterizedType.Decimal]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.IntervalDay): Boolean =
    typeToMatch.isInstanceOf[Type.IntervalDay] || typeToMatch
      .isInstanceOf[ParameterizedType.IntervalDay]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.IntervalCompound): Boolean =
    typeToMatch.isInstanceOf[Type.IntervalCompound] || typeToMatch
      .isInstanceOf[ParameterizedType.IntervalCompound]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.Struct): Boolean =
    typeToMatch.isInstanceOf[Type.Struct] || typeToMatch.isInstanceOf[ParameterizedType.Struct]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.ListType): Boolean =
    typeToMatch.isInstanceOf[Type.ListType] || typeToMatch.isInstanceOf[ParameterizedType.ListType]

  @throws[RuntimeException]
  override def visit(expr: ParameterizedType.Map): Boolean =
    typeToMatch.isInstanceOf[Type.Map] || typeToMatch.isInstanceOf[ParameterizedType.Map]

  @throws[RuntimeException]
  override def visit(stringLiteral: ParameterizedType.StringLiteral): Boolean = false

  @throws[RuntimeException]
  override def visit(precisionTime: ParameterizedType.PrecisionTime): Boolean =
    typeToMatch.isInstanceOf[ParameterizedType.PrecisionTime]

  @throws[RuntimeException]
  override def visit(precisionTimestamp: ParameterizedType.PrecisionTimestamp): Boolean =
    typeToMatch.isInstanceOf[ParameterizedType.PrecisionTimestamp]

  @throws[RuntimeException]
  override def visit(precisionTimestampTZ: ParameterizedType.PrecisionTimestampTZ): Boolean =
    typeToMatch.isInstanceOf[ParameterizedType.PrecisionTimestampTZ]

  @throws[RuntimeException]
  override def visit(precisionTime: Type.PrecisionTime): Boolean =
    typeToMatch.isInstanceOf[Type.PrecisionTime]

  @throws[RuntimeException]
  override def visit(precisionTimestamp: Type.PrecisionTimestamp): Boolean =
    typeToMatch.isInstanceOf[Type.PrecisionTimestamp]

  @throws[RuntimeException]
  override def visit(precisionTimestampTZ: Type.PrecisionTimestampTZ): Boolean =
    typeToMatch.isInstanceOf[Type.PrecisionTimestampTZ]
}
