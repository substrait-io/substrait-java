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
package io.substrait.spark

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types._

import io.substrait.`type`.{NamedStruct, Type, TypeVisitor}
import io.substrait.function.TypeExpression
import io.substrait.utils.Util

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

private class ToSparkType
  extends TypeVisitor.TypeThrowsVisitor[DataType, RuntimeException]("Unknown expression type.") {

  override def visit(expr: Type.I8): DataType = ByteType
  override def visit(expr: Type.I16): DataType = ShortType
  override def visit(expr: Type.I32): DataType = IntegerType
  override def visit(expr: Type.I64): DataType = LongType

  override def visit(expr: Type.FP32): DataType = FloatType
  override def visit(expr: Type.FP64): DataType = DoubleType

  override def visit(expr: Type.Decimal): DataType =
    DecimalType(expr.precision(), expr.scale())

  override def visit(expr: Type.Date): DataType = DateType

  override def visit(expr: Type.Str): DataType = StringType

  override def visit(expr: Type.Binary): DataType = BinaryType

  override def visit(expr: Type.FixedChar): DataType = StringType

  override def visit(expr: Type.VarChar): DataType = StringType

  override def visit(expr: Type.Bool): DataType = BooleanType

  override def visit(expr: Type.PrecisionTimestamp): DataType = {
    Util.assertMicroseconds(expr.precision())
    TimestampNTZType
  }
  override def visit(expr: Type.PrecisionTimestampTZ): DataType = {
    Util.assertMicroseconds(expr.precision())
    TimestampType
  }

  override def visit(expr: Type.IntervalDay): DataType = {
    Util.assertMicroseconds(expr.precision())
    DayTimeIntervalType.DEFAULT
  }

  override def visit(expr: Type.IntervalYear): DataType = YearMonthIntervalType.DEFAULT

  override def visit(expr: Type.Struct): DataType = {
    StructType(
      expr.fields.asScala.zipWithIndex
        .map { case (t, i) => StructField(s"col${i + 1}", t.accept(this), t.nullable()) }
    )
  }

  override def visit(expr: Type.ListType): DataType =
    ArrayType(expr.elementType().accept(this), containsNull = expr.elementType().nullable())

  override def visit(expr: Type.Map): DataType =
    MapType(
      expr.key().accept(this),
      expr.value().accept(this),
      valueContainsNull = expr.value().nullable())
}
class ToSubstraitType {

  def convert(typeExpression: TypeExpression): DataType = {
    typeExpression.accept(new ToSparkType)
  }

  def convert(dataType: DataType, nullable: Boolean): Option[Type] = {
    convert(dataType, Seq.empty, nullable)
  }

  def apply(dataType: DataType, nullable: Boolean): Type = {
    convert(dataType, Seq.empty, nullable)
      .getOrElse(
        throw new UnsupportedOperationException(s"Unable to convert the type ${dataType.typeName}"))
  }

  protected def convert(dataType: DataType, names: Seq[String], nullable: Boolean): Option[Type] = {
    val creator = Type.withNullability(nullable)
    dataType match {
      case BooleanType => Some(creator.BOOLEAN)
      case ByteType => Some(creator.I8)
      case ShortType => Some(creator.I16)
      case IntegerType => Some(creator.I32)
      case LongType => Some(creator.I64)
      case FloatType => Some(creator.FP32)
      case DoubleType => Some(creator.FP64)
      case decimal: DecimalType if decimal.precision <= 38 =>
        Some(creator.decimal(decimal.precision, decimal.scale))
      case charType: CharType => Some(creator.fixedChar(charType.length))
      case varcharType: VarcharType => Some(creator.varChar(varcharType.length))
      case StringType => Some(creator.STRING)
      case BinaryType => Some(creator.BINARY)
      case DateType => Some(creator.DATE)
      case TimestampNTZType => Some(creator.precisionTimestamp(Util.MICROSECOND_PRECISION))
      case TimestampType => Some(creator.precisionTimestampTZ(Util.MICROSECOND_PRECISION))
      case DayTimeIntervalType.DEFAULT => Some(creator.intervalDay(Util.MICROSECOND_PRECISION))
      case YearMonthIntervalType.DEFAULT => Some(creator.INTERVAL_YEAR)
      case ArrayType(elementType, containsNull) =>
        convert(elementType, Seq.empty, containsNull).map(creator.list)
      case MapType(keyType, valueType, valueContainsNull) =>
        convert(keyType, Seq.empty, nullable = false)
          .flatMap(
            keyT =>
              convert(valueType, Seq.empty, valueContainsNull)
                .map(valueT => creator.map(keyT, valueT)))
      case _ =>
        None
    }
  }
  def toNamedStruct(output: Seq[Attribute]): Option[NamedStruct] = {
    val names = JavaConverters.seqAsJavaList(output.map(_.name))
    val creator = Type.withNullability(false)
    Util
      .seqToOption(output.map(a => convert(a.dataType, a.nullable)))
      .map(l => creator.struct(JavaConverters.asJavaIterable(l)))
      .map(NamedStruct.of(names, _))
  }
  def toNamedStruct(schema: StructType): NamedStruct = {
    val creator = Type.withNullability(false)
    val names = new java.util.ArrayList[String]
    val children = new java.util.ArrayList[Type]
    schema.fields.foreach(
      field => {
        names.add(field.name)
        children.add(apply(field.dataType, field.nullable))
      })
    val struct = creator.struct(children)
    NamedStruct.of(names, struct)
  }

  def toStructType(namedStruct: NamedStruct): StructType = {
    StructType(
      fields = namedStruct
        .struct()
        .fields()
        .asScala
        .map(t => (t, convert(t)))
        .zip(namedStruct.names().asScala)
        .map { case ((t, d), name) => StructField(name, d, t.nullable()) }
    )
  }

  def toAttributeSeq(namedStruct: NamedStruct): Seq[AttributeReference] = {
    namedStruct
      .struct()
      .fields()
      .asScala
      .map(t => (t, convert(t)))
      .zip(namedStruct.names().asScala)
      .map { case ((t, d), name) => StructField(name, d, t.nullable()) }
      .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }
}

object ToSubstraitType extends ToSubstraitType
