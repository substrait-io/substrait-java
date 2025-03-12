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

/**
 * @param dfsNames
 *   a flattened depth-first listing of column/struct field names. If empty, names are
 *   auto-generated.
 */
private class ToSparkType(dfsNames: Seq[String])
  extends TypeVisitor.TypeThrowsVisitor[DataType, RuntimeException]("Unknown expression type.") {

  var nameIdx: Int = 0 // where in dfsNames we're currently at

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

  override def visit(expr: Type.ListType): DataType =
    ArrayType(expr.elementType().accept(this), containsNull = expr.elementType().nullable())

  override def visit(expr: Type.Map): DataType =
    MapType(
      expr.key().accept(this),
      expr.value().accept(this),
      valueContainsNull = expr.value().nullable())

  override def visit(expr: Type.Struct): DataType =
    StructType(
      expr.fields.asScala.zipWithIndex
        .map {
          case (f, i) =>
            val name = if (dfsNames.nonEmpty) {
              require(nameIdx < dfsNames.size)
              val n = dfsNames(nameIdx)
              nameIdx += 1
              n
            } else {
              // If names are not given, default to "col1", "col2", ... like Spark
              s"col${i + 1}"
            }
            StructField(name, f.accept(this), f.nullable())
        })

  def convert(typeExpression: TypeExpression): DataType = {
    typeExpression.accept(this)
  }
}

object ToSparkType {
  def convert(typeExpression: TypeExpression): DataType = {
    typeExpression.accept(new ToSparkType(Seq.empty))
  }

  def toStructType(namedStruct: NamedStruct): StructType = {
    new ToSparkType(namedStruct.names().asScala)
      .visit(namedStruct.struct())
      .asInstanceOf[StructType]
  }

  def toAttributeSeq(namedStruct: NamedStruct): Seq[AttributeReference] = {
    toStructType(namedStruct).fields
      .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }
}

class ToSubstraitType {
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
      case StructType(fields) =>
        if (fields.isEmpty) {
          Some(creator.struct(JavaConverters.asJavaIterable(Seq.empty)))
        } else {
          Util
            .seqToOption(fields.map(f => convert(f.dataType, f.nullable)))
            .map(l => creator.struct(JavaConverters.asJavaIterable(l)))
        }
      case _ => None
    }
  }

  def toNamedStruct(schema: StructType): NamedStruct = {
    val dfsNames = JavaConverters.seqAsJavaList(fieldNamesDfs(schema))
    val types = JavaConverters.seqAsJavaList(
      schema.fields.map(field => apply(field.dataType, field.nullable)))
    val struct = Type.withNullability(false).struct(types)
    NamedStruct.of(dfsNames, struct)
  }

  private def fieldNamesDfs(dtype: DataType): Seq[String] = {
    // NamedStruct expects a flattened list of the full tree of names, including subtype names
    dtype match {
      case StructType(fields) =>
        fields.flatMap(field => Seq(field.name) ++ fieldNamesDfs(field.dataType))
      case ArrayType(elementType, _) => fieldNamesDfs(elementType)
      case MapType(keyType, valueType, _) => fieldNamesDfs(keyType) ++ fieldNamesDfs(valueType)
      case _ => Seq()
    }
  }
}

object ToSubstraitType extends ToSubstraitType
