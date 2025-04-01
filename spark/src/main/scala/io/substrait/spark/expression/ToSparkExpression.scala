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

import io.substrait.spark.{DefaultExpressionVisitor, HasOutputStack, SparkExtension, ToSparkType}
import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Cast, Expression, In, InSubquery, ListQuery, Literal, MakeDecimal, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.substrait.SparkTypeUtil
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{EnumArg, Expression => SExpression}
import io.substrait.extension.SimpleExtension
import io.substrait.util.DecimalUtil
import io.substrait.utils.Util

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

class ToSparkExpression(
    val scalarFunctionConverter: ToScalarFunction,
    val toLogicalPlan: Option[ToLogicalPlan] = None)
  extends DefaultExpressionVisitor[Expression]
  with HasOutputStack[Seq[NamedExpression]] {

  override def visit(expr: SExpression.BoolLiteral): Expression = {
    if (expr.value()) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  override def visit(expr: SExpression.I8Literal): Expression = {
    Literal(expr.value().asInstanceOf[Byte], ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I16Literal): Expression = {
    Literal(expr.value().asInstanceOf[Short], ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I32Literal): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I64Literal): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FP32Literal): Literal = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FP64Literal): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.StrLiteral): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FixedCharLiteral): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.VarCharLiteral): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.BinaryLiteral): Literal = {
    Literal(expr.value().toByteArray, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.DecimalLiteral): Expression = {
    val value = expr.value.toByteArray
    val decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale, 16)
    Literal(Decimal(decimal), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.DateLiteral): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.PrecisionTimestampLiteral): Literal = {
    // Spark timestamps are stored as a microseconds Long
    Util.assertMicroseconds(expr.precision())
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.PrecisionTimestampTZLiteral): Literal = {
    // Spark timestamps are stored as a microseconds Long
    Util.assertMicroseconds(expr.precision())
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.IntervalDayLiteral): Literal = {
    Util.assertMicroseconds(expr.precision())
    // Spark uses a single microseconds Long as the "physical" type for DayTimeInterval
    val micros =
      (expr.days() * Util.SECONDS_PER_DAY + expr.seconds()) * Util.MICROS_PER_SECOND +
        expr.subseconds()
    Literal(micros, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.IntervalYearLiteral): Literal = {
    // Spark uses a single months Int as the "physical" type for YearMonthInterval
    val months = expr.years() * 12 + expr.months()
    Literal(months, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.ListLiteral): Literal = {
    val array = expr.values().asScala.map(value => value.accept(this).asInstanceOf[Literal].value)
    Literal.create(array, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.EmptyListLiteral): Expression = {
    Literal.default(ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.MapLiteral): Literal = {
    val map = expr.values().asScala.map {
      case (key, value) =>
        (
          key.accept(this).asInstanceOf[Literal].value,
          value.accept(this).asInstanceOf[Literal].value
        )
    }
    Literal.create(map, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.EmptyMapLiteral): Literal = {
    Literal.default(ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.StructLiteral): Literal = {
    Literal.create(
      Row.fromSeq(expr.fields.asScala.map(field => field.accept(this).asInstanceOf[Literal].value)),
      ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.NullLiteral): Expression = {
    Literal(null, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.Cast): Expression = {
    val childExp = expr.input().accept(this)
    val tt = ToSparkType.convert(expr.getType)
    val tz =
      if (Cast.needsTimeZone(childExp.dataType, tt))
        Some(SQLConf.get.getConf(SQLConf.SESSION_LOCAL_TIMEZONE))
      else
        None
    Cast(childExp, tt, tz)
  }

  override def visit(expr: exp.FieldReference): Expression = {
    withFieldReference(expr)(i => currentOutput(i).clone())
  }

  override def visit(expr: SExpression.IfThen): Expression = {
    val branches = expr
      .ifClauses()
      .asScala
      .map(
        ifClause => {
          val predicate = ifClause.condition().accept(this)
          val elseValue = ifClause.`then`().accept(this)
          (predicate, elseValue)
        })
    val default = expr.elseClause().accept(this) match {
      case l: Literal if l.nullable => None
      case other => Some(other)
    }
    CaseWhen(branches, default)
  }

  override def visit(expr: SExpression.ScalarSubquery): Expression = {
    val rel = expr.input()
    val dataType = ToSparkType.convert(expr.getType)
    toLogicalPlan
      .map(
        relConverter => {
          val plan = rel.accept(relConverter)
          require(plan.resolved)
          val result = ScalarSubquery(plan)
          SparkTypeUtil.sameType(result.dataType, dataType)
          result
        })
      .getOrElse(visitFallback(expr))
  }

  override def visit(expr: SExpression.SingleOrList): Expression = {
    val value = expr.condition().accept(this)
    val list = expr.options().asScala.map(e => e.accept(this))
    In(value, list)
  }

  override def visit(expr: SExpression.InPredicate): Expression = {
    val needles = expr.needles().asScala.map(e => e.accept(this))
    val haystack = expr.haystack().accept(toLogicalPlan.get)
    new InSubquery(needles, ListQuery(haystack, childOutputs = haystack.output)) {
      override def nullable: Boolean = expr.getType.nullable()
    }
  }

  override def visitEnumArg(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      e: EnumArg): Expression = {
    Enum(e.value.orElse(""))
  }

  override def visit(expr: SExpression.ScalarFunctionInvocation): Expression = {
    val eArgs = expr.arguments().asScala
    val args = eArgs.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(expr.declaration(), i, this)
    }.toList

    expr.declaration.name match {
      case "make_decimal" if expr.declaration.uri == SparkExtension.uri =>
        expr.outputType match {
          // Need special case handing of this internal function.
          // Because the precision and scale arguments are extracted from the output type,
          // we can't use the generic scalar function conversion mechanism here.
          case d: Type.Decimal => MakeDecimal(args.head, d.precision, d.scale)
          case _ =>
            throw new IllegalArgumentException("Output type of MakeDecimal must be a decimal type")
        }
      case _ =>
        scalarFunctionConverter
          .getSparkExpressionFromSubstraitFunc(expr.declaration.key, args)
          .getOrElse({
            val msg = String.format(
              "Unable to convert scalar function %s(%s).",
              expr.declaration.name,
              expr.arguments.asScala
                .map {
                  case ea: exp.EnumArg => ea.value.toString
                  case e: SExpression => e.getType.accept(new StringTypeVisitor)
                  case t: Type => t.accept(new StringTypeVisitor)
                  case a => throw new IllegalStateException("Unexpected value: " + a)
                }
                .mkString(", ")
            )
            throw new IllegalArgumentException(msg)
          })
    }
  }
}
