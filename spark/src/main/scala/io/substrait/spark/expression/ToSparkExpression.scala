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
import io.substrait.spark.utils.Util

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
import io.substrait.util.EmptyVisitationContext

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

class ToSparkExpression(
    val scalarFunctionConverter: ToScalarFunction,
    val toLogicalPlan: Option[ToLogicalPlan] = None)
  extends DefaultExpressionVisitor[Expression]
  with HasOutputStack[Seq[NamedExpression]] {

  override def visit(expr: SExpression.BoolLiteral, context: EmptyVisitationContext): Expression = {
    if (expr.value()) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  override def visit(expr: SExpression.I8Literal, context: EmptyVisitationContext): Expression = {
    Literal(expr.value().asInstanceOf[Byte], ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I16Literal, context: EmptyVisitationContext): Expression = {
    Literal(expr.value().asInstanceOf[Short], ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I32Literal, context: EmptyVisitationContext): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I64Literal, context: EmptyVisitationContext): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FP32Literal, context: EmptyVisitationContext): Literal = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FP64Literal, context: EmptyVisitationContext): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.StrLiteral, context: EmptyVisitationContext): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.FixedCharLiteral,
      context: EmptyVisitationContext): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.VarCharLiteral,
      context: EmptyVisitationContext): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.BinaryLiteral, context: EmptyVisitationContext): Literal = {
    Literal(expr.value().toByteArray, ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.DecimalLiteral,
      context: EmptyVisitationContext): Expression = {
    val value = expr.value.toByteArray
    val decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale, 16)
    Literal(Decimal(decimal), ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.DateLiteral, context: EmptyVisitationContext): Expression = {
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.PrecisionTimestampLiteral,
      context: EmptyVisitationContext): Literal = {
    // Spark timestamps are stored as a microseconds Long
    Util.assertMicroseconds(expr.precision())
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.PrecisionTimestampTZLiteral,
      context: EmptyVisitationContext): Literal = {
    // Spark timestamps are stored as a microseconds Long
    Util.assertMicroseconds(expr.precision())
    Literal(expr.value(), ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.IntervalDayLiteral,
      context: EmptyVisitationContext): Literal = {
    Util.assertMicroseconds(expr.precision())
    // Spark uses a single microseconds Long as the "physical" type for DayTimeInterval
    val micros =
      (expr.days() * Util.SECONDS_PER_DAY + expr.seconds()) * Util.MICROS_PER_SECOND +
        expr.subseconds()
    Literal(micros, ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.IntervalYearLiteral,
      context: EmptyVisitationContext): Literal = {
    // Spark uses a single months Int as the "physical" type for YearMonthInterval
    val months = expr.years() * 12 + expr.months()
    Literal(months, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.ListLiteral, context: EmptyVisitationContext): Literal = {
    val array =
      expr.values().asScala.map(value => value.accept(this, context).asInstanceOf[Literal].value)
    Literal.create(array, ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.EmptyListLiteral,
      context: EmptyVisitationContext): Expression = {
    Literal.default(ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.MapLiteral, context: EmptyVisitationContext): Literal = {
    val map = expr.values().asScala.map {
      case (key, value) =>
        (
          key.accept(this, context).asInstanceOf[Literal].value,
          value.accept(this, context).asInstanceOf[Literal].value
        )
    }
    Literal.create(map, ToSparkType.convert(expr.getType))
  }

  override def visit(
      expr: SExpression.EmptyMapLiteral,
      context: EmptyVisitationContext): Literal = {
    Literal.default(ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.StructLiteral, context: EmptyVisitationContext): Literal = {
    Literal.create(
      Row.fromSeq(
        expr.fields.asScala.map(field => field.accept(this, context).asInstanceOf[Literal].value)),
      ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.NullLiteral, context: EmptyVisitationContext): Expression = {
    Literal(null, ToSparkType.convert(expr.getType))
  }

  override def visit(expr: SExpression.Cast, context: EmptyVisitationContext): Expression = {
    val childExp = expr.input().accept(this, context)
    val tt = ToSparkType.convert(expr.getType)
    val tz =
      if (Cast.needsTimeZone(childExp.dataType, tt))
        Some(SQLConf.get.getConf(SQLConf.SESSION_LOCAL_TIMEZONE))
      else
        None
    Cast(childExp, tt, tz)
  }

  override def visit(expr: exp.FieldReference, context: EmptyVisitationContext): Expression = {
    withFieldReference(expr)(i => currentOutput(i).clone())
  }

  override def visit(expr: SExpression.IfThen, context: EmptyVisitationContext): Expression = {
    val branches = expr
      .ifClauses()
      .asScala
      .map(
        ifClause => {
          val predicate = ifClause.condition().accept(this, context)
          val elseValue = ifClause.`then`().accept(this, context)
          (predicate, elseValue)
        })
    val default = expr.elseClause().accept(this, context) match {
      case l: Literal if l.nullable => None
      case other => Some(other)
    }
    CaseWhen(branches, default)
  }

  override def visit(
      expr: SExpression.ScalarSubquery,
      context: EmptyVisitationContext): Expression = {
    val rel = expr.input()
    val dataType = ToSparkType.convert(expr.getType)
    toLogicalPlan
      .map(
        relConverter => {
          val plan = rel.accept(relConverter, context)
          require(plan.resolved)
          val result = ScalarSubquery(plan)
          SparkTypeUtil.sameType(result.dataType, dataType)
          result
        })
      .getOrElse(visitFallback(expr, context))
  }

  override def visit(
      expr: SExpression.SingleOrList,
      context: EmptyVisitationContext): Expression = {
    val value = expr.condition().accept(this, context)
    val list = expr.options().asScala.map(e => e.accept(this, context))
    In(value, list)
  }

  override def visit(expr: SExpression.InPredicate, context: EmptyVisitationContext): Expression = {
    val needles = expr.needles().asScala.map(e => e.accept(this, context))
    val haystack = expr.haystack().accept(toLogicalPlan.get, context)
    new InSubquery(needles, ListQuery(haystack, childOutputs = haystack.output)) {
      override def nullable: Boolean = expr.getType.nullable()
    }
  }

  override def visitEnumArg(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      e: EnumArg,
      context: EmptyVisitationContext): Expression = {
    Enum(e.value.orElse(""))
  }

  override def visit(
      expr: SExpression.ScalarFunctionInvocation,
      context: EmptyVisitationContext): Expression = {
    val eArgs = expr.arguments().asScala
    val args = eArgs.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(expr.declaration(), i, this, context)
    }.toList

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
