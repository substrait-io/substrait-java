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
package io.substrait.debug

import io.substrait.spark.DefaultExpressionVisitor

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import io.substrait.expression.{Expression, FieldReference}
import io.substrait.expression.Expression.{DateLiteral, DecimalLiteral, I32Literal, I64Literal, StrLiteral}
import io.substrait.function.ToTypeString
import io.substrait.util.DecimalUtil
import io.substrait.util.EmptyVisitationContext

import scala.collection.JavaConverters.asScalaBufferConverter

class ExpressionToString extends DefaultExpressionVisitor[String] {

  override def visit(expr: DecimalLiteral, context: EmptyVisitationContext): String = {
    val value = expr.value.toByteArray
    val decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale, 16)
    decimal.toString
  }

  override def visit(expr: StrLiteral, context: EmptyVisitationContext): String = {
    expr.value()
  }

  override def visit(expr: I32Literal, context: EmptyVisitationContext): String = {
    expr.value().toString
  }

  override def visit(expr: I64Literal, context: EmptyVisitationContext): String = {
    expr.value().toString
  }

  override def visit(expr: DateLiteral, context: EmptyVisitationContext): String = {
    DateTimeUtils.toJavaDate(expr.value()).toString
  }

  override def visit(expr: FieldReference, context: EmptyVisitationContext): String = {
    withFieldReference(expr)(i => "$" + i.toString)
  }

  override def visit(expr: Expression.SingleOrList, context: EmptyVisitationContext): String = {
    expr.toString
  }

  override def visit(
      expr: Expression.ScalarFunctionInvocation,
      context: EmptyVisitationContext): String = {
    val args = expr
      .arguments()
      .asScala
      .zipWithIndex
      .map {
        case (arg, i) =>
          arg.accept(expr.declaration(), i, this, context)
      }
      .mkString(",")

    s"${expr.declaration().key()}[${expr.outputType().accept(ToTypeString.INSTANCE)}]($args)"
  }

  override def visit(
      expr: Expression.UserDefinedLiteral,
      context: EmptyVisitationContext): String = {
    expr.toString
  }

  override def visit(expr: Expression.EmptyMapLiteral, context: EmptyVisitationContext): String = {
    expr.toString
  }

  override def visit(expr: Expression.Cast, context: EmptyVisitationContext): String = {
    expr.getType.toString
  }

  override def visit(expr: Expression.InPredicate, context: EmptyVisitationContext): String = {
    expr.toString
  }

  override def visit(expr: Expression.ScalarSubquery, context: EmptyVisitationContext): String = {
    expr.toString
  }
}
