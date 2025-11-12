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

import io.substrait.`type`.Type
import io.substrait.expression._
import io.substrait.extension.SimpleExtension
import io.substrait.util.EmptyVisitationContext

class DefaultExpressionVisitor[T]
  extends AbstractExpressionVisitor[T, EmptyVisitationContext, RuntimeException]
  with FunctionArg.FuncArgVisitor[T, EmptyVisitationContext, RuntimeException] {

  override def visitFallback(expr: Expression, context: EmptyVisitationContext): T =
    throw new UnsupportedOperationException(
      s"Expression type ${expr.getClass.getCanonicalName} " +
        s"not handled by visitor type ${getClass.getCanonicalName}.")

  override def visitType(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      t: Type,
      context: EmptyVisitationContext): T =
    throw new UnsupportedOperationException(
      s"FunctionArg $t not handled by visitor type ${getClass.getCanonicalName}.")

  override def visitEnumArg(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      e: EnumArg,
      context: EmptyVisitationContext): T =
    throw new UnsupportedOperationException(
      s"EnumArg(value=${e.value()}) not handled by visitor type ${getClass.getCanonicalName}.")

  protected def withFieldReference(fieldReference: FieldReference)(f: Int => T): T = {
    if (fieldReference.isSimpleRootReference) {
      val segment = fieldReference.segments().get(0)
      segment match {
        case s: FieldReference.StructField => f(s.offset())
        case _ => throw new IllegalArgumentException(s"Unhandled type: $segment")
      }
    } else {
      visitFallback(fieldReference, null)
    }
  }

  override def visitExpr(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      e: Expression,
      context: EmptyVisitationContext): T =
    e.accept(this, context)

  override def visit(expr: Expression.UserDefinedAny, context: EmptyVisitationContext): T =
    visitFallback(expr, context)

  override def visit(expr: Expression.UserDefinedStruct, context: EmptyVisitationContext): T =
    visitFallback(expr, context)
}
