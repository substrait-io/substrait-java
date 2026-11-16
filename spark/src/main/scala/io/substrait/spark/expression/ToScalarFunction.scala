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

import org.apache.spark.sql.catalyst.expressions.{Add, EvalMode, Expression, Multiply, Subtract}

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, FunctionArg}
import io.substrait.extension.{DefaultExtensionCatalog, SimpleExtension}

import scala.jdk.CollectionConverters._

abstract class ToScalarFunction(functions: Seq[SimpleExtension.ScalarFunctionVariant])
  extends FunctionConverter[SimpleExtension.ScalarFunctionVariant, SExpression](functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.ScalarFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): SExpression = {
    SExpression.ScalarFunctionInvocation
      .builder()
      .outputType(outputType)
      .declaration(function)
      .addAllArguments(arguments.asJava)
      .build()
  }

  def convert(expression: Expression, operands: Seq[FunctionArg]): Option[SExpression] = {
    Option(signatures.get(expression.getClass))
      .flatMap(m => m.attemptMatch(expression, operands))
  }

  def getSparkExpressionFromSubstraitFunc(
      invocation: SExpression.ScalarFunctionInvocation,
      args: Seq[Expression]): Option[Expression] = {
    getSparkExpressionFromSubstraitFunc(invocation.declaration().key(), args)
      .map(expression => resolveDecimalOverflow(invocation, expression))
  }

  private def resolveDecimalOverflow(
      invocation: SExpression.ScalarFunctionInvocation,
      expression: Expression): Expression = {
    val declaration = invocation.declaration()
    if (
      invocation.options().isEmpty ||
      declaration.urn() != DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL ||
      !Set("add:dec_dec", "subtract:dec_dec", "multiply:dec_dec").contains(declaration.key())
    ) {
      return expression
    }
    val declaredValues = declaration.options().get("overflow").getValues.asScala
    invocation.options().asScala.foreach {
      option =>
        if (!option.getName.equalsIgnoreCase("overflow")) {
          throw new UnsupportedOperationException(
            s"Unsupported decimal arithmetic option: ${option.getName}")
        }
        val values = option.values().asScala
        if (
          !values.forall(value => declaredValues.exists(_.equalsIgnoreCase(value))) ||
          !values.exists(_.equalsIgnoreCase("ERROR"))
        ) {
          throw new UnsupportedOperationException(
            s"Unsupported overflow preferences for ${declaration.key()}: ${option.values()}")
        }
    }
    expression match {
      case add: Add => add.copy(evalMode = EvalMode.ANSI)
      case subtract: Subtract => subtract.copy(evalMode = EvalMode.ANSI)
      case multiply: Multiply => multiply.copy(evalMode = EvalMode.ANSI)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot honor overflow=ERROR for ${declaration.key()}")
    }
  }
}

object ToScalarFunction {
  def apply(functions: Seq[SimpleExtension.ScalarFunctionVariant]): ToScalarFunction = {
    new ToScalarFunction(functions) {
      override def getSigs: Seq[Sig] = FunctionMappings.SCALAR_SIGS
    }
  }
}
