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

import org.apache.spark.sql.catalyst.expressions.{Expression, MakeDecimal}

import io.substrait.`type`.{Type, TypeCreator}
import io.substrait.expression.{Expression => SExpression, FunctionArg}
import io.substrait.extension.SimpleExtension

import scala.jdk.CollectionConverters._

abstract class ToScalarFunction(functions: Seq[SimpleExtension.ScalarFunctionVariant])
  extends FunctionConverter[SimpleExtension.ScalarFunctionVariant, SExpression](functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.ScalarFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): SExpression = {
    if (ToScalarFunction.isDecimalConstructor(function)) {
      val types = arguments.map {
        case e: SExpression => e.getType
        case t: Type => t
        case other => throw new IllegalArgumentException(s"Unexpected decimal argument: $other")
      }
      require(
        function.resolveType(types.asJava) == outputType,
        "Decimal constructor output type mismatch")
    }
    SExpression.ScalarFunctionInvocation
      .builder()
      .outputType(outputType)
      .declaration(function)
      .addAllArguments(arguments.asJava)
      .build()
  }

  def convert(expression: Expression, operands: Seq[FunctionArg]): Option[SExpression] = {
    expression match {
      case decimal: MakeDecimal =>
        val name = if (decimal.nullOnOverflow) "try_make_decimal" else "make_decimal"
        functions
          .find(f => f.urn() == SparkAggregateFunctions.URN && f.key() == s"$name:i64_type")
          .map(
            f =>
              generateBinding(
                decimal,
                f,
                operands :+ TypeCreator.REQUIRED.decimal(decimal.precision, decimal.scale),
                ToSubstraitType(decimal.dataType, decimal.nullable)))
      case _ =>
        Option(signatures.get(expression.getClass))
          .flatMap(m => m.attemptMatch(expression, operands))
    }
  }
}

object ToScalarFunction {
  private[spark] val decimalConstructorKeys =
    Set("make_decimal:i64_type", "try_make_decimal:i64_type")

  private[spark] def isDecimalConstructor(function: SimpleExtension.Function): Boolean =
    function.urn() == SparkAggregateFunctions.URN && decimalConstructorKeys.contains(function.key())

  def apply(functions: Seq[SimpleExtension.ScalarFunctionVariant]): ToScalarFunction = {
    new ToScalarFunction(functions) {
      override def getSigs: Seq[Sig] = FunctionMappings.SCALAR_SIGS
    }
  }
}
