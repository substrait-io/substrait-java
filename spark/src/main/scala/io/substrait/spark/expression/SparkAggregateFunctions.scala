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

import org.apache.spark.sql.catalyst.expressions.EvalMode
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Complete, Sum}
import org.apache.spark.sql.types.LongType

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, FunctionArg, FunctionOption}
import io.substrait.extension.{DefaultExtensionCatalog, SimpleExtension}

import scala.jdk.CollectionConverters._

/** The accumulator, rounding and overflow contracts carried by the Spark extension aggregates. */
private[spark] object SparkAggregateFunctions {
  val URN = "extension:substrait:spark"
  val keys: Set[String] = Set("sum:dec", "avg:dec", "sum:i64", "avg:i64")

  def isSparkFunction(function: SimpleExtension.Function): Boolean =
    function.urn() == URN && keys.contains(function.key())

  def replaces(function: SimpleExtension.Function): Boolean =
    (function.urn() == DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL &&
      Set("sum:dec", "avg:dec").contains(function.key())) ||
      (function.urn() == DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC &&
        Set("sum:i64", "avg:i64").contains(function.key()))

  def validateResult(
      function: SimpleExtension.Function,
      arguments: Seq[FunctionArg],
      outputType: Type): Unit = {
    if (isSparkFunction(function)) {
      val inputTypes = arguments.map {
        case expression: SExpression => expression.getType
        case other =>
          throw new IllegalArgumentException(s"Expected a Spark aggregate value, got $other")
      }
      validateTypes(function, inputTypes, outputType)
    }
  }

  private def validateTypes(
      function: SimpleExtension.Function,
      inputTypes: Seq[Type],
      outputType: Type): Unit = {
    val derived = function.resolveType(inputTypes.asJava)
    if (derived != outputType) {
      throw new IllegalArgumentException(
        s"Spark ${function.key()} declares $outputType but its definition derives $derived")
    }
  }

  def options(
      expression: AggregateExpression,
      function: SimpleExtension.Function): Seq[FunctionOption] = {
    if (!isSparkFunction(function)) return Seq.empty
    if (expression.mode != Complete) {
      throw new UnsupportedOperationException(
        s"Spark extension ${function.key()} only supports complete aggregation, got ${expression.mode}")
    }
    val (mode, wraps) = expression.aggregateFunction match {
      case sum: Sum => (sum.evalMode, sum.child.dataType == LongType)
      case avg: Average => (avg.evalMode, false)
      case other => throw new UnsupportedOperationException(s"Unsupported Spark aggregate: $other")
    }
    val overflow = mode match {
      case EvalMode.ANSI => "ERROR"
      case EvalMode.LEGACY if wraps => "WRAP"
      case _ => "NULL"
    }
    Seq(FunctionOption.builder().name("overflow").addValues(overflow).build())
  }

  def configure(
      aggregate: AggregateFunction,
      function: SimpleExtension.Function,
      options: java.util.List[FunctionOption],
      phase: SExpression.AggregationPhase,
      outputType: Type): AggregateFunction = {
    if (!isSparkFunction(function)) return aggregate
    if (phase != SExpression.AggregationPhase.INITIAL_TO_RESULT) {
      throw new UnsupportedOperationException(
        s"Spark extension ${function.key()} only supports INITIAL_TO_RESULT, got $phase")
    }
    val unknown = options.asScala.filterNot(_.getName == "overflow")
    if (unknown.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Unsupported Spark aggregate options: ${unknown.map(_.getName).mkString(", ")}")
    }
    val overflow = options.asScala.filter(_.getName == "overflow")
    if (overflow.size > 1) {
      throw new IllegalArgumentException("Spark aggregate has duplicate overflow options")
    }
    val supported =
      if (function.key() == "sum:i64") Seq("WRAP", "NULL", "ERROR")
      else Seq("NULL", "ERROR")
    val preferences = overflow.headOption
      .map(_.values().asScala.toSeq)
      .getOrElse(Seq(supported.head))
    val selected = preferences.find(supported.contains).getOrElse {
      throw new UnsupportedOperationException(
        s"Unsupported overflow preferences for Spark ${function.key()}: ${preferences.mkString(", ")}")
    }
    val mode = selected match {
      case "ERROR" => EvalMode.ANSI
      case "NULL" => EvalMode.TRY
      case "WRAP" => EvalMode.LEGACY
    }
    val configured = aggregate match {
      case sum: Sum => sum.copy(evalMode = mode)
      case avg: Average => avg.copy(evalMode = mode)
      case other => throw new UnsupportedOperationException(s"Unsupported Spark aggregate: $other")
    }
    validateTypes(
      function,
      configured.children.map(child => ToSubstraitType(child.dataType, child.nullable)),
      outputType)
    val actualType = ToSubstraitType(configured.dataType, configured.nullable)
    if (actualType != outputType) {
      throw new IllegalArgumentException(
        s"Spark ${function.key()} declares $outputType but derives $actualType")
    }
    configured
  }
}
