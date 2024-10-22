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

import io.substrait.spark.expression.ToWindowFunction.{fromSpark, fromSparkFollowing, fromSparkPreceding}

import org.apache.spark.sql.catalyst.expressions.{CurrentRow, Expression, FrameType, Literal, OffsetWindowFunction, RangeFrame, RowFrame, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnspecifiedFrame, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.types.{IntegerType, LongType}

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, ExpressionCreator, FunctionArg, WindowBound}
import io.substrait.expression.Expression.WindowBoundsType
import io.substrait.expression.WindowBound.{CURRENT_ROW, Following, Preceding, UNBOUNDED}
import io.substrait.extension.SimpleExtension
import io.substrait.relation.ConsistentPartitionWindow.WindowRelFunctionInvocation

import scala.collection.JavaConverters

abstract class ToWindowFunction(functions: Seq[SimpleExtension.WindowFunctionVariant])
  extends FunctionConverter[SimpleExtension.WindowFunctionVariant, WindowRelFunctionInvocation](
    functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.WindowFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): WindowRelFunctionInvocation = {

    val (frameType, lower, upper) = sparkExp match {
      case WindowExpression(_: OffsetWindowFunction, _) =>
        (WindowBoundsType.ROWS, UNBOUNDED, CURRENT_ROW)
      case WindowExpression(
            _,
            WindowSpecDefinition(_, _, SpecifiedWindowFrame(frameType, lower, upper))) =>
        (fromSpark(frameType), fromSparkPreceding(lower), fromSparkFollowing(upper))
      case WindowExpression(_, WindowSpecDefinition(_, orderSpec, UnspecifiedFrame)) =>
        if (orderSpec.isEmpty) {
          (WindowBoundsType.ROWS, UNBOUNDED, UNBOUNDED)
        } else {
          (WindowBoundsType.RANGE, UNBOUNDED, CURRENT_ROW)
        }

      case _ => throw new UnsupportedOperationException(s"Unsupported window expression: $sparkExp")
    }

    ExpressionCreator.windowRelFunction(
      function,
      outputType,
      SExpression.AggregationPhase.INITIAL_TO_RESULT, // use defaults...
      SExpression.AggregationInvocation.ALL, // Spark doesn't define these
      frameType,
      lower,
      upper,
      JavaConverters.asJavaIterable(arguments)
    )
  }

  def convert(
      expression: WindowExpression,
      operands: Seq[SExpression]): Option[WindowRelFunctionInvocation] = {
    val cls = expression.windowFunction match {
      case agg: AggregateExpression => agg.aggregateFunction.getClass
      case other => other.getClass
    }

    Option(signatures.get(cls))
      .flatMap(m => m.attemptMatch(expression, operands))
  }

  def apply(
      expression: WindowExpression,
      operands: Seq[SExpression]): WindowRelFunctionInvocation = {
    convert(expression, operands).getOrElse(throw new UnsupportedOperationException(
      s"Unable to find binding for call ${expression.windowFunction} -- $operands -- $expression"))
  }
}

object ToWindowFunction {
  def fromSpark(frameType: FrameType): WindowBoundsType = frameType match {
    case RowFrame => WindowBoundsType.ROWS
    case RangeFrame => WindowBoundsType.RANGE
    case other => throw new UnsupportedOperationException(s"Unsupported bounds type: $other.")
  }

  def toSpark(boundsType: WindowBoundsType): FrameType = boundsType match {
    case WindowBoundsType.ROWS => RowFrame
    case WindowBoundsType.RANGE => RangeFrame
    case other => throw new UnsupportedOperationException(s"Unsupported bounds type: $other.")
  }

  def fromSparkPreceding(bound: Expression): WindowBound = bound match {
    case UnboundedPreceding => UNBOUNDED
    case CurrentRow => CURRENT_ROW
    case Literal(i: Int, IntegerType) => Preceding.of(i.toLong)
    case Literal(l: Long, LongType) => Preceding.of(l)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported bounds expression ${bound.getClass}")
  }

  def fromSparkFollowing(bound: Expression): WindowBound = bound match {
    case UnboundedFollowing => UNBOUNDED
    case CurrentRow => CURRENT_ROW
    case Literal(i: Int, IntegerType) => Following.of(i.toLong)
    case Literal(l: Long, LongType) => Following.of(l)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported bounds expression ${bound.getClass}")
  }

  def toSparkPreceding(bound: WindowBound): Expression = bound match {
    case UNBOUNDED => UnboundedPreceding
    case CURRENT_ROW => CurrentRow
    case p: Preceding => Literal(p.offset())
    case _ => throw new UnsupportedOperationException(s"Unsupported bounds expression $bound")
  }

  def toSparkFollowing(bound: WindowBound): Expression = bound match {
    case UNBOUNDED => UnboundedFollowing
    case CURRENT_ROW => CurrentRow
    case f: Following => Literal(f.offset())
    case _ => throw new UnsupportedOperationException(s"Unsupported bounds expression $bound")
  }

  def apply(functions: Seq[SimpleExtension.WindowFunctionVariant]): ToWindowFunction = {
    new ToWindowFunction(functions) {
      override def getSigs: Seq[Sig] =
        FunctionMappings.WINDOW_SIGS ++ FunctionMappings.AGGREGATE_SIGS
    }
  }

}
