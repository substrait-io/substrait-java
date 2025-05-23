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

import io.substrait.spark.{HasOutputStack, ToSubstraitType}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.types.LongType
import org.apache.spark.substrait.SparkTypeUtil

import io.substrait.expression.{EnumArg, Expression => SExpression, ExpressionCreator, FieldReference, ImmutableEnumArg, ImmutableExpression}
import io.substrait.expression.Expression.FailureBehavior
import io.substrait.utils.Util

import java.util.Optional

import scala.collection.JavaConverters.asJavaIterableConverter

/** The builder to generate substrait expressions from catalyst expressions. */
abstract class ToSubstraitExpression extends HasOutputStack[Seq[Attribute]] {

  object ScalarFunction {
    def unapply(e: Expression): Option[Seq[Expression]] = e match {
      case DateFunction(arguments) => Some(arguments)
      case BinaryExpression(left, right) => Some(Seq(left, right))
      case UnaryExpression(child) => Some(Seq(child))
      case t: TernaryExpression => Some(Seq(t.first, t.second, t.third))
      case Coalesce(children) => Some(children.toList)
      case Concat(children) => Some(children.toList)
      case TrimFunction(arguments) => Some(arguments)
      case _ => None
    }
  }

  object MergedStruct {
    // This matches the structure produced by the spark optimizer and translates it back to
    // its original form to make it substrait friendly.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/MergeScalarSubqueries.scala#L76
    def unapply(e: Expression): Option[ScalarSubquery] = e match {
      case GetStructField(
            ScalarSubquery(
              Project(Seq(Alias(CreateNamedStruct(args), "mergedValue")), child),
              _,
              _,
              _,
              _,
              _),
            ordinal,
            _) =>
        val (name, value) =
          args.grouped(2).map { case Seq(name, value) => (name, value) }.toArray.apply(ordinal)

        child match {
          case Aggregate(groupingExpressions, aggregateExpressions, child)
              if aggregateExpressions.forall(e => e.isInstanceOf[Alias]) =>
            val used = value match {
              case ref: AttributeReference => ref.exprId.id
              case _ => throw new UnsupportedOperationException(s"Cannot convert expression: $e")
            }
            val filteredAggExprs = aggregateExpressions.filter(ae => used == ae.exprId.id)
            Some(
              ScalarSubquery(
                Aggregate(groupingExpressions, filteredAggExprs, child)
              )
            )
          case _ =>
            Some(
              ScalarSubquery(
                Project(Seq(Alias(value, name.toString())()), child)
              )
            )
        }
      case _ => None
    }
  }

  type OutputT = Seq[Attribute]

  protected val toScalarFunction: ToScalarFunction

  protected def default(e: Expression): Option[SExpression] = {
    throw new UnsupportedOperationException(s"Unable to convert the expression $e")
  }

  def apply(e: Expression, output: OutputT = Nil): SExpression = {
    convert(e, output).getOrElse(
      throw new UnsupportedOperationException(s"Unable to convert the expression $e")
    )
  }
  def convert(expr: Expression, output: OutputT = Nil): Option[SExpression] = {
    pushOutput(output)
    try {
      translateUp(expr)
    } finally {
      popOutput()
    }
  }

  protected def translateSubQuery(expr: PlanExpression[_]): Option[SExpression] = default(expr)

  protected def translateInSubquery(expr: InSubquery): Option[SExpression] = default(expr)

  protected def translateAttribute(a: AttributeReference): Option[SExpression] = {
    val bindReference =
      BindReferences.bindReference[Expression](a, currentOutput, allowFailures = false)
    if (bindReference == a) {
      default(a)
    } else {
      Some(
        FieldReference.newRootStructReference(
          bindReference.asInstanceOf[BoundReference].ordinal,
          ToSubstraitType.apply(a.dataType, a.nullable))
      )
    }
  }

  protected def translateCaseWhen(
      branches: Seq[(Expression, Expression)],
      elseValue: Option[Expression]): Option[SExpression] = {
    val cases =
      for ((predicate, trueValue) <- branches)
        yield translateUp(predicate).flatMap(
          p =>
            translateUp(trueValue).map(
              t => {
                ImmutableExpression.IfClause.builder
                  .condition(p)
                  .`then`(t)
                  .build()
              }))
    val sparkElse = elseValue.getOrElse(Literal.create(null, branches.head._2.dataType))
    Util
      .seqToOption(cases.toList)
      .flatMap(
        caseConditions =>
          translateUp(sparkElse).map(
            defaultResult => {
              ExpressionCreator.ifThenStatement(defaultResult, caseConditions.asJava)
            }))
  }
  protected def translateIn(value: Expression, list: Seq[Expression]): Option[SExpression] = {
    Util
      .seqToOption(list.map(translateUp).toList)
      .flatMap(
        inList =>
          translateUp(value).map(
            inValue => {
              SExpression.SingleOrList
                .builder()
                .condition(inValue)
                .options(inList.asJava)
                .build()
            }))
  }

  protected def translateUp(expr: Expression): Option[SExpression] = {
    expr match {
      case c @ Cast(child, dataType, _, _) =>
        translateUp(child)
          .map(ExpressionCreator
            .cast(ToSubstraitType.apply(dataType, c.nullable), _, FailureBehavior.THROW_EXCEPTION))
      case UnscaledValue(value) => translateUp(Cast(value, LongType))
      case c @ CheckOverflow(child, dataType, _) =>
        // CheckOverflow similar with cast
        translateUp(child)
          .map(
            childExpr => {
              if (SparkTypeUtil.sameType(dataType, child.dataType)) {
                childExpr
              } else {
                ExpressionCreator.cast(
                  ToSubstraitType.apply(dataType, c.nullable),
                  childExpr,
                  FailureBehavior.THROW_EXCEPTION)
              }
            })
      case SubstraitLiteral(substraitLiteral) => Some(substraitLiteral)
      case a: AttributeReference if currentOutput.nonEmpty => translateAttribute(a)
      case a: Alias => translateUp(a.child)
      case p
          if p.getClass.getCanonicalName == // removed in spark-3.3
            "org.apache.spark.sql.catalyst.expressions.PromotePrecision" =>
        translateUp(p.children.head)
      case CaseWhen(branches, elseValue) => translateCaseWhen(branches, elseValue)
      case MergedStruct(replacement) => translateUp(replacement)
      case In(value, list) => translateIn(value, list)
      case InSet(value, set) => translateIn(value, set.toSeq.map(v => Literal(v)))
      case scalar @ ScalarFunction(children) =>
        Util
          .seqToOption(children.map {
            case Enum(value) => Some(EnumArg.of(value))
            case child: Expression => translateUp(child)
          })
          .flatMap(toScalarFunction.convert(scalar, _))
      case p: PlanExpression[_] => translateSubQuery(p)
      case in: InSubquery => translateInSubquery(in)
      case other => default(other)
    }
  }
}
