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

import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.extension.DefaultExtensionCatalog
import io.substrait.`type`.TypeCreator
import io.substrait.util.EmptyVisitationContext
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, Literal, Or}
import org.scalatest.Assertions.assertResult

import scala.jdk.CollectionConverters._

class PredicateSuite extends SparkFunSuite with SubstraitExpressionTestBase {

  private val extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION
  private val sb = new SubstraitBuilder(extensions)
  private val toSparkExpression =
    new ToSparkExpression(ToScalarFunction(io.substrait.spark.SparkExtension.SparkScalarFunctions))

  test("And") {
    runTest("and:bool", And(Literal(true), Literal(false)))
  }

  test("Multivariate AND with 3 arguments") {
    // Create a substrait AND expression with 3 boolean arguments: a AND b AND c
    val boolA = ExpressionCreator.bool(false, true)
    val boolB = ExpressionCreator.bool(false, false)
    val boolC = ExpressionCreator.bool(false, true)

    val substraitAnd = sb.and(boolA, boolB, boolC)

    // Verify it's an AND function
    val scalarFunc = substraitAnd.asInstanceOf[SExpression.ScalarFunctionInvocation]
    assertResult("and")(scalarFunc.declaration().name())
    assertResult(3)(scalarFunc.arguments().size())

    // Convert to Spark expression
    val sparkExpr = substraitAnd.accept(toSparkExpression, EmptyVisitationContext.INSTANCE)

    // Verify the result is nested And expressions: And(And(a, b), c)
    // reduceLeft creates: ((a AND b) AND c)
    sparkExpr match {
      case And(And(Literal(a, _), Literal(b, _)), Literal(c, _)) =>
        assertResult(true)(a)
        assertResult(false)(b)
        assertResult(true)(c)
      case _ =>
        fail(s"Expected nested And expressions, got: $sparkExpr")
    }
  }

  test("Multivariate AND with 4 arguments") {
    // Create a substrait AND expression with 4 boolean arguments: a AND b AND c AND d
    val boolA = ExpressionCreator.bool(false, true)
    val boolB = ExpressionCreator.bool(false, true)
    val boolC = ExpressionCreator.bool(false, false)
    val boolD = ExpressionCreator.bool(false, true)

    val substraitAnd = sb.and(boolA, boolB, boolC, boolD)

    // Verify it's an AND function with 4 arguments
    val scalarFunc = substraitAnd.asInstanceOf[SExpression.ScalarFunctionInvocation]
    assertResult("and")(scalarFunc.declaration().name())
    assertResult(4)(scalarFunc.arguments().size())

    // Convert to Spark expression
    val sparkExpr = substraitAnd.accept(toSparkExpression, EmptyVisitationContext.INSTANCE)

    // Verify the result is nested And expressions: And(And(And(a, b), c), d)
    // reduceLeft creates: (((a AND b) AND c) AND d)
    sparkExpr match {
      case And(And(And(Literal(a, _), Literal(b, _)), Literal(c, _)), Literal(d, _)) =>
        assertResult(true)(a)
        assertResult(true)(b)
        assertResult(false)(c)
        assertResult(true)(d)
      case _ =>
        fail(s"Expected nested And expressions, got: $sparkExpr")
    }
  }

  test("Multivariate OR with 3 arguments") {
    // Create a substrait OR expression with 3 boolean arguments: a OR b OR c
    val boolA = ExpressionCreator.bool(false, false)
    val boolB = ExpressionCreator.bool(false, true)
    val boolC = ExpressionCreator.bool(false, false)

    val substraitOr = sb.or(boolA, boolB, boolC)

    // Verify it's an OR function
    val scalarFunc = substraitOr.asInstanceOf[SExpression.ScalarFunctionInvocation]
    assertResult("or")(scalarFunc.declaration().name())
    assertResult(3)(scalarFunc.arguments().size())

    // Convert to Spark expression
    val sparkExpr = substraitOr.accept(toSparkExpression, EmptyVisitationContext.INSTANCE)

    // Verify the result is nested Or expressions: Or(Or(a, b), c)
    // reduceLeft creates: ((a OR b) OR c)
    sparkExpr match {
      case Or(Or(Literal(a, _), Literal(b, _)), Literal(c, _)) =>
        assertResult(false)(a)
        assertResult(true)(b)
        assertResult(false)(c)
      case _ =>
        fail(s"Expected nested Or expressions, got: $sparkExpr")
    }
  }

  test("Multivariate OR with 5 arguments") {
    // Create a substrait OR expression with 5 boolean arguments: a OR b OR c OR d OR e
    val boolA = ExpressionCreator.bool(false, false)
    val boolB = ExpressionCreator.bool(false, false)
    val boolC = ExpressionCreator.bool(false, true)
    val boolD = ExpressionCreator.bool(false, false)
    val boolE = ExpressionCreator.bool(false, false)

    val substraitOr = sb.or(boolA, boolB, boolC, boolD, boolE)

    // Verify it's an OR function with 5 arguments
    val scalarFunc = substraitOr.asInstanceOf[SExpression.ScalarFunctionInvocation]
    assertResult("or")(scalarFunc.declaration().name())
    assertResult(5)(scalarFunc.arguments().size())

    // Convert to Spark expression
    val sparkExpr = substraitOr.accept(toSparkExpression, EmptyVisitationContext.INSTANCE)

    // Verify the result is nested Or expressions: Or(Or(Or(Or(a, b), c), d), e)
    // reduceLeft creates: ((((a OR b) OR c) OR d) OR e)
    sparkExpr match {
      case Or(Or(Or(Or(Literal(a, _), Literal(b, _)), Literal(c, _)), Literal(d, _)), Literal(e, _)) =>
        assertResult(false)(a)
        assertResult(false)(b)
        assertResult(true)(c)
        assertResult(false)(d)
        assertResult(false)(e)
      case _ =>
        fail(s"Expected nested Or expressions, got: $sparkExpr")
    }
  }

  test("Mixed multivariate AND and OR") {
    // Create a complex expression: (a AND b AND c) OR (d AND e)
    val boolA = ExpressionCreator.bool(false, true)
    val boolB = ExpressionCreator.bool(false, true)
    val boolC = ExpressionCreator.bool(false, false)
    val boolD = ExpressionCreator.bool(false, true)
    val boolE = ExpressionCreator.bool(false, true)

    val substraitAndLeft = sb.and(boolA, boolB, boolC)
    val substraitAndRight = sb.and(boolD, boolE)
    val substraitOr = sb.or(substraitAndLeft, substraitAndRight)

    // Convert to Spark expression
    val sparkExpr = substraitOr.accept(toSparkExpression, EmptyVisitationContext.INSTANCE)

    // Verify the structure: Or(And(And(a, b), c), And(d, e))
    sparkExpr match {
      case Or(And(And(Literal(a, _), Literal(b, _)), Literal(c, _)), And(Literal(d, _), Literal(e, _))) =>
        assertResult(true)(a)
        assertResult(true)(b)
        assertResult(false)(c)
        assertResult(true)(d)
        assertResult(true)(e)
      case _ =>
        fail(s"Expected mixed nested And/Or expressions, got: $sparkExpr")
    }
  }
}
