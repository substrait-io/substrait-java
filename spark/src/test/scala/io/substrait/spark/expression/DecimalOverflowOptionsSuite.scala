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

import io.substrait.spark.SparkExtension

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal

import io.substrait.`type`.TypeCreator
import io.substrait.expression.{Expression => SExpression, ExpressionCreator, FunctionOption}
import io.substrait.extension.DefaultExtensionCatalog
import io.substrait.util.EmptyVisitationContext

import java.math.BigDecimal

import scala.jdk.CollectionConverters._

class DecimalOverflowOptionsSuite extends SparkFunSuite {

  private val converter = new ToSparkExpression(
    ToScalarFunction(SparkExtension.SparkScalarFunctions))
  private val max = "9" * 38
  private val cases =
    Seq(("add", max, "1", "8"), ("subtract", s"-$max", "1", "4"), ("multiply", max, "2", "12"))

  private def withAnsi(enabled: Boolean)(body: SQLConf => Unit): Unit = {
    val conf = new SQLConf
    conf.setConfString("spark.sql.ansi.enabled", enabled.toString)
    SQLConf.withExistingConf(conf)(body(conf))
  }

  private def call(
      name: String,
      left: String,
      right: String,
      preferences: Option[Seq[String]] = Some(Seq("ERROR")),
      optionName: String = "overflow"): SExpression.ScalarFunctionInvocation = {
    val declaration = DefaultExtensionCatalog.DEFAULT_COLLECTION
      .scalarFunctions()
      .asScala
      .find(_.key() == s"$name:dec_dec")
      .get
    val builder = SExpression.ScalarFunctionInvocation
      .builder()
      .declaration(declaration)
      .outputType(TypeCreator.REQUIRED.decimal(38, 0))
      .addArguments(
        ExpressionCreator.decimal(false, new BigDecimal(left), 38, 0),
        ExpressionCreator.decimal(false, new BigDecimal(right), 38, 0))
    preferences.foreach(
      values =>
        builder.addOptions(
          FunctionOption.builder().name(optionName).addAllValues(values.asJava).build()))
    builder.build()
  }

  private def convert(call: SExpression.ScalarFunctionInvocation): Expression =
    call.accept(converter, EmptyVisitationContext.INSTANCE)

  private def assertOverflow(expression: Expression): Unit = {
    intercept[ArithmeticException](expression.eval(InternalRow.empty))
    intercept[ArithmeticException](
      UnsafeProjection.create(Seq(expression)).apply(InternalRow.empty))
  }

  private def assertValue(expression: Expression, expected: String): Unit = {
    val value = expression.eval(InternalRow.empty).asInstanceOf[Decimal]
    val row = UnsafeProjection.create(Seq(expression)).apply(InternalRow.empty)
    if (expected == null) {
      assert(value == null)
      assert(row.isNullAt(0))
    } else {
      assertResult(new BigDecimal(expected))(value.toJavaBigDecimal)
      assertResult(new BigDecimal(expected))(row.getDecimal(0, 38, 0).toJavaBigDecimal)
    }
  }

  for {
    (name, left, right, safe) <- cases
    ansi <- Seq(false, true)
  } {
    test(s"$name decimal overflow=ERROR throws with ANSI=$ansi") {
      withAnsi(ansi) {
        conf =>
          assertOverflow(convert(call(name, left, right)))
          assertResult(ansi)(conf.ansiEnabled)
      }
    }

    test(s"$name decimal overflow=ERROR retains safe values with ANSI=$ansi") {
      withAnsi(ansi) {
        conf =>
          assertValue(convert(call(name, "6", "2")), safe)
          assertResult(ansi)(conf.ansiEnabled)
      }
    }

    test(s"$name without an overflow option retains ANSI=$ansi behavior") {
      withAnsi(ansi) {
        _ =>
          val expression = convert(call(name, left, right, None))
          if (ansi) assertOverflow(expression) else assertValue(expression, null)
      }
    }
  }

  cases.foreach {
    case (name, left, right, _) =>
      test(s"$name matches overflow option names and preferences without case sensitivity") {
        withAnsi(false) {
          _ => assertOverflow(convert(call(name, left, right, Some(Seq("eRrOr")), "OvErFlOw")))
        }
      }

      test(s"$name selects ERROR after unsupported preferences") {
        withAnsi(false) {
          _ =>
            assertOverflow(
              convert(call(name, left, right, Some(Seq("SATURATE", "SILENT", "ERROR")))))
        }
      }

      Seq(
        Seq("SILENT"),
        Seq("SATURATE"),
        Seq("SILENT", "SATURATE"),
        Seq("BOGUS", "ERROR"),
        Seq.empty[String]).foreach {
        values =>
          test(s"$name rejects unsupported overflow preferences ${values.mkString(",")}") {
            withAnsi(false) {
              _ =>
                val error = intercept[UnsupportedOperationException] {
                  convert(call(name, left, right, Some(values)))
                }
                assert(error.getMessage.contains("overflow"))
            }
          }
      }

      test(s"$name rejects a conflicting repeated overflow option") {
        withAnsi(false) {
          _ =>
            val invocation = SExpression.ScalarFunctionInvocation
              .builder()
              .from(call(name, left, right))
              .addOptions(FunctionOption.builder().name("OVERFLOW").addValues("SATURATE").build())
              .build()
            intercept[UnsupportedOperationException](convert(invocation))
        }
      }

      test(s"$name accepts repeated overflow options that both support ERROR") {
        withAnsi(false) {
          _ =>
            val invocation = SExpression.ScalarFunctionInvocation
              .builder()
              .from(call(name, left, right))
              .addOptions(
                FunctionOption.builder().name("OVERFLOW").addValues("SATURATE", "error").build())
              .build()
            assertOverflow(convert(invocation))
        }
      }

      test(s"$name rejects an undeclared decimal arithmetic option") {
        withAnsi(false) {
          _ =>
            intercept[UnsupportedOperationException] {
              convert(call(name, left, right, Some(Seq("ERROR")), "unknown"))
            }
        }
      }

      test(s"$name retains explicit ERROR after the session ANSI setting changes") {
        withAnsi(true) {
          conf =>
            val expression = convert(call(name, left, right))
            conf.setConfString("spark.sql.ansi.enabled", "false")
            assertOverflow(expression)
        }
      }
  }
}
