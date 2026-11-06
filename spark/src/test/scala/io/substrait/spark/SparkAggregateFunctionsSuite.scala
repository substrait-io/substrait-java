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

import io.substrait.spark.compat.SparkCompat
import io.substrait.spark.expression.SparkAggregateFunctions
import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.{SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{EvalMode, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Final, Partial, PartialMerge, Sum}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType}

import io.substrait.`type`.TypeCreator
import io.substrait.expression.{Expression => SExpression, ExpressionCreator, FunctionOption}
import io.substrait.extension.SimpleExtension
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.{Aggregate, ConsistentPartitionWindow, Rel, SingleInputRel}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class SparkAggregateFunctionsSuite extends SparkFunSuite with SharedSparkSession {
  private val ansiKey = "spark.sql.ansi.enabled"
  private val urn = "extension:substrait:spark"
  private val input = Literal.create(Decimal(BigDecimal("1.00")), DecimalType(10, 2))
  private val sum = Sum(input, EvalMode.LEGACY)
  private val declaration = SparkExtension.COLLECTION.getAggregateFunction(
    SimpleExtension.FunctionAnchor.of(urn, "sum:dec"))
  private val sumType = TypeCreator.NULLABLE.decimal(20, 2)

  private def option(values: String*): FunctionOption =
    FunctionOption.builder().name("overflow").addAllValues(values.asJava).build()

  // Spark 4 deprecates this accessor; Spark 3 does not yet provide getCondition.
  @nowarn("cat=deprecation")
  private def errorClass(error: SparkThrowable): String = error.getErrorClass

  private def outcome(body: => Seq[Row]): Either[String, Seq[Row]] = {
    try Right(body)
    catch {
      case error: Exception =>
        var cause: Throwable = error
        while (cause.getCause != null) cause = cause.getCause
        cause match {
          case throwable: SparkThrowable => Left(errorClass(throwable))
          case other => throw other
        }
    }
  }

  private def verifyTypes(rel: Rel): Unit = {
    def verify(
        function: SimpleExtension.Function,
        arguments: java.util.List[io.substrait.expression.FunctionArg],
        output: io.substrait.`type`.Type): Unit = {
      if (function.urn() == urn) {
        val actual = arguments.asScala.map(_.asInstanceOf[SExpression].getType).toList.asJava
        assertResult(output)(function.resolveType(actual))
      }
    }
    rel match {
      case aggregate: Aggregate =>
        aggregate.getMeasures.asScala.foreach {
          measure =>
            val function = measure.getFunction
            verify(function.declaration(), function.arguments(), function.outputType())
        }
      case window: ConsistentPartitionWindow =>
        window.getWindowFunctions.asScala.foreach {
          function => verify(function.declaration(), function.arguments(), function.outputType())
        }
      case _ =>
    }
    rel match {
      case single: SingleInputRel => verifyTypes(single.getInput)
      case _ =>
    }
  }

  private def roundTrip(sql: String, ansi: Boolean): Either[String, Seq[Row]] = {
    var result: Option[Either[String, Seq[Row]]] = None
    withSQLConf(ansiKey -> ansi.toString) {
      val native = spark.sql(sql)
      val exported = new ToSubstraitRel().convert(native.queryExecution.optimizedPlan)
      val proto = new PlanProtoConverter().toProto(exported)
      val parsed = new ProtoPlanConverter(SparkExtension.COLLECTION)
        .from(io.substrait.proto.Plan.parseFrom(proto.toByteArray))
      parsed.getRoots.asScala.foreach(root => verifyTypes(root.getInput))
      val unsupported = intercept[IllegalArgumentException] {
        new ProtoPlanConverter().from(proto)
      }
      assert(unsupported.getMessage.contains(urn))
      val expected = outcome(native.collect().toSeq)
      withSQLConf(ansiKey -> (!ansi).toString) {
        val restored = new ToLogicalPlan(spark).convert(parsed)
        assertResult(native.schema)(restored.schema)
        val actual = outcome {
          val qe = SparkCompat.instance.createQueryExecution(spark, restored)
          val convert = CatalystTypeConverters.createToScalaConverter(restored.schema)
          qe.executedPlan.executeCollect().map(row => convert(row).asInstanceOf[Row]).toSeq
        }
        assertResult(expected)(actual)
      }
      result = Some(expected)
    }
    result.get
  }

  private def values(p: Int, s: Int, rows: String*): String =
    " FROM VALUES " + rows
      .map {
        value =>
          val literal = if (value == null) "NULL" else s"'$value'"
          s"(CAST($literal AS DECIMAL($p,$s)))"
      }
      .mkString(",") + " AS t(d)"

  test("decimal aggregates preserve values and types across ANSI modes") {
    for (ansi <- Seq(false, true)) {
      val ordinary = values(10, 2, "1.00", "2.01", null)
      assert(roundTrip("SELECT sum(d) AS s, avg(d) AS a" + ordinary, ansi).isRight)
      roundTrip("SELECT sum(DISTINCT d) AS s, avg(DISTINCT d) AS a" + ordinary, ansi)
      roundTrip(
        "SELECT sum(d) FILTER (WHERE d > 1) AS s, avg(d) FILTER (WHERE d > 1) AS a" + ordinary,
        ansi)
      roundTrip("SELECT sum(d) AS s, avg(d) AS a" + values(10, 2, null), ansi)
      roundTrip("SELECT sum(d) AS s, avg(d) AS a" + ordinary + " WHERE false", ansi)
      val large = "9" + "0" * 31 + ".00"
      roundTrip("SELECT sum(d) AS s, avg(d) AS a" + values(34, 2, large, large), ansi)
      val cancel = "1" + "0" * 34 + ".00"
      roundTrip("SELECT avg(d) AS a" + values(38, 2, cancel, "-" + cancel), ansi)
    }
  }

  test("decimal AVG retains HALF_UP rounding at the precision limit") {
    val tiny = "0." + "0" * 37 + "1"
    for (ansi <- Seq(false, true); value <- Seq(tiny, "-" + tiny)) {
      val result = roundTrip("SELECT avg(d) AS a" + values(38, 38, value, "0"), ansi)
      assertResult(Right(Seq(Row(new java.math.BigDecimal(value)))))(result)
      val below = roundTrip("SELECT avg(d) AS a" + values(38, 38, value, "0", "0"), ansi)
      assertResult(Right(Seq(Row(new java.math.BigDecimal("0").setScale(38)))))(below)
    }
  }

  test("decimal overflow preserves NULL versus ERROR, including try aggregates") {
    val overflow = values(38, 0, "9" * 38, "9" * 38)
    for (ansi <- Seq(false, true); function <- Seq("sum", "avg")) {
      val result = roundTrip(s"SELECT $function(d) AS a" + overflow, ansi)
      if (ansi) assert(result.isLeft)
      else assertResult(Right(Seq(Row(null))))(result)
      assertResult(Right(Seq(Row(null))))(
        roundTrip(s"SELECT try_$function(d) AS a" + overflow, ansi))
    }
  }

  test("window SUM and AVG carry their types and overflow modes") {
    val ordinary = values(10, 2, "1.00", "2.01", "3.00")
    for (
      ansi <- Seq(false, true);
      frame <- Seq(
        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
    ) {
      roundTrip(
        s"SELECT d, sum(d) OVER (ORDER BY d $frame) AS s, avg(d) OVER (ORDER BY d $frame) AS a" + ordinary + " ORDER BY d",
        ansi)
    }
    for (ansi <- Seq(false, true)) {
      val overflow = values(38, 0, "9" * 38, "9" * 38)
      val result = roundTrip("SELECT sum(d) OVER () AS s, avg(d) OVER () AS a" + overflow, ansi)
      if (ansi) assert(result.isLeft)
      else assertResult(Right(Seq(Row(null, null), Row(null, null))))(result)
    }
  }

  test("i64 optimization paths retain wrapping, errors, try_sum, and floating AVG") {
    val input =
      " FROM VALUES (CAST('9223372036854775807' AS BIGINT)), (CAST('1' AS BIGINT)) AS t(d)"
    for (ansi <- Seq(false, true)) {
      val sum = roundTrip("SELECT sum(d) AS s" + input, ansi)
      if (ansi) assert(sum.isLeft)
      else assertResult(Right(Seq(Row(Long.MinValue))))(sum)
      roundTrip("SELECT avg(d) AS a" + input, ansi)
      assertResult(Right(Seq(Row(null))))(roundTrip("SELECT try_sum(d) AS s" + input, ansi))
      val window = roundTrip("SELECT try_sum(d) OVER () AS s" + input, ansi)
      assertResult(Right(Seq(Row(null), Row(null))))(window)
    }
  }

  test("undoing decimal aggregate optimizations preserves try overflow options") {
    def functions(rel: Rel): Seq[io.substrait.expression.AggregateFunctionInvocation] = {
      val here = rel match {
        case aggregate: Aggregate => aggregate.getMeasures.asScala.map(_.getFunction).toSeq
        case _ => Seq.empty
      }
      rel match {
        case single: SingleInputRel => here ++ functions(single.getInput)
        case _ => here
      }
    }
    withSQLConf(ansiKey -> "true") {
      val sql = "SELECT try_sum(d) AS s, try_avg(d) AS a" + values(5, 2, "1.00", "2.00")
      // Some Spark versions lose TRY in DecimalAggregates. Supply a plan that still carries it.
      val plan = spark.sql(sql).queryExecution.optimizedPlan.transformAllExpressions {
        case expression: AggregateExpression =>
          expression.aggregateFunction match {
            case aggregate: Sum =>
              expression.copy(aggregateFunction = aggregate.copy(evalMode = EvalMode.TRY))
            case aggregate: Average =>
              expression.copy(aggregateFunction = aggregate.copy(evalMode = EvalMode.TRY))
            case _ => expression
          }
      }
      val exported = new ToSubstraitRel().convert(plan)
      val aggregates = exported.getRoots.asScala.toSeq.flatMap(root => functions(root.getInput))
      assert(aggregates.nonEmpty)
      aggregates.foreach {
        function =>
          assertResult(urn)(function.declaration().urn())
          assertResult(Seq(option("NULL")))(function.options().asScala.toSeq)
      }
    }
  }

  test("unscaled_value preserves fractional digits, NULL, and i64 overflow errors") {
    val scalar = io.substrait.spark.expression.ToScalarFunction(SparkExtension.SparkScalarFunctions)
    val converter = new io.substrait.spark.expression.ToSparkExpression(scalar, None)
    for (value <- Seq("1.23", "-1.23", "123456789012345678901234567890123456.78", null)) {
      val literal =
        Literal.create(if (value == null) null else Decimal(BigDecimal(value)), DecimalType(38, 2))
      val native = org.apache.spark.sql.catalyst.expressions.UnscaledValue(literal)
      val argument =
        if (value == null) ExpressionCreator.typedNull(TypeCreator.NULLABLE.decimal(38, 2))
        else ExpressionCreator.decimal(false, new java.math.BigDecimal(value), 38, 2)
      val exported = scalar.convert(native, Seq(argument)).get
      val restored = exported.accept(converter, io.substrait.util.EmptyVisitationContext.INSTANCE)
      if (value != null && new java.math.BigDecimal(value).unscaledValue().bitLength() > 63) {
        intercept[ArithmeticException](native.eval())
        intercept[ArithmeticException](restored.eval())
      } else assertResult(native.eval())(restored.eval())
    }
  }

  test("export refuses a Spark aggregate declaration with a different result type") {
    val incompatible = SimpleExtension.load(s"""urn: $urn
                                               |aggregate_functions:
                                               |  - name: sum
                                               |    impls:
                                               |      - args:
                                               |          - value: DECIMAL<P,S>
                                               |        nullability: DECLARED_OUTPUT
                                               |        decomposable: NONE
                                               |        return: DECIMAL?<38,S>
                                               |""".stripMargin)
    val converter = io.substrait.spark.expression
      .ToAggregateFunction(incompatible.aggregateFunctions().asScala.toSeq)
    intercept[IllegalArgumentException] {
      converter(
        AggregateExpression(
          sum,
          org.apache.spark.sql.catalyst.expressions.aggregate.Complete,
          isDistinct = false,
          filter = None),
        Seq(ExpressionCreator.decimal(false, new java.math.BigDecimal("1.00"), 10, 2))
      )
    }
  }

  test("Spark extension aggregates reject staged export and import") {
    for (mode <- Seq(Partial, PartialMerge, Final)) {
      val expression = AggregateExpression(sum, mode, isDistinct = false, filter = None)
      val error = intercept[UnsupportedOperationException] {
        SparkExtension.toAggregateFunction(
          expression,
          Seq(ExpressionCreator.decimal(false, new java.math.BigDecimal("1.00"), 10, 2)))
      }
      assert(error.getMessage.contains("complete aggregation"))
    }
    for (
      phase <- SExpression.AggregationPhase
        .values()
        .filterNot(_ == SExpression.AggregationPhase.INITIAL_TO_RESULT)
    ) {
      val error = intercept[UnsupportedOperationException] {
        SparkAggregateFunctions.configure(sum, declaration, java.util.List.of(), phase, sumType)
      }
      assert(error.getMessage.contains("INITIAL_TO_RESULT"))
    }
  }

  test("overflow preference order, defaults, and unsupported declarations") {
    def configured(options: FunctionOption*): Sum = SparkAggregateFunctions
      .configure(
        sum,
        declaration,
        options.toList.asJava,
        SExpression.AggregationPhase.INITIAL_TO_RESULT,
        sumType)
      .asInstanceOf[Sum]
    assertResult(EvalMode.TRY)(configured().evalMode)
    assertResult(EvalMode.ANSI)(configured(option("FUTURE", "ERROR", "NULL")).evalMode)
    assertResult(EvalMode.TRY)(configured(option("NULL", "ERROR")).evalMode)
    intercept[UnsupportedOperationException](configured(option("WRAP")))
    intercept[UnsupportedOperationException](configured(option()))
    intercept[IllegalArgumentException](configured(option("NULL"), option("ERROR")))
    intercept[UnsupportedOperationException] {
      configured(FunctionOption.builder().name("unknown").addValues("x").build())
    }
    val mismatch = intercept[IllegalArgumentException] {
      SparkAggregateFunctions.configure(
        sum,
        declaration,
        java.util.List.of(),
        SExpression.AggregationPhase.INITIAL_TO_RESULT,
        TypeCreator.NULLABLE.decimal(38, 2))
    }
    assert(mismatch.getMessage.contains("derives"))
  }
}
