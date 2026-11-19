package io.substrait.spark

import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.`type`.{NamedStruct, Type, TypeCreator}
import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.{Expression, ExpressionCreator, FunctionArg, WindowBound}
import io.substrait.extension.{DefaultExtensionCatalog, SimpleExtension}
import io.substrait.relation.{Aggregate, ConsistentPartitionWindow, Rel, VirtualTableScan}
import io.substrait.util.EmptyVisitationContext

import java.util.{Arrays, Collections}

/**
 * Substrait's `count()`, with no arguments, counts rows, which Spark has no call for: the runtime
 * converts it to `count(1)`, both as an aggregate measure and over a window.
 */
class CountRowsSuite extends SparkFunSuite with SharedSparkSession {

  private val extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION
  private val sb = new SubstraitBuilder(extensions)

  private def row(value: Expression): Expression.NestedStruct =
    Expression.NestedStruct.builder().addFields(value).build()

  /** Three rows, one of them null, so a count of the column's values would give 2. */
  private val table: VirtualTableScan = VirtualTableScan
    .builder()
    .initialSchema(
      NamedStruct.of(
        Arrays.asList("value"),
        Type.Struct.builder().addFields(TypeCreator.NULLABLE.I32).nullable(false).build()))
    .addRows(row(ExpressionCreator.i32(true, 1)))
    .addRows(row(ExpressionCreator.typedNull(TypeCreator.NULLABLE.I32)))
    .addRows(row(ExpressionCreator.i32(true, 3)))
    .build()

  private def rows(rel: Rel): Seq[Row] = {
    val plan = rel.accept(new ToLogicalPlan(spark), EmptyVisitationContext.INSTANCE)
    DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq
  }

  test("count() as an aggregate measure counts rows") {
    val aggregate = Aggregate
      .builder()
      .input(table)
      .addGroupings(Aggregate.Grouping.builder().build())
      .addMeasures(sb.countStar())
      .build()

    assertResult(Seq(Row(3L)))(rows(aggregate))
  }

  test("count() over a window counts rows") {
    val count = ExpressionCreator.windowRelFunction(
      extensions.getWindowFunction(
        SimpleExtension.FunctionAnchor
          .of(DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:")),
      TypeCreator.REQUIRED.I64,
      Expression.AggregationPhase.INITIAL_TO_RESULT,
      Expression.AggregationInvocation.ALL,
      Expression.WindowBoundsType.ROWS,
      WindowBound.UNBOUNDED,
      WindowBound.UNBOUNDED,
      Collections.emptyList[FunctionArg]()
    )
    val window = ConsistentPartitionWindow.builder().input(table).addWindowFunctions(count).build()

    // The window appends the count, over every row, to each row.
    assertResult(Seq(3L, 3L, 3L))(rows(window).map(_.getLong(1)))
  }
}
