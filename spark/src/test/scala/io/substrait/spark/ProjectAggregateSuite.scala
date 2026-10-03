package io.substrait.spark

import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate => SparkAggregate, LogicalPlan}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.`type`.{NamedStruct, TypeCreator}
import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.ExpressionCreator
import io.substrait.extension.DefaultExtensionCatalog
import io.substrait.hint.Hint
import io.substrait.relation.{Aggregate, Project, VirtualTableScan}
import io.substrait.relation.Set.SetOp

import java.util.Arrays

class ProjectAggregateSuite
  extends SparkFunSuite
  with SharedSparkSession
  with SubstraitPlanTestBase {

  private val builder = new SubstraitBuilder(DefaultExtensionCatalog.DEFAULT_COLLECTION)

  private def inputRows(): VirtualTableScan = {
    val required = TypeCreator.REQUIRED
    VirtualTableScan
      .builder()
      .initialSchema(NamedStruct
        .of(Arrays.asList("group", "value"), required.struct(required.I32, required.I64)))
      .addRows(ExpressionCreator
        .nestedStruct(false, ExpressionCreator.i32(false, 1), ExpressionCreator.i64(false, 10)))
      .addRows(ExpressionCreator
        .nestedStruct(false, ExpressionCreator.i32(false, 1), ExpressionCreator.i64(false, 20)))
      .addRows(ExpressionCreator
        .nestedStruct(false, ExpressionCreator.i32(false, 2), ExpressionCreator.i64(false, 7)))
      .build()
  }

  private def aggregateRows(): Aggregate =
    builder.aggregate(
      input => builder.grouping(input, 0),
      input => Arrays.asList(builder.sum(input, 1)),
      inputRows())

  private def projectOverAggregate(): Project = {
    builder.project(
      input =>
        Arrays.asList(
          builder.i32(99),
          builder.fieldReference(input, 0),
          builder.add(builder.fieldReference(input, 1), builder.i64(1))),
      aggregateRows())
  }

  private def assertProjectRows(project: Project, expected: Seq[Row]): LogicalPlan = {
    val converted = new ToLogicalPlan(spark).convert(project)
    assert(converted.isInstanceOf[SparkAggregate])
    assertResult(project.getRecordType.fields().size())(converted.output.size)
    assertRows(converted, expected)
    converted
  }

  private def assertRows(converted: LogicalPlan, expected: Seq[Row]): Unit = {
    val actual = DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  test("project retains inherited grouping and measure outputs") {
    assertProjectRows(projectOverAggregate(), Seq(Row(1, 30L, 99, 1, 31L), Row(2, 7L, 99, 2, 8L)))
  }

  test("project emit can select only inherited aggregate outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(builder.remap(0, 1))
      .build()
    val converted = assertProjectRows(project, Seq(Row(1, 30L), Row(2, 7L)))
    assertResult(Seq("group", "sum(value)"))(converted.output.map(_.name))
  }

  test("project emit can reorder and duplicate inherited and appended outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(builder.remap(4, 1, 0, 1, 2))
      .build()
    assertProjectRows(project, Seq(Row(31L, 30L, 1, 30L, 99), Row(8L, 7L, 2, 7L, 99)))
  }

  test("project emit can remove all aggregate outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(builder.remap())
      .build()
    assertProjectRows(project, Seq(Row(), Row()))
  }

  Seq(false, true).foreach {
    folded =>
      test(s"project hint names follow emit ordering with aggregate folding=$folded") {
        val input = if (folded) aggregateRows() else inputRows()
        val project = Project
          .builder()
          .from(
            builder.project(
              _ => Arrays.asList(builder.i32(99), builder.i32(100)),
              builder.remap(3, 0),
              input))
          .hint(Hint.builder().addOutputNames("hundred", "grp").build())
          .build()
        val converted = new ToLogicalPlan(spark).convert(project)
        assertResult(Seq("hundred", "grp"))(converted.output.map(_.name))
        val expected =
          if (folded) Seq(Row(100, 1), Row(100, 2))
          else Seq(Row(100, 1), Row(100, 1), Row(100, 2))
        assertRows(converted, expected)
      }
  }

  Seq("aggregate project", "project", "emit").foreach {
    source =>
      test(s"union keeps repeated $source outputs distinct") {
        val first = source match {
          case "aggregate project" =>
            builder.project(
              input => Arrays.asList(builder.fieldReference(input, 0)),
              aggregateRows())
          case "project" =>
            builder.project(input => Arrays.asList(builder.fieldReference(input, 0)), inputRows())
          case "emit" =>
            VirtualTableScan.builder().from(inputRows()).remap(builder.remap(0, 1, 0)).build()
        }
        val required = TypeCreator.REQUIRED
        val second = VirtualTableScan
          .builder()
          .initialSchema(
            NamedStruct.of(
              Arrays.asList("group", "value", "other"),
              required.struct(required.I32, required.I64, required.I32)))
          .addRows(ExpressionCreator
            .nestedStruct(false, builder.i32(100), builder.i64(20), builder.i32(200)))
          .build()
        val union = builder.set(SetOp.UNION_ALL, first, second)
        val selected = builder.project(
          input => Arrays.asList(builder.fieldReference(input, 2)),
          builder.remap(3),
          union)
        val expected =
          if (source == "aggregate project") Seq(Row(1), Row(2), Row(200))
          else Seq(Row(1), Row(1), Row(2), Row(200))
        assertRows(new ToLogicalPlan(spark).convert(selected), expected)
      }
  }

  test("Spark projected aggregate preserves roundtrip shape and rows") {
    val query =
      "select group_id + 1 as group_key, sum(value) + 1 as total " +
        "from (values (1, 10), (1, 20), (2, 7)) as input(group_id, value) " +
        "group by group_id + 1"
    val converted = assertSqlSubstraitRelRoundTrip(query)
    assertResult(Seq("group_key", "total"))(converted.output.map(_.name))
    assertRows(converted, Seq(Row(2, 31L), Row(3, 8L)))
  }
}
