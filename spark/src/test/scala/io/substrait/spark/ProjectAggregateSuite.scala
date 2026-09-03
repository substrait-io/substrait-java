package io.substrait.spark

import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate => SparkAggregate}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.`type`.{NamedStruct, TypeCreator}
import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.ExpressionCreator
import io.substrait.extension.DefaultExtensionCatalog
import io.substrait.relation.{Aggregate, Project, Rel, VirtualTableScan}

import java.util.Arrays

class ProjectAggregateSuite
  extends SparkFunSuite
  with SharedSparkSession
  with SubstraitPlanTestBase {

  private val builder = new SubstraitBuilder(DefaultExtensionCatalog.DEFAULT_COLLECTION)

  private def projectOverAggregate(): Project = {
    val required = TypeCreator.REQUIRED
    val input = VirtualTableScan
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
    val aggregate = Aggregate
      .builder()
      .input(input)
      .addGroupings(
        Aggregate.Grouping
          .builder()
          .addExpressions(builder.fieldReference(input, 0))
          .build())
      .addMeasures(builder.sum(input, 1))
      .build()

    Project
      .builder()
      .input(aggregate)
      .addExpressions(ExpressionCreator.i32(false, 99))
      .addExpressions(builder.fieldReference(aggregate, 0))
      .addExpressions(
        builder.add(builder.fieldReference(aggregate, 1), ExpressionCreator.i64(false, 1)))
      .build()
  }

  private def assertRows(project: Project, expected: Seq[Row]): Unit = {
    val converted = new ToLogicalPlan(spark).convert(project)
    assert(converted.isInstanceOf[SparkAggregate])
    assertResult(project.getRecordType.fields().size())(converted.output.size)
    val actual = DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  test("project retains inherited grouping and measure outputs") {
    assertRows(projectOverAggregate(), Seq(Row(1, 30L, 99, 1, 31L), Row(2, 7L, 99, 2, 8L)))
  }

  test("project emit can select only inherited aggregate outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(Rel.Remap.of(Arrays.asList(0, 1)))
      .build()
    assertRows(project, Seq(Row(1, 30L), Row(2, 7L)))
  }

  test("project emit can reorder and duplicate inherited and appended outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(Rel.Remap.of(Arrays.asList(4, 1, 0, 1, 2)))
      .build()
    assertRows(project, Seq(Row(31L, 30L, 1, 30L, 99), Row(8L, 7L, 2, 7L, 99)))
  }

  test("project emit can remove all aggregate outputs") {
    val project = Project
      .builder()
      .from(projectOverAggregate())
      .remap(Rel.Remap.of(Arrays.asList()))
      .build()
    assertRows(project, Seq(Row(), Row()))
  }

  test("Spark projected aggregate preserves roundtrip shape and rows") {
    val query =
      "select group_id + 1 as group_key, sum(value) + 1 as total " +
        "from (values (1, 10), (1, 20), (2, 7)) as input(group_id, value) " +
        "group by group_id + 1"
    val converted = assertSqlSubstraitRelRoundTrip(query)
    val actual = DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
    assertResult(Seq(Row(2, 31L), Row(3, 8L)))(actual.sortBy(_.getInt(0)))
  }
}
