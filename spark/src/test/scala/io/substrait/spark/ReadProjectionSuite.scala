package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.{Expression, MaskExpression}
import io.substrait.plan.Plan
import io.substrait.relation.{AbstractReadRel, LocalFiles => LocalFilesRel, NamedScan, Rel, VirtualTableScan}
import io.substrait.util.EmptyVisitationContext

import java.util.Optional

class ReadProjectionSuite extends SparkFunSuite with SharedSparkSession {

  private val builder = new SubstraitBuilder

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  private def columns(fields: Int*): MaskExpression = {
    val select = MaskExpression.StructSelect.builder()
    fields.foreach(field => select.addStructItems(MaskExpression.StructItem.of(field)))
    MaskExpression.builder().select(select.build()).build()
  }

  private def assertRows(rel: Rel, expected: Row*): Unit = {
    val plan = rel.accept(new ToLogicalPlan(spark), EmptyVisitationContext.INSTANCE)
    assert(plan.resolved)
    assertResult(rel.getRecordType)(ToSubstraitType.toNamedStruct(plan.schema).struct())
    val actual = DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  private def withScan(kind: String)(body: AbstractReadRel => Unit): Unit = {
    val data = spark.sql(
      "SELECT * FROM VALUES (1L, 'one', true), (2L, 'two', false), " +
        "(3L, NULL, NULL) AS t(id, label, keep)")
    kind match {
      case "named table" =>
        withTempView("projection_scan") {
          data.createOrReplaceTempView("projection_scan")
          body(
            NamedScan
              .builder()
              .addNames("projection_scan")
              .initialSchema(ToSubstraitType.toNamedStruct(data.schema))
              .build())
        }
      case "virtual table" =>
        body(
          new ToSubstraitRel()
            .visit(data.queryExecution.optimizedPlan)
            .asInstanceOf[VirtualTableScan])
      case "local files" =>
        withTempPath {
          path =>
            data.write.parquet(path.getAbsolutePath)
            val read = spark.read.parquet(path.getAbsolutePath)
            body(
              new ToSubstraitRel()
                .visit(read.queryExecution.optimizedPlan)
                .asInstanceOf[LocalFilesRel])
        }
      case other => throw new IllegalArgumentException(s"Unknown scan kind: $other")
    }
  }

  private def project(
      scan: AbstractReadRel,
      mask: MaskExpression,
      filter: Optional[Expression] = Optional.empty[Expression](),
      emit: Optional[Rel.Remap] = Optional.empty[Rel.Remap]()): AbstractReadRel = {
    scan match {
      case named: NamedScan =>
        NamedScan.builder().from(named).projection(mask).filter(filter).remap(emit).build()
      case virtual: VirtualTableScan =>
        VirtualTableScan.builder().from(virtual).projection(mask).filter(filter).remap(emit).build()
      case files: LocalFilesRel =>
        LocalFilesRel.builder().from(files).projection(mask).filter(filter).remap(emit).build()
      case other => throw new IllegalArgumentException(s"Unknown scan: $other")
    }
  }

  Seq("named table", "virtual table", "local files").foreach {
    kind =>
      test(s"read projection on $kind selects and reorders whole columns") {
        withScan(kind) {
          scan =>
            assertRows(scan, Row(1L, "one", true), Row(2L, "two", false), Row(3L, null, null))
            assertRows(project(scan, columns(0, 2)), Row(1L, true), Row(2L, false), Row(3L, null))
            assertRows(project(scan, columns(2, 0)), Row(true, 1L), Row(false, 2L), Row(null, 3L))
        }
      }

      test(s"read projection on $kind keeps a single column as a relation") {
        withScan(kind) {
          scan =>
            Seq(false, true).foreach {
              maintain =>
                val mask = MaskExpression
                  .builder()
                  .from(columns(2))
                  .maintainSingularStruct(maintain)
                  .build()
                assertRows(project(scan, mask), Row(true), Row(false), Row(null))
            }
        }
      }

      test(s"empty read projection on $kind preserves row count") {
        withScan(kind)(scan => assertRows(project(scan, columns()), Row(), Row(), Row()))
      }

      test(s"read projection on $kind runs after the filter and before emit") {
        withScan(kind) {
          scan =>
            val projected = project(
              scan,
              columns(1, 0),
              Optional.of(builder.fieldReference(scan, 2)),
              Optional.of(Rel.Remap.offset(1, 1)))
            assertRows(projected, Row(1L))
        }
      }

      test(s"emit on $kind indexes the projected columns") {
        withScan(kind) {
          scan =>
            assertRows(
              project(scan, columns(0, 2), emit = Optional.of(Rel.Remap.offset(1, 1))),
              Row(true),
              Row(false),
              Row(null))
        }
      }

      test(s"a parent relation references the projected columns of $kind") {
        withScan(kind) {
          scan =>
            val masked = project(scan, columns(0, 2))
            val filtered = builder.filter((input: Rel) => builder.fieldReference(input, 1), masked)
            assertRows(filtered, Row(1L, true))
        }
      }
  }

  test("root naming preserves the projected column types and values") {
    withScan("named table") {
      scan =>
        val root = Plan.Root
          .builder()
          .input(project(scan, columns(0, 2)))
          .addNames("selected_id", "selected_keep")
          .build()
        val substrait = Plan
          .builder()
          .executionBehavior(
            Plan.ExecutionBehavior
              .builder()
              .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
              .build())
          .addRoots(root)
          .build()
        val plan = new ToLogicalPlan(spark).convert(substrait)
        assertResult(Seq("selected_id", "selected_keep"))(plan.output.map(_.name))
        assertResult(Seq(Row(1L, true), Row(2L, false), Row(3L, null)))(
          DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq.sortBy(_.getLong(0)))
    }
  }

  test("a read projection selecting inside a column is rejected") {
    withTempView("projection_struct") {
      val data = spark.sql("SELECT named_struct('x', 1, 'y', 'hidden') AS s")
      data.createOrReplaceTempView("projection_struct")
      val mask = MaskExpression
        .builder()
        .select(
          MaskExpression.StructSelect
            .builder()
            .addStructItems(MaskExpression.StructItem.of(0, columns(0).getSelect))
            .build())
        .build()
      val scan = NamedScan
        .builder()
        .addNames("projection_struct")
        .initialSchema(ToSubstraitType.toNamedStruct(data.schema))
        .projection(mask)
        .build()
      val error = intercept[UnsupportedOperationException] {
        scan.accept(new ToLogicalPlan(spark), EmptyVisitationContext.INSTANCE)
      }
      assert(error.getMessage.contains("selects inside a column"))
    }
  }
}
