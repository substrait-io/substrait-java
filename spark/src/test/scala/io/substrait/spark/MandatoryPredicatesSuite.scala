package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

import io.substrait.`type`.TypeCreator
import io.substrait.dsl.SubstraitBuilder
import io.substrait.expression.{Expression, ExpressionCreator}
import io.substrait.relation.{AbstractReadRel, Join, LocalFiles => LocalFilesRel, NamedScan, Rel, VirtualTableScan}
import io.substrait.util.EmptyVisitationContext

class MandatoryPredicatesSuite extends SparkFunSuite with SharedSparkSession {

  private val builder = new SubstraitBuilder

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  private def assertRows(rel: Rel, expected: Row*): Unit = {
    val plan = rel.accept(new ToLogicalPlan(spark), EmptyVisitationContext.INSTANCE)
    assert(plan.resolved)
    val actual = DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  private def withScan(kind: String)(body: AbstractReadRel => Unit): Unit = {
    val data = spark.sql("SELECT * FROM VALUES (1, true), (2, false), (3, NULL) AS t(id, keep)")
    kind match {
      case "named table" =>
        withTempView("predicate_scan") {
          data.createOrReplaceTempView("predicate_scan")
          body(
            NamedScan
              .builder()
              .addNames("predicate_scan")
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

  private def filteredScan(scan: AbstractReadRel, predicate: Expression): Rel = {
    val remap = Rel.Remap.offset(0, 1)
    scan match {
      case named: NamedScan =>
        NamedScan.builder().from(named).filter(predicate).remap(remap).build()
      case virtual: VirtualTableScan =>
        VirtualTableScan.builder().from(virtual).filter(predicate).remap(remap).build()
      case files: LocalFilesRel =>
        LocalFilesRel.builder().from(files).filter(predicate).remap(remap).build()
      case other => throw new IllegalArgumentException(s"Unknown scan: $other")
    }
  }

  Seq("named table", "virtual table", "local files").foreach {
    kind =>
      test(s"mandatory read predicates on $kind run before emit") {
        withScan(kind) {
          scan =>
            assertRows(scan, Row(1, true), Row(2, false), Row(3, null))
            assertRows(filteredScan(scan, builder.bool(false)))
            assertRows(
              filteredScan(scan, ExpressionCreator.typedNull(TypeCreator.NULLABLE.BOOLEAN)))
            // The predicate column is omitted from the output, and NULL must be rejected.
            assertRows(filteredScan(scan, builder.fieldReference(scan, 1)), Row(1))
        }
      }
  }

  test("mandatory predicate on a zero-column virtual table") {
    val scan = VirtualTableScan
      .builder()
      .initialSchema(ToSubstraitType.toNamedStruct(new StructType()))
      .addRows(ExpressionCreator.nestedStruct(false))
      .build()
    assertRows(scan, Row())
    assertRows(VirtualTableScan.builder().from(scan).filter(builder.bool(false)).build())
  }

  private def join(joinType: Join.JoinType): Join = {
    val left = new ToSubstraitRel().visit(
      spark.sql("SELECT * FROM VALUES (1), (2) AS l(id)").queryExecution.optimizedPlan)
    val right = new ToSubstraitRel().visit(
      spark.sql("SELECT * FROM VALUES (1), (3) AS r(id)").queryExecution.optimizedPlan)
    builder.join(
      (input: SubstraitBuilder.JoinInput) =>
        builder.equal(builder.fieldReference(input, 0), builder.fieldReference(input, 1)),
      joinType,
      left,
      right)
  }

  Seq(Join.JoinType.INNER, Join.JoinType.LEFT, Join.JoinType.RIGHT, Join.JoinType.OUTER).foreach {
    joinType =>
      test(s"mandatory false post-join predicate on $joinType") {
        val input = join(joinType)
        assertRows(Join.builder().from(input).postJoinFilter(builder.bool(false)).build())
      }
  }

  test("left outer post-join predicate sees null-extended output before emit") {
    val input = join(Join.JoinType.LEFT)
    assertRows(input, Row(1, 1), Row(2, null))
    val filtered = Join
      .builder()
      .from(input)
      .postJoinFilter(builder.isNull(builder.fieldReference(input, 1)))
      .remap(Rel.Remap.offset(0, 1))
      .build()
    assertRows(filtered, Row(2))
  }

  test("right outer post-join predicate sees null-extended output before emit") {
    val input = join(Join.JoinType.RIGHT)
    assertRows(input, Row(1, 1), Row(null, 3))
    val filtered = Join
      .builder()
      .from(input)
      .postJoinFilter(builder.isNull(builder.fieldReference(input, 0)))
      .remap(Rel.Remap.offset(1, 1))
      .build()
    assertRows(filtered, Row(3))
  }

  test("full outer post-join predicate filters both sides of the join") {
    val input = join(Join.JoinType.OUTER)
    val filtered = Join
      .builder()
      .from(input)
      .postJoinFilter(
        builder.or(
          builder.isNull(builder.fieldReference(input, 0)),
          builder.isNull(builder.fieldReference(input, 1))))
      .build()
    assertRows(filtered, Row(2, null), Row(null, 3))
  }

  test("post-join predicate rejects null-extended rows") {
    val input = join(Join.JoinType.LEFT)
    val filtered = Join
      .builder()
      .from(input)
      .postJoinFilter(builder.equal(builder.fieldReference(input, 1), builder.i32(1)))
      .remap(Rel.Remap.offset(0, 1))
      .build()
    assertRows(filtered, Row(1))
  }

  Seq(Join.JoinType.LEFT_SEMI, Join.JoinType.LEFT_ANTI).foreach {
    joinType =>
      test(s"post-join predicate uses $joinType output") {
        val input = join(joinType)
        val filtered = Join
          .builder()
          .from(input)
          .postJoinFilter(builder.equal(builder.fieldReference(input, 0), builder.i32(2)))
          .build()
        if (joinType == Join.JoinType.LEFT_ANTI) {
          assertRows(filtered, Row(2))
        } else {
          assertRows(filtered)
        }
      }
  }
}
