package io.substrait.spark

import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.{Join => SparkJoin}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.dsl.SubstraitBuilder
import io.substrait.plan.{Plan, PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.{Join, NamedScan, Rel}

import scala.jdk.CollectionConverters._

class NamedScanSelfJoinSuite extends SparkFunSuite with SharedSparkSession {

  private val builder = new SubstraitBuilder
  private val behavior = Plan.ExecutionBehavior
    .builder()
    .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
    .build()

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  private def withScan(kind: String, empty: Boolean)(body: (NamedScan, Seq[Row]) => Unit): Unit = {
    val values = spark.sql("SELECT * FROM VALUES (1L, 'one'), (2L, 'two') AS t(id, label)")
    val data = if (empty) values.where("id < 0") else values
    def use(name: String, rows: Seq[Row]): Unit = {
      val scan = NamedScan
        .builder()
        .addNames(name)
        .initialSchema(ToSubstraitType.toNamedStruct(spark.table(name).schema))
        .build()
      body(scan, rows)
    }
    kind match {
      case "catalog table" =>
        withTable("self_join_catalog") {
          data.write.saveAsTable("self_join_catalog")
          use("self_join_catalog", data.collect().toSeq)
        }
      case _ =>
        withTempView("self_join_view", "self_join_base") {
          val view = kind match {
            case "temporary values" => data
            case "temporary projection" => data.selectExpr("id + 10 AS id", "upper(label) AS label")
            case "nested temporary view" =>
              data.createOrReplaceTempView("self_join_base")
              spark.sql("SELECT id, label FROM self_join_base")
          }
          view.createOrReplaceTempView("self_join_view")
          use("self_join_view", view.collect().toSeq)
        }
    }
  }

  private def assertRows(rel: Rel, expected: Seq[Row]): Unit = {
    val names = (0 until rel.getRecordType.fields().size()).map(i => s"out_$i")
    val plan = builder.plan(behavior, builder.root(rel, names.asJava))
    val proto = new PlanProtoConverter().toProto(plan)
    val restored = new ProtoPlanConverter().from(proto)
    val logical = new ToLogicalPlan(spark).convert(restored)
    assert(logical.resolved)
    logical.collect {
      case join: SparkJoin =>
        assert(join.left.outputSet.intersect(join.right.outputSet).isEmpty)
    }
    assertResult(names)(logical.output.map(_.name))
    assertResult(rel.getRecordType)(ToSubstraitType.toNamedStruct(logical.schema).struct())
    val actual = DatasetUtil.fromLogicalPlan(spark, logical).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  for {
    kind <- Seq(
      "temporary values",
      "temporary projection",
      "nested temporary view",
      "catalog table")
    empty <- Seq(false, true)
  } {
    test(s"named self-join over $kind has independent attributes, empty=$empty") {
      withScan(kind, empty) {
        (scan, rows) =>
          val cross = builder.join(_ => builder.bool(true), Join.JoinType.INNER, scan, scan)
          val expectedCross =
            for (left <- rows; right <- rows)
              yield Row.fromSeq(left.toSeq ++ right.toSeq)
          assertRows(cross, expectedCross)

          val equi = builder.join(
            input =>
              builder.equal(builder.fieldReference(input, 0), builder.fieldReference(input, 2)),
            Join.JoinType.INNER,
            scan,
            scan)
          assertRows(equi, rows.map(row => Row.fromSeq(row.toSeq ++ row.toSeq)))
      }
    }
  }

  test("temporary-view self-join keeps read filters and emit bound to their own input") {
    withScan("temporary values", empty = false) {
      (scan, _) =>
        val left = NamedScan.builder().from(scan).remap(builder.remap(0)).build()
        val right = NamedScan
          .builder()
          .from(scan)
          .filter(builder.equal(builder.fieldReference(scan, 0), builder.i64(2)))
          .remap(builder.remap(0))
          .build()
        val joined = builder.join(
          input =>
            builder.equal(builder.fieldReference(input, 0), builder.fieldReference(input, 1)),
          Join.JoinType.LEFT,
          left,
          right)
        assertRows(joined, Seq(Row(1L, null), Row(2L, 2L)))
    }
  }
}
