package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Or}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}

class ExistenceJoinSuite extends SparkFunSuite with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  private def withInputs(body: => Unit): Unit = {
    withTempView("exists_left", "exists_right") {
      spark
        .sql(
          "SELECT * FROM VALUES (1, 10, false), (2, 20, false), (3, 30, true), " +
            "(4, NULL, false), (5, 50, false), (NULL, 40, true) AS l(a, b, keep)")
        .createOrReplaceTempView("exists_left")
      spark
        .sql("SELECT * FROM VALUES (10, 1), (20, 2), (99, 3), (NULL, 4) AS r(a, b)")
        .createOrReplaceTempView("exists_right")
      body
    }
  }

  private def assertQuery(condition: String, expected: Row*): Unit = {
    val query = spark.sql(
      s"""SELECT l.a, l.b FROM exists_left l
         |WHERE EXISTS (SELECT 1 FROM exists_right r WHERE $condition) OR l.keep""".stripMargin)
    val optimized = query.queryExecution.optimizedPlan
    assert(optimized.exists {
      case Join(_, _, _: ExistenceJoin, _, _) => true
      case _ => false
    })
    val nativeRows = query.collect().toSeq.sortBy(_.toString)
    assertResult(expected.sortBy(_.toString))(nativeRows)

    assertConverted(optimized, nativeRows)
  }

  private def assertConverted(plan: LogicalPlan, expected: Seq[Row]): Unit = {
    val exported = new ToSubstraitRel().convert(plan)
    val proto = new PlanProtoConverter().toProto(exported)
    val imported = new ProtoPlanConverter().from(proto)
    val restored = new ToLogicalPlan(spark).convert(imported)
    assertResult(plan.schema)(restored.schema)
    val actual = DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq.sortBy(_.toString)
    assertResult(expected.sortBy(_.toString))(actual)
  }

  Seq("r.b = l.a", "l.a = r.b").foreach {
    condition =>
      test(s"EXISTS under OR binds either equality operand order: $condition") {
        withInputs {
          assertQuery(condition, Row(1, 10), Row(2, 20), Row(3, 30), Row(4, null), Row(null, 40))
        }
      }
  }

  for {
    first <- Seq("r.b = l.a", "l.a = r.b")
    second <- Seq("r.a = l.b", "l.b = r.a")
  } {
    test(s"EXISTS under OR aligns composite keys: $first AND $second") {
      withInputs {
        assertQuery(s"$first AND $second", Row(1, 10), Row(2, 20), Row(3, 30), Row(null, 40))
      }
    }
  }

  test("EXISTS under OR aligns keys when conjuncts are reordered") {
    withInputs {
      assertQuery("r.a = l.b AND l.a = r.b", Row(1, 10), Row(2, 20), Row(3, 30), Row(null, 40))
    }
  }

  test("EXISTS under OR projects key expressions on both inputs") {
    withInputs {
      assertQuery(
        "r.b + 1 = l.a + 1 AND l.b = r.a",
        Row(1, 10),
        Row(2, 20),
        Row(3, 30),
        Row(null, 40))
    }
  }

  test("existence join keys need not follow the right input's column order") {
    withInputs {
      val left = spark.table("exists_left").queryExecution.analyzed
      val right = spark.table("exists_right").queryExecution.analyzed
      val exists = AttributeReference("exists", BooleanType, nullable = false)()
      val condition =
        And(EqualTo(left.output(0), right.output(1)), EqualTo(right.output(0), left.output(1)))
      val joined = Join(left, right, ExistenceJoin(exists), Some(condition), JoinHint.NONE)
      val plan = Project(left.output.take(2), Filter(Or(exists, left.output(2)), joined))
      assert(plan.resolved)
      val expected = Seq(Row(1, 10), Row(2, 20), Row(3, 30), Row(null, 40))
      assertResult(expected.sortBy(_.toString))(
        DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq.sortBy(_.toString))
      assertConverted(plan, expected)
    }
  }
}
