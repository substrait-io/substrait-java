package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Add, And, AttributeReference, EqualTo, Expression, Or, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}

import io.substrait.dsl.SubstraitBuilder
import io.substrait.extension.ExtensionCollector
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.RelProtoConverter

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
    assertSqlQuery(
      s"""SELECT l.a, l.b FROM exists_left l
         |WHERE EXISTS (SELECT 1 FROM exists_right r WHERE $condition) OR l.keep""".stripMargin,
      expected: _*)
  }

  private def assertSqlQuery(sql: String, expected: Row*): Unit = {
    val query = spark.sql(sql)
    val optimized = query.queryExecution.optimizedPlan
    assert(optimized.exists {
      case Join(_, _, _: ExistenceJoin, _, _) => true
      case _ => false
    })
    val nativeRows = query.collect().toSeq.sortBy(_.toString)
    assertResult(expected.sortBy(_.toString))(nativeRows)

    assertConverted(optimized, nativeRows)
  }

  private def assertConverted(
      plan: LogicalPlan,
      expected: Seq[Row],
      converter: ToSubstraitRel = new ToSubstraitRel()): Unit = {
    val exported = converter.convert(plan)
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

  Seq("r.a = l.a", "l.a = r.a").foreach {
    condition =>
      test(s"NOT EXISTS under OR retains unmatched null keys: $condition") {
        withTempView("null_exists_left", "null_exists_right") {
          spark
            .sql("SELECT * FROM VALUES (1, NULL, false), (2, 1, false), (3, 2, false), " +
              "(4, NULL, true), (5, 3, false) AS l(id, a, keep)")
            .createOrReplaceTempView("null_exists_left")
          spark
            .sql("SELECT * FROM VALUES (2), (NULL) AS r(a)")
            .createOrReplaceTempView("null_exists_right")
          assertSqlQuery(
            s"""SELECT l.id FROM null_exists_left l
               |WHERE NOT EXISTS (SELECT 1 FROM null_exists_right r WHERE $condition) OR l.keep""".stripMargin,
            Row(1),
            Row(2),
            Row(4),
            Row(5)
          )
        }
      }
  }

  test("ordinary imported NOT IN keeps unknown distinct from false") {
    val builder = new SubstraitBuilder
    val left = new ToSubstraitRel().visit(
      spark
        .sql("SELECT * FROM VALUES (CAST(NULL AS INT)), (1), (2) AS l(a)")
        .queryExecution
        .optimizedPlan)
    val right = new ToSubstraitRel().visit(
      spark.sql("SELECT * FROM VALUES (2), (NULL) AS r(a)").queryExecution.optimizedPlan)
    val predicate = builder.inPredicate(right, builder.fieldReference(left, 0))
    val filtered = builder.filter(_ => builder.not(predicate), left)
    val restored = new ToLogicalPlan(spark).convert(filtered)
    assert(DatasetUtil.fromLogicalPlan(spark, restored).collect().isEmpty)
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

  test("wide existence keys fit the default protobuf nesting limit") {
    val names = (0 until 40).map(i => s"k$i").mkString(", ")
    val values = (1 to 40).mkString(", ")
    val left = spark.sql(s"SELECT * FROM VALUES ($values) AS l($names)").queryExecution.analyzed
    val right = spark.sql(s"SELECT * FROM VALUES ($values) AS r($names)").queryExecution.analyzed
    val exists = AttributeReference("exists", BooleanType, nullable = false)()
    val condition = left.output
      .zip(right.output)
      .map { case (l, r) => EqualTo(l, r): Expression }
      .reduceLeft(And)
    val joined = Join(left, right, ExistenceJoin(exists), Some(condition), JoinHint.NONE)
    val plan = Project(left.output.take(1), Filter(exists, joined))
    assert(plan.resolved)

    val exported = new ToSubstraitRel().visit(plan)
    val proto = new RelProtoConverter(new ExtensionCollector).toProto(exported)
    assertResult(proto)(io.substrait.proto.Rel.parseFrom(proto.toByteArray))
    assertConverted(plan, Seq(Row(1)))
  }

  test("existence null guard reuses the converted scalar-subquery needle") {
    withTempView("exists_weights") {
      spark
        .createDataFrame(
          sparkContext.parallelize(Seq(Row(1))),
          StructType(Seq(StructField("v", IntegerType, nullable = false))))
        .createOrReplaceTempView("exists_weights")
      val weights = spark.sql("SELECT MAX(v) FROM exists_weights").queryExecution.analyzed
      val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(k)").queryExecution.analyzed
      val right = spark.sql("SELECT * FROM VALUES (2) AS r(k)").queryExecution.analyzed
      val exists = AttributeReference("exists", BooleanType, nullable = false)()
      val condition = EqualTo(Add(left.output.head, ScalarSubquery(weights)), right.output.head)
      val joined = Join(left, right, ExistenceJoin(exists), Some(condition), JoinHint.NONE)
      val plan = Project(left.output, Filter(exists, joined))
      assert(plan.resolved)
      var rddVisits = 0
      val converter = new ToSubstraitRel {
        override def default(p: LogicalPlan): io.substrait.relation.Rel = {
          if (p.isInstanceOf[LogicalRDD]) {
            rddVisits += 1
          }
          super.default(p)
        }
      }
      assertConverted(plan, Seq(Row(1)), converter)
      assertResult(1)(rddVisits)
    }
  }

}
