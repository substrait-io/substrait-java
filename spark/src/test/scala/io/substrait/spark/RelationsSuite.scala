package io.substrait.spark

import io.substrait.spark.compat.SparkCompat
import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, InSubquery, Literal, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, Project}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

class RelationsSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("local_relation_simple") {
    assertSqlSubstraitRelRoundTrip(
      "select * from (values (1, 'a'), (2, 'b') as table(col1, col2))"
    )
  }

  Seq(
    (
      "retained input",
      "col1, col1 in (select x from values (1) as t2(x)) as flag",
      Seq(Row(1, true), Row(2, false))),
    (
      "predicate only",
      "col1 in (select x from values (1) as t2(x)) as flag",
      Seq(Row(true), Row(false))),
    (
      "multiple predicates",
      "col1 in (select x from values (1) as t2(x)) as first, " +
        "col1 in (select x from values (2) as t3(x)) as second, col1",
      Seq(Row(true, false, 1), Row(false, true, 2)))
  ).foreach {
    case (name, projection, expected) =>
      test(s"select-list IN subquery with $name") {
        val query = s"select $projection from values (1), (2) as t(col1)"
        def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)
        assertResult(sorted(expected))(sorted(spark.sql(query).collect().toSeq))
        val converted = assertSqlSubstraitRelRoundTrip(query)
        assertResult(sorted(expected))(
          sorted(DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq))
      }
  }

  Seq("retained input", "reordered input", "predicate only").foreach {
    shape =>
      test(s"nested Project retains a materialized existence column: $shape") {
        val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(x)").queryExecution.analyzed
        val right = spark.sql("SELECT * FROM VALUES (1) AS r(x)").queryExecution.analyzed
        val exists = AttributeReference("exists", BooleanType, nullable = false)()
        val joined = Join(
          left,
          right,
          ExistenceJoin(exists),
          Some(EqualTo(left.output.head, right.output.head)),
          JoinHint.NONE)
        val columns: Seq[NamedExpression] = shape match {
          case "retained input" => Seq(left.output.head, exists)
          case "reordered input" => Seq(exists, left.output.head)
          case "predicate only" => Seq(exists)
        }
        // Keep the existence attribute's ID rather than introducing a fresh alias.
        val inner = Project(columns, joined)
        val plan = Project(inner.output, inner)
        assert(plan.resolved)
        val expected = shape match {
          case "retained input" => Seq(Row(1, true), Row(2, false))
          case "reordered input" => Seq(Row(true, 1), Row(false, 2))
          case "predicate only" => Seq(Row(true), Row(false))
        }
        def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)
        assertResult(sorted(expected))(
          sorted(DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq))
        val exported = new ToSubstraitRel().convert(plan)
        val restored = new ToLogicalPlan(spark).convert(exported)
        assertResult(plan.schema)(restored.schema)
        assertResult(sorted(expected))(
          sorted(DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq))
      }
  }

  for {
    kind <- Seq("scalar", "IN")
    order <- Seq("subquery first", "existence first")
  } {
    test(s"$kind subquery keeps enclosing existence-column state: $order") {
      val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(x)").queryExecution.analyzed
      val right = spark.sql("SELECT * FROM VALUES (1) AS r(x)").queryExecution.analyzed
      val exists = AttributeReference("exists", BooleanType, nullable = false)()
      val joined = Join(
        left,
        right,
        ExistenceJoin(exists),
        Some(EqualTo(left.output.head, right.output.head)),
        JoinHint.NONE)
      // Reuse the join and existence ID in the independent subquery plan.
      val subqueryPlan = Project(Seq(exists), Filter(EqualTo(left.output.head, Literal(1)), joined))
      val subquery = kind match {
        case "scalar" => ScalarSubquery(subqueryPlan)
        case "IN" =>
          InSubquery(
            Seq(Literal(true)),
            SparkCompat.instance.createListQuery(subqueryPlan, subqueryPlan.output))
      }
      val projected = Alias(subquery, "subquery_exists")()
      val columns = order match {
        case "subquery first" => Seq(projected, exists)
        case "existence first" => Seq(exists, projected)
      }
      val plan = Project(left.output ++ columns, joined)
      assert(plan.resolved)
      val expected = order match {
        case "subquery first" => Seq(Row(1, true, true), Row(2, true, false))
        case "existence first" => Seq(Row(1, true, true), Row(2, false, true))
      }
      def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)
      assertResult(sorted(expected))(
        sorted(DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq))
      val exported = new ToSubstraitRel().convert(plan)
      val restored = new ToLogicalPlan(spark).convert(exported)
      assertResult(plan.schema)(restored.schema)
      assertResult(sorted(expected))(
        sorted(DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq))
    }
  }

  test("sibling plan preserves an unmaterialized existence mapping") {
    val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(x)").queryExecution.analyzed
    val right = spark.sql("SELECT * FROM VALUES (1) AS r(x)").queryExecution.analyzed
    val exists = AttributeReference("exists", BooleanType, nullable = false)()
    val joined = Join(
      left,
      right,
      ExistenceJoin(exists),
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    val materialized = Project(Seq(exists), joined)
    val sibling = Project(Seq(Alias(Literal(true), "right_flag")()), materialized)
    val cross = Join(joined, sibling, Cross, None, JoinHint.NONE)
    val plan = Project(Seq(Alias(exists, "left_exists")()), cross)
    assert(plan.resolved)
    val expected = Seq(Row(false), Row(false), Row(true), Row(true))
    def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)
    assertResult(expected)(sorted(DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq))
    val exported = new ToSubstraitRel().convert(plan)
    val restored = new ToLogicalPlan(spark).convert(exported)
    assertResult(plan.schema)(restored.schema)
    assertResult(expected)(sorted(DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq))
  }

  test("project binds fields after an omitted existence column") {
    val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(x)").queryExecution.analyzed
    val right = spark.sql("SELECT * FROM VALUES (1) AS r(x)").queryExecution.analyzed
    val exists = AttributeReference("exists", BooleanType, nullable = false)()
    val joined = Join(
      left,
      right,
      ExistenceJoin(exists),
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    val sibling = spark.sql("SELECT * FROM VALUES (9) AS s(y)").queryExecution.analyzed
    val cross = Join(joined, sibling, Cross, None, JoinHint.NONE)
    val plan = Project(Seq(sibling.output.head), cross)
    assert(plan.resolved)
    val expected = Seq(Row(9), Row(9))
    assertResult(expected)(DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq)
    val exported = new ToSubstraitRel().convert(plan)
    val restored = new ToLogicalPlan(spark).convert(exported)
    assertResult(plan.schema)(restored.schema)
    assertResult(expected)(DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq)
  }

  Seq("left", "right").foreach {
    side =>
      test(s"conditioned join materializes the $side existence column") {
        val left = spark.sql("SELECT * FROM VALUES (1), (2) AS l(x)").queryExecution.analyzed
        val right = spark.sql("SELECT * FROM VALUES (1) AS r(x)").queryExecution.analyzed
        val exists = AttributeReference("exists", BooleanType, nullable = false)()
        val joined = Join(
          left,
          right,
          ExistenceJoin(exists),
          Some(EqualTo(left.output.head, right.output.head)),
          JoinHint.NONE)
        val sibling = spark.sql("SELECT * FROM VALUES (1), (2) AS s(y)").queryExecution.analyzed
        val (first, second) = if (side == "left") (joined, sibling) else (sibling, joined)
        val join = Join(
          first,
          second,
          Inner,
          Some(EqualTo(left.output.head, sibling.output.head)),
          JoinHint.NONE)
        val plan = Project(Seq(sibling.output.head, left.output.head, exists), join)
        assert(plan.resolved)
        val expected = Seq(Row(1, 1, true), Row(2, 2, false))
        def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)
        assertResult(expected)(sorted(DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq))
        val exported = new ToSubstraitRel().convert(plan)
        val restored = new ToLogicalPlan(spark).convert(exported)
        assertResult(plan.schema)(restored.schema)
        assertResult(expected)(sorted(DatasetUtil.fromLogicalPlan(spark, restored).collect().toSeq))
      }
  }

  test("local_relation_null") {
    assertSqlSubstraitRelRoundTrip(
      "select * from (values (1), (NULL) as table(col))"
    )
  }

  test("one_row_relation") {
    assertSqlSubstraitRelRoundTrip(
      "select 1 + 1"
    )
  }

  test("local_relation_struct") {
    assertSqlSubstraitRelRoundTrip(
      "select * from (values (1, struct(2, 'a')) as table(int_col, struct_col))"
    )

    assertSqlSubstraitRelRoundTrip(
      // the struct() cast gets evaluated into a literal struct value by spark
      "select * from (values (1, cast(struct(1, 'a') as struct<f1: int, f2: string>)) as table(int_col, col))"
    )
  }

  test("create_dataset - LocalRelation") {
    val spark = this.spark
    import spark.implicits._

    val df = Seq(
      (1, "one"),
      (2, "two"),
      (3, "three")
    ).toDF("id", "value")

    assertSparkSubstraitRelRoundTrip(df.queryExecution.optimizedPlan)
  }

  test("createdataframe - LogicalRDD") {
    val data = Seq(
      Row(1, "one"),
      Row(2, "two"),
      Row(3, "three")
    )

    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("value", StringType, true)
      ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    assertSparkSubstraitRelRoundTrip(df.queryExecution.optimizedPlan)
  }

  test("Limit RDD size") {
    val data = Seq(
      Row(1, "one"),
      Row(2, "two"),
      Row(3, "three"),
      Row(4, "four")
    )

    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("value", StringType, true)
      ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    assertResult(4)(df.count())

    val plan = assertSparkSubstraitRelRoundTrip(df.queryExecution.optimizedPlan, 2)
    assertResult(2)(plan.count())
  }
}
