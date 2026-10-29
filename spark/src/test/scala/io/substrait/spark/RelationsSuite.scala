package io.substrait.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
