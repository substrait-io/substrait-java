package io.substrait.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

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

  test("local_relation_null") {
    assertSqlSubstraitRelRoundTrip(
      "select * from (values (1), (NULL) as table(col))"
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
}
