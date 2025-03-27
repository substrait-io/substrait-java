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

  test("one_row_relation") {
    assertSqlSubstraitRelRoundTrip(
      "select 1 + 1"
    )
  }

}
