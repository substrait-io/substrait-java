package io.substrait.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class DateTimeSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("date_add") {
    val qry =
      "select cast(d AS DATE) + interval 5 days from (values ('2025-03-27'), ('2025-01-02')) as table(d)"
    assertSqlSubstraitRelRoundTrip(qry)
  }

  test("date_sub") {
    val qry =
      "select cast(d AS DATE) - interval 5 days from (values ('2025-03-27'), ('2025-01-02')) as table(d)"
    assertSqlSubstraitRelRoundTrip(qry)
  }

  test("extract_year_month") {
    val qry = "select year(cast(d AS DATE)), month(cast(d AS DATE)) " +
      "from (values ('2025-03-27'), ('2025-01-02')) as table(d)"
    assertSqlSubstraitRelRoundTrip(qry)
  }
}
