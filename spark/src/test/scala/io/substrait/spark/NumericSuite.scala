package io.substrait.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class NumericSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  test("basic") {
    assertSqlSubstraitRelRoundTrip(
      "select sqrt(abs(num)), mod(num, 2) from (values (-5), (7.4)) as table(num)"
    )
  }

  test("exponentials") {
    assertSqlSubstraitRelRoundTrip(
      "select power(num, 3), exp(num), ln(num), log10(num) from (values (5), (17)) as table(num)"
    )
  }

  test("trig") {
    assertSqlSubstraitRelRoundTrip(
      "select sin(num), cos(num), tan(num) from (values (30), (90)) as table(num)"
    )
    assertSqlSubstraitRelRoundTrip(
      "select asin(num), acos(num), atan(num) from (values (0.5), (-0.5)) as table(num)"
    )
    assertSqlSubstraitRelRoundTrip(
      "select sinh(num), cosh(num), tanh(num) from (values (30), (90)) as table(num)"
    )
    assertSqlSubstraitRelRoundTrip(
      "select asinh(num), acosh(num), atanh(num) from (values (0.5), (-0.5)) as table(num)"
    )
  }

}
