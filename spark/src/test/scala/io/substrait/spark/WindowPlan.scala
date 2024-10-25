package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.TPCBase
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * These tests are based on the examples in the Spark documentation on Window functions.
 * https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
 */
class WindowPlan extends TPCBase with SubstraitPlanTestBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  override protected def createTables(): Unit = {
    spark.sql(
      "CREATE TABLE employees (name STRING, dept STRING, salary INT, age INT) USING parquet;")
  }

  override protected def dropTables(): Unit = {
    spark.sessionState.catalog.dropTable(TableIdentifier("employees"), true, true)
  }

  test("rank") {
    val query =
      """
        |SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank FROM employees
        |
        |""".stripMargin
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("cume_dist") {
    val query =
      """
        |SELECT name, dept, age, CUME_DIST() OVER (PARTITION BY dept ORDER BY age
        |    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_dist FROM employees
        |
        |""".stripMargin
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("aggregate") {
    val query =
      """
        |SELECT name, dept, salary, MIN(salary) OVER (PARTITION BY dept ORDER BY salary) AS min
        |    FROM employees
        |
        |""".stripMargin
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("lag/lead") {
    val query =
      """
        |SELECT name, salary,
        |    LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
        |    LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
        |    FROM employees;
        |
        |""".stripMargin
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("different partitions") {
    val query =
      """
        |SELECT name, salary,
        |    LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS lag,
        |    LEAD(age, 1, 0) OVER (PARTITION BY salary ORDER BY age) AS lead
        |    FROM employees;
        |
        |""".stripMargin
    assertSqlSubstraitRelRoundTrip(query)
  }
}
