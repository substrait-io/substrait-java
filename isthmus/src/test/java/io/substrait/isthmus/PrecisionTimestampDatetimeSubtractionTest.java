package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import java.util.List;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.jupiter.api.Test;

/**
 * Test class for precision timestamp datetime subtraction operations. Tests the mapping between
 * Calcite datetime subtraction and Substrait's subtract function for precision_timestamp and
 * precision_timestamp_tz types.
 */
class PrecisionTimestampDatetimeSubtractionTest extends PlanTestBase {

  static String CREATES =
      "CREATE TABLE events ("
          + "event_id INT, "
          + "event_date DATE, "
          + "event_timestamp TIMESTAMP(3), "
          + "event_timestamp_tz TIMESTAMP(6) WITH LOCAL TIME ZONE"
          + ")";

  @Test
  void dateSubtractIntervalYear() throws Exception {
    String query = "SELECT event_date - INTERVAL '1' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractIntervalMonth() throws Exception {
    String query = "SELECT event_date - INTERVAL '3' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractIntervalYearToMonth() throws Exception {
    String query = "SELECT event_date - INTERVAL '1-6' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractIntervalDay() throws Exception {
    String query = "SELECT event_date - INTERVAL '5' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractIntervalUnparsesAsSqlArithmetic() throws Exception {
    String query = "SELECT event_date - INTERVAL '5' DAY FROM events";
    Plan plan = assertProtoPlanRoundrip(query, new SqlToSubstrait(), CREATES);

    String generatedSql = new SubstraitToSql().convert(plan, PostgresqlSqlDialect.DEFAULT).get(0);
    assertFalse(generatedSql.contains("DATETIME_SUBTRACT"));
    assertTrue(generatedSql.contains("- INTERVAL"));
  }

  @Test
  void timezoneDatetimeSubtractUnparsesAsFunctionCall() {
    Expression.ScalarFunctionInvocation subtraction =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "subtract:ptstz_iyear_str",
            R.precisionTimestampTZ(3),
            ExpressionCreator.precisionTimestampTZ(false, 0, 3),
            ExpressionCreator.intervalYear(false, 1, 0),
            ExpressionCreator.string(false, "UTC"));
    Project project = sb.project(input -> List.of(subtraction), sb.emptyVirtualTableScan());
    Plan plan = sb.plan(sb.root(project, List.of("result")));

    String generatedSql = new SubstraitToSql().convert(plan, PostgresqlSqlDialect.DEFAULT).get(0);
    assertTrue(generatedSql.contains("DATETIME_SUBTRACT("));
    assertTrue(generatedSql.contains(", 'UTC')"));
    assertFalse(generatedSql.contains(") 'UTC'"));
  }

  @Test
  void precisionTimestampSubtractIntervalYear() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '1' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalMonth() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '3' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalYearToMonth() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '1-6' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalDay() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '5' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalHour() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '12' HOUR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalMinute() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '30' MINUTE FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalSecond() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '45' SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractIntervalDayToSecond() throws Exception {
    String query = "SELECT event_timestamp - INTERVAL '1 12:30:45' DAY TO SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalYear() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '2' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalMonth() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '6' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalYearToMonth() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '2-3' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalDay() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '10' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalHour() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '6' HOUR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalMinute() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '15' MINUTE FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalSecond() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '30' SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractIntervalDayToSecond() throws Exception {
    String query = "SELECT event_timestamp_tz - INTERVAL '2 06:15:30' DAY TO SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void multiplePrecisionTimestampSubtractions() throws Exception {
    String query =
        "SELECT "
            + "event_timestamp - INTERVAL '1' YEAR, "
            + "event_timestamp - INTERVAL '5' DAY, "
            + "event_timestamp_tz - INTERVAL '2' MONTH, "
            + "event_timestamp_tz - INTERVAL '12' HOUR "
            + "FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events "
            + "WHERE event_timestamp - INTERVAL '1' DAY < TIMESTAMP '2024-01-01 00:00:00'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzSubtractionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events "
            + "WHERE event_timestamp_tz - INTERVAL '1' MONTH < TIMESTAMP WITH LOCAL TIME ZONE '2024-01-01 00:00:00'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampSubtractionWithComparison() throws Exception {
    String query =
        "SELECT event_id, event_timestamp FROM events "
            + "WHERE event_timestamp - INTERVAL '7' DAY > event_timestamp - INTERVAL '14' DAY";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events " + "WHERE event_date - INTERVAL '1' DAY < DATE '2024-01-01'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateSubtractionWithComparison() throws Exception {
    String query =
        "SELECT event_id, event_date FROM events "
            + "WHERE event_date - INTERVAL '7' DAY > event_date - INTERVAL '14' DAY";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void multipleDateSubtractions() throws Exception {
    String query =
        "SELECT "
            + "event_date - INTERVAL '1' YEAR, "
            + "event_date - INTERVAL '5' DAY, "
            + "event_date - INTERVAL '2' MONTH "
            + "FROM events";
    assertFullRoundTrip(query, CREATES);
  }
}
