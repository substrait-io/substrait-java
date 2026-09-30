package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import org.junit.jupiter.api.Test;

/**
 * Test class for precision timestamp datetime addition operations. Tests the mapping of Calcite's
 * DATETIME_PLUS operator to Substrait's add function for precision_timestamp and
 * precision_timestamp_tz types.
 */
class PrecisionTimestampDatetimeAdditionTest extends PlanTestBase {

  static String CREATES =
      "CREATE TABLE events ("
          + "event_id INT, "
          + "event_date DATE, "
          + "event_timestamp TIMESTAMP(3), "
          + "event_timestamp_tz TIMESTAMP(6) WITH LOCAL TIME ZONE"
          + ")";

  @Test
  void dateAddIntervalYear() throws Exception {
    String query = "SELECT event_date + INTERVAL '1' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateAddIntervalMonth() throws Exception {
    String query = "SELECT event_date + INTERVAL '3' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateAddIntervalYearToMonth() throws Exception {
    String query = "SELECT event_date + INTERVAL '1-6' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  /**
   * The spec declares add(date, interval_day&lt;P&gt;) -&gt; precision_timestamp&lt;P&gt;, where
   * Calcite types the sum a DATE. The call carries the declared type and is cast back.
   */
  @Test
  void dateAddIntervalDayCarriesTheDeclaredType() throws Exception {
    Expression.Cast cast =
        assertInstanceOf(
            Expression.Cast.class,
            firstExpression("SELECT event_date + INTERVAL '5' DAY FROM events"));
    assertEquals(N.DATE, cast.getType());
    Expression.ScalarFunctionInvocation addition =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, cast.input());
    assertEquals("add:date_iday", addition.declaration().key());
    assertEquals(N.precisionTimestamp(6), addition.outputType());
  }

  @Test
  void dateAddIntervalDay() throws Exception {
    String query = "SELECT event_date + INTERVAL '5' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalYear() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '1' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalMonth() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '3' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalYearToMonth() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '1-6' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalDay() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '5' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalHour() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '12' HOUR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalMinute() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '30' MINUTE FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalSecond() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '45' SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAddIntervalDayToSecond() throws Exception {
    String query = "SELECT event_timestamp + INTERVAL '1 12:30:45' DAY TO SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalYear() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '2' YEAR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalMonth() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '6' MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalYearToMonth() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '2-3' YEAR TO MONTH FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalDay() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '10' DAY FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalHour() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '6' HOUR FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalMinute() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '15' MINUTE FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalSecond() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '30' SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAddIntervalDayToSecond() throws Exception {
    String query = "SELECT event_timestamp_tz + INTERVAL '2 06:15:30' DAY TO SECOND FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void multiplePrecisionTimestampAdditions() throws Exception {
    String query =
        "SELECT "
            + "event_timestamp + INTERVAL '1' YEAR, "
            + "event_timestamp + INTERVAL '5' DAY, "
            + "event_timestamp_tz + INTERVAL '2' MONTH, "
            + "event_timestamp_tz + INTERVAL '12' HOUR "
            + "FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAdditionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events "
            + "WHERE event_timestamp + INTERVAL '1' DAY > TIMESTAMP '2024-01-01 00:00:00'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampTzAdditionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events "
            + "WHERE event_timestamp_tz + INTERVAL '1' MONTH > TIMESTAMP WITH LOCAL TIME ZONE '2024-01-01 00:00:00'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void precisionTimestampAdditionWithComparison() throws Exception {
    String query =
        "SELECT event_id, event_timestamp FROM events "
            + "WHERE event_timestamp + INTERVAL '7' DAY < event_timestamp + INTERVAL '14' DAY";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateAdditionInWhereClause() throws Exception {
    String query =
        "SELECT event_id FROM events " + "WHERE event_date + INTERVAL '1' DAY > DATE '2024-01-01'";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void dateAdditionWithComparison() throws Exception {
    String query =
        "SELECT event_id, event_date FROM events "
            + "WHERE event_date + INTERVAL '7' DAY < event_date + INTERVAL '14' DAY";
    assertFullRoundTrip(query, CREATES);
  }

  @Test
  void multipleDateAdditions() throws Exception {
    String query =
        "SELECT "
            + "event_date + INTERVAL '1' YEAR, "
            + "event_date + INTERVAL '5' DAY, "
            + "event_date + INTERVAL '2' MONTH "
            + "FROM events";
    assertFullRoundTrip(query, CREATES);
  }

  private static Expression firstExpression(String query) throws Exception {
    Plan plan =
        new SqlToSubstrait()
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
    return ((Project) plan.getRoots().get(0).getInput()).getExpressions().get(0);
  }
}
