package io.substrait.isthmus.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.IntStream;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer.NoDriverFoundException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class PostgreSqlIntegrationTest extends PlanTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlIntegrationTest.class);

  @Container
  PostgreSQLContainer postgres =
      new PostgreSQLContainer("postgres:18")
          .withClasspathResourceMapping("tpch/data", "/tmp/tpc-h", BindMode.READ_ONLY)
          .withClasspathResourceMapping(
              "tpch/postgresql/tpch_init.sql",
              "/docker-entrypoint-initdb.d/tpch_init.sql",
              BindMode.READ_ONLY);

  private static final String COMPARE_RESULTS_SQL_TEMPLATE =
      """
      WITH expected AS (%s),
          actual AS (%s)
      SELECT count(*) FROM (
        SELECT * FROM
          (SELECT * FROM expected EXCEPT SELECT * FROM actual)
          UNION (SELECT * FROM actual EXCEPT SELECT * FROM expected)
      )
      """;

  static IntStream tpcHTestCases() {
    // TODO: query 21 currently does not produce the same result when run through Substrait
    return IntStream.rangeClosed(1, 22).filter(i -> i != 21);
  }

  @ParameterizedTest
  @MethodSource("tpcHTestCases")
  void testTpcH(int queryNo)
      throws NoDriverFoundException, SQLException, IOException, SqlParseException {

    String inputSql = asString(String.format("tpch/queries/%02d.sql", queryNo));
    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    Plan plan = sqlToSubstrait.convert(inputSql, TPCH_CATALOG);

    SubstraitToSql substraitToSql = new SubstraitToSql(extensions);

    String generatedSql = substraitToSql.convert(plan, PostgresqlSqlDialect.DEFAULT).get(0);

    String referenceSql = asString(String.format("tpch/postgresql/%02d.sql", queryNo));

    String compareSql = String.format(COMPARE_RESULTS_SQL_TEMPLATE, referenceSql, generatedSql);

    LOG.atInfo().log(compareSql);

    try (Connection conn = postgres.createConnection("");
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(compareSql); ) {
      // we expect exactly one row
      assertTrue(result.next());

      // the count should be zero if both the reference and generated SQL produce the same results
      assertEquals(0, result.getInt(1));

      // we expect exactly one row
      assertFalse(result.next());
    }
  }
}
