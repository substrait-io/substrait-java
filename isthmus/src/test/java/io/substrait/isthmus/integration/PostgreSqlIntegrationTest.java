package io.substrait.isthmus.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.Plan;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer.NoDriverFoundException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@Testcontainers
class PostgreSqlIntegrationTest extends PlanTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlIntegrationTest.class);

  // TODO: These queries produce different results when generated from Substrait
  private static final List<Integer> EXCLUDED_QUERIES = List.of(14);

  private static final DockerImageName UV_IMAGE =
      DockerImageName.parse("ghcr.io/astral-sh/uv:python3.14-trixie-slim");
  private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:18");

  private static final String TPCH_DATA_HOST_PATH = "tpch/data";
  private static final String TPCH_DATA_CONTAINER_PATH = "/tmp/tpc-h";
  private static final String TPCH_INIT_HOST_PATH = "tpch/postgresql/tpch_init.sql";

  private static final List<String> TPCHGEN_ARGS =
      List.of(
          "--scale-factor", "0.001", "--format", "csv", "--output-dir", TPCH_DATA_CONTAINER_PATH);

  private static final List<String> TPCHGEN_CMD =
      Stream.concat(
              Stream.of("uvx", "--from", "tpchgen-cli == 2.*", "tpchgen-cli"),
              TPCHGEN_ARGS.stream())
          .collect(Collectors.toList());

  /** Create TPC-H test data. */
  @Container
  @SuppressWarnings("resource")
  private static final GenericContainer<?> tpchgen =
      new GenericContainer<>(UV_IMAGE)
          .withClasspathResourceMapping(
              TPCH_DATA_HOST_PATH, TPCH_DATA_CONTAINER_PATH, BindMode.READ_WRITE)
          .withCommand(TPCHGEN_CMD.toArray(new String[0]))
          .withStartupCheckStrategy(new SuccessfulExitCheckStrategy());

  /** PostgreSQL instance shared across all test methods in this class. */
  @Container
  @SuppressWarnings("resource")
  private static final PostgreSQLContainer postgres =
      new PostgreSQLContainer(POSTGRES_IMAGE)
          .dependsOn(tpchgen)
          .withClasspathResourceMapping(
              TPCH_DATA_HOST_PATH, TPCH_DATA_CONTAINER_PATH, BindMode.READ_ONLY)
          .withClasspathResourceMapping(
              TPCH_INIT_HOST_PATH, "/docker-entrypoint-initdb.d/tpch_init.sql", BindMode.READ_ONLY);

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
    return IntStream.rangeClosed(1, 22).filter(i -> !EXCLUDED_QUERIES.contains(i));
  }

  @ParameterizedTest
  @MethodSource("tpcHTestCases")
  void testTpcH(final int queryNo)
      throws NoDriverFoundException, SQLException, IOException, SqlParseException {

    final String inputSql = asString(String.format("tpch/queries/%02d.sql", queryNo));
    final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    final Plan plan = sqlToSubstrait.convert(inputSql, TPCH_CATALOG);

    final ConverterProvider provider = new ConverterProvider(extensions);
    final SubstraitToSql substraitToSql = new SubstraitToSql(provider);

    final String generatedSql = substraitToSql.convert(plan, PostgresqlSqlDialect.DEFAULT).get(0);

    final String referenceSql = asString(String.format("tpch/postgresql/%02d.sql", queryNo));

    final String compareSql =
        String.format(COMPARE_RESULTS_SQL_TEMPLATE, referenceSql, generatedSql);

    LOG.atDebug().log(compareSql);

    try (Connection conn = postgres.createConnection("");
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(compareSql); ) {
      // we expect exactly one row
      assertTrue(result.next());

      // the count should be zero if both the reference and generated SQL produce the same results
      int differenceCount = result.getInt(1);
      assertEquals(
          0,
          differenceCount,
          String.format(
              "Reference and generated SQL produce %d different results.\n\nReference SQL:\n%s\n\nGenerated SQL:\n%s",
              differenceCount, referenceSql, generatedSql));

      // we expect exactly one row
      assertFalse(result.next());
    }
  }
}
