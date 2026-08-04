package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SqlToSubstraitTest {

  private static final String CREATE_STATEMENT = "CREATE TABLE employees (id BIGINT, name VARCHAR)";

  /**
   * The unquoted-identifier casing configured on the {@link ConverterProvider} is applied to both
   * the CREATE statements and the query, so the table name carried by the resulting {@link
   * NamedScan} follows it.
   */
  @ParameterizedTest
  @CsvSource({"TO_UPPER, EMPLOYEES", "TO_LOWER, employees", "UNCHANGED, employees"})
  void namedScanFollowsProviderUnquotedCasing(Casing casing, String expectedTableName)
      throws Exception {
    ConverterProvider provider = ConverterProvider.builder().unquotedCasing(casing).build();
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    Plan plan = new SqlToSubstrait(provider).convert("SELECT id FROM employees", catalog);

    NamedScan scan = (NamedScan) ((Project) plan.getRoots().get(0).getInput()).getInput();
    assertEquals(expectedTableName, scan.getNames().get(0));
  }
}
