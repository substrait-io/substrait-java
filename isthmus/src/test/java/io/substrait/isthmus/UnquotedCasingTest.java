package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the {@link ConverterProvider} unquoted casing parameter controls identifier casing
 * consistently across both CREATE TABLE parsing and query parsing, so that the table name stored in
 * a Substrait {@link NamedScan} reflects the configured casing.
 */
class UnquotedCasingTest {

  private static final String CREATE_STATEMENT = "CREATE TABLE employees (id BIGINT, name VARCHAR)";

  @Test
  void defaultCasingIsToUpper() {
    ConverterProvider provider = new ConverterProvider();
    assertEquals(Casing.TO_UPPER, provider.getSqlParserConfig().unquotedCasing());
    assertEquals(Casing.TO_UPPER, provider.getUnquotedCasing());
  }

  @Test
  void constructorCasingUnchanged() {
    ConverterProvider provider = new ConverterProvider(Casing.UNCHANGED);
    assertEquals(Casing.UNCHANGED, provider.getSqlParserConfig().unquotedCasing());
    assertEquals(Casing.UNCHANGED, provider.getUnquotedCasing());
  }

  @Test
  void constructorCasingToLower() {
    ConverterProvider provider = new ConverterProvider(Casing.TO_LOWER);
    assertEquals(Casing.TO_LOWER, provider.getSqlParserConfig().unquotedCasing());
    assertEquals(Casing.TO_LOWER, provider.getUnquotedCasing());
  }

  /**
   * With the default {@link Casing#TO_UPPER}, both CREATE TABLE and SELECT fold unquoted
   * identifiers to upper-case. The resulting {@link NamedScan} table name is {@code EMPLOYEES}.
   */
  @Test
  void defaultCasingFoldsTableNameToUpper() throws Exception {
    ConverterProvider provider = new ConverterProvider();
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    Plan plan = new SqlToSubstrait(provider).convert("SELECT id FROM employees", catalog);

    NamedScan scan = (NamedScan) ((Project) plan.getRoots().get(0).getInput()).getInput();
    assertEquals("EMPLOYEES", scan.getNames().get(0));
  }

  /**
   * With {@link Casing#UNCHANGED}, both CREATE TABLE and SELECT preserve the written casing of
   * unquoted identifiers. A query using lowercase {@code employees} against a catalog built from
   * {@code CREATE TABLE employees} therefore produces a {@link NamedScan} with name {@code
   * employees}.
   */
  @Test
  void unchangedCasingPreservesTableName() throws Exception {
    ConverterProvider provider = new ConverterProvider(Casing.UNCHANGED);
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    Plan plan = new SqlToSubstrait(provider).convert("SELECT id FROM employees", catalog);

    NamedScan scan = (NamedScan) ((Project) plan.getRoots().get(0).getInput()).getInput();
    assertEquals("employees", scan.getNames().get(0));
  }
}
