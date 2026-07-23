package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link ConverterProvider#builder()} configures the Calcite {@link SqlParser.Config}
 * used for SQL parsing — via the {@code unquotedCasing} convenience or a full {@code
 * sqlParserConfig} — and that the configured casing is applied consistently across both CREATE
 * TABLE parsing and query parsing, so that the table name stored in a Substrait {@link NamedScan}
 * reflects the configured casing.
 */
class UnquotedCasingTest {

  private static final String CREATE_STATEMENT = "CREATE TABLE employees (id BIGINT, name VARCHAR)";

  @Test
  void defaultCasingIsToUpper() {
    ConverterProvider provider = ConverterProvider.builder().build();
    assertEquals(Casing.TO_UPPER, provider.getSqlParserConfig().unquotedCasing());
  }

  @Test
  void builderCasingUnchanged() {
    ConverterProvider provider =
        ConverterProvider.builder().unquotedCasing(Casing.UNCHANGED).build();
    assertEquals(Casing.UNCHANGED, provider.getSqlParserConfig().unquotedCasing());
  }

  @Test
  void builderCasingToLower() {
    ConverterProvider provider =
        ConverterProvider.builder().unquotedCasing(Casing.TO_LOWER).build();
    assertEquals(Casing.TO_LOWER, provider.getSqlParserConfig().unquotedCasing());
  }

  /**
   * A full {@link SqlParser.Config} supplied to the builder is used verbatim. Deriving it from
   * {@link ConverterProvider#DEFAULT_SQL_PARSER_CONFIG} preserves isthmus' parser defaults (here,
   * {@link SqlConformanceEnum#LENIENT} conformance) while overriding a single setting.
   */
  @Test
  void builderFullSqlParserConfig() {
    SqlParser.Config config =
        ConverterProvider.DEFAULT_SQL_PARSER_CONFIG.withUnquotedCasing(Casing.TO_LOWER);
    ConverterProvider provider = ConverterProvider.builder().sqlParserConfig(config).build();
    assertEquals(Casing.TO_LOWER, provider.getSqlParserConfig().unquotedCasing());
    assertEquals(SqlConformanceEnum.LENIENT, provider.getSqlParserConfig().conformance());
  }

  /**
   * With the default {@link Casing#TO_UPPER}, both CREATE TABLE and SELECT fold unquoted
   * identifiers to upper-case. The resulting {@link NamedScan} table name is {@code EMPLOYEES}.
   */
  @Test
  void defaultCasingFoldsTableNameToUpper() throws Exception {
    ConverterProvider provider = ConverterProvider.builder().build();
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
    ConverterProvider provider =
        ConverterProvider.builder().unquotedCasing(Casing.UNCHANGED).build();
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    Plan plan = new SqlToSubstrait(provider).convert("SELECT id FROM employees", catalog);

    NamedScan scan = (NamedScan) ((Project) plan.getRoots().get(0).getInput()).getInput();
    assertEquals("employees", scan.getNames().get(0));
  }
}
