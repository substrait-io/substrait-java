package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Version;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * Take a SQL statement and a set of table definitions and return a substrait plan.
 *
 * <p>Conversion behaviours can be customized using a {@link ConverterProvider}
 */
public class SqlToSubstrait extends SqlConverterBase {

  /** Creates a SQL-to-Substrait converter using the default configuration. */
  public SqlToSubstrait() {
    this(ConverterProvider.DEFAULT);
  }

  /**
   * Creates a SQL-to-Substrait converter with explicit extensions and features.
   *
   * @param converterProvider Converter Provider for the configuration
   */
  public SqlToSubstrait(ConverterProvider converterProvider) {
    super(converterProvider);
  }

  /**
   * Converts one or more SQL statements into a Substrait {@link Plan}.
   *
   * @param sqlStatements a string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @return the Substrait {@link Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public Plan convert(final String sqlStatements, final Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    Builder builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());
    builder.executionBehavior(converterProvider.getExecutionBehavior());

    // TODO: consider case in which one sql passes conversion while others don't
    SubstraitSqlToCalcite.convertQueries(sqlStatements, catalogReader, converterProvider).stream()
        .map(root -> SubstraitRelVisitor.convert(root, converterProvider))
        .forEach(root -> builder.addRoots(root));

    return builder.build();
  }

  /**
   * Converts one or more SQL statements into a Substrait {@link Plan}.
   *
   * <p>The {@code sqlDialect} parameter was previously used to influence identifier casing during
   * parsing. This is now configurable via {@link ConverterProvider#builder()} (for example {@code
   * ConverterProvider.builder().unquotedCasing(...)}), or for fully custom parser behaviour, by
   * subclassing {@link ConverterProvider} and overriding {@link
   * ConverterProvider#getSqlParserConfig()}.
   *
   * @param sqlStatements a string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @param sqlDialect The sql dialect to use for parsing.
   * @return the Substrait {@link Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements
   * @deprecated Prefer constructing {@link SqlToSubstrait} with a {@link ConverterProvider}
   *     configured for the desired casing (via {@link ConverterProvider#builder()}) and calling
   *     {@link #convert(String, Prepare.CatalogReader)}. For fully custom parser behaviour,
   *     subclass {@link ConverterProvider} and override {@link
   *     ConverterProvider#getSqlParserConfig()}.
   */
  @Deprecated
  public Plan convert(
      final String sqlStatements,
      final Prepare.CatalogReader catalogReader,
      final SqlDialect sqlDialect)
      throws SqlParseException {
    return convert(sqlStatements, catalogReader);
  }
}
