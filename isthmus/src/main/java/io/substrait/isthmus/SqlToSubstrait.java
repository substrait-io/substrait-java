package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Version;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Take a SQL statement and a set of table definitions and return a substrait plan.
 *
 * <p>Conversion behaviours can be customized using a {@link ConverterProvider}
 */
public class SqlToSubstrait extends SqlConverterBase {
  private final SqlOperatorTable operatorTable;
  protected final ConverterProvider converterProvider;

  public SqlToSubstrait() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /** Use {@link SqlToSubstrait#SqlToSubstrait(ConverterProvider)} instead */
  @Deprecated
  public SqlToSubstrait(SimpleExtension.ExtensionCollection extensions) {
    this(new ConverterProvider(extensions));
  }

  public SqlToSubstrait(ConverterProvider converterProvider) {
    super(converterProvider);
    this.operatorTable = converterProvider.getSqlOperatorTable();
    this.converterProvider = converterProvider;
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

    // TODO: consider case in which one sql passes conversion while others don't
    SubstraitSqlToCalcite.convertQueries(sqlStatements, catalogReader, operatorTable).stream()
        .map(root -> SubstraitRelVisitor.convert(root, converterProvider))
        .forEach(root -> builder.addRoots(root));

    return builder.build();
  }

  /**
   * Converts one or more SQL statements into a Substrait {@link Plan}.
   *
   * @param sqlStatements a string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @param sqlDialect The sql dialect to use for parsing.
   * @return the Substrait {@link Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements
   */
  public Plan convert(
      final String sqlStatements,
      final Prepare.CatalogReader catalogReader,
      final SqlDialect sqlDialect)
      throws SqlParseException {
    Builder builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    final SqlParser.Config sqlParserConfig = sqlDialect.configureParser(SqlParser.config());

    // TODO: consider case in which one sql passes conversion while others don't
    SubstraitSqlToCalcite.convertQueries(sqlStatements, catalogReader, sqlParserConfig).stream()
        .map(root -> SubstraitRelVisitor.convert(root, converterProvider))
        .forEach(root -> builder.addRoots(root));

    return builder.build();
  }
}
