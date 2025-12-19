package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {
  private final SqlOperatorTable operatorTable;

  /**
   * Creates a SQL-to-Substrait converter using the default extension catalog and no feature
   * overrides.
   */
  public SqlToSubstrait() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, null);
  }

  /**
   * Creates a SQL-to-Substrait converter using the default extension catalog and provided features.
   *
   * @param features Feature flags controlling conversion behavior; may be {@code null} for
   *     defaults.
   */
  public SqlToSubstrait(FeatureBoard features) {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, features);
  }

  /**
   * Creates a SQL-to-Substrait converter with explicit extensions and features.
   *
   * @param extensions Substrait extension collection for function/operator mappings.
   * @param features Feature flags controlling conversion behavior; may be {@code null} for
   *     defaults.
   */
  public SqlToSubstrait(SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    super(features, extensions);

    if (featureBoard.allowDynamicUdfs()) {
      SimpleExtension.ExtensionCollection dynamicExtensionCollection =
          ExtensionUtils.getDynamicExtensions(extensions);
      if (!dynamicExtensionCollection.scalarFunctions().isEmpty()
          || !dynamicExtensionCollection.aggregateFunctions().isEmpty()) {
        List<SqlOperator> generatedDynamicOperators =
            SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, this.factory);
        this.operatorTable =
            SqlOperatorTables.chain(
                SubstraitOperatorTable.INSTANCE, SqlOperatorTables.of(generatedDynamicOperators));
        return;
      }
    }
    this.operatorTable = SubstraitOperatorTable.INSTANCE;
  }

  /**
   * Converts one or more SQL statements into a Substrait {@link io.substrait.proto.Plan}.
   *
   * @param sqlStatements a string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements
   * @return a Substrait proto {@link io.substrait.proto.Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements string
   * @deprecated use {@link #convert(String, org.apache.calcite.prepare.Prepare.CatalogReader)}
   *     instead to get a {@link Plan} and convert that to a {@link io.substrait.proto.Plan} using
   *     {@link PlanProtoConverter#toProto(Plan)}
   */
  @Deprecated
  public io.substrait.proto.Plan execute(String sqlStatements, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    PlanProtoConverter planToProto = new PlanProtoConverter();
    return planToProto.toProto(
        convert(sqlStatements, catalogReader, SqlDialect.DatabaseProduct.CALCITE.getDialect()));
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
        .map(root -> SubstraitRelVisitor.convert(root, extensionCollection, featureBoard))
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
        .map(root -> SubstraitRelVisitor.convert(root, extensionCollection, featureBoard))
        .forEach(root -> builder.addRoots(root));

    return builder.build();
  }
}
