package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import io.substrait.isthmus.sql.SubstraitSqlValidator;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {
  private final SqlOperatorTable operatorTable;

  public SqlToSubstrait() {
    this(SimpleExtension.loadDefaults(), null);
  }

  public SqlToSubstrait(FeatureBoard features) {
    this(SimpleExtension.loadDefaults(), features);
  }

  public SqlToSubstrait(SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    super(features, extensions);

    SimpleExtension.ExtensionCollection dynamicExtensionCollection =
        ExtensionUtils.getDynamicExtensions(extensions);
    List<SqlOperator> generatedDynamicOperators =
        SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, this.factory);

    this.operatorTable =
        SqlOperatorTables.chain(
            SubstraitOperatorTable.INSTANCE, SqlOperatorTables.of(generatedDynamicOperators));
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
    return planToProto.toProto(convert(sqlStatements, catalogReader));
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
  public Plan convert(String sqlStatements, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    Builder builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
      SubstraitSqlToCalcite.convertQueries(sqlStatements, catalogReader).stream()
              .map(root -> SubstraitRelVisitor.convert(root, extensionCollection, featureBoard))
              .forEach(root -> builder.addRoots(root));

    return builder.build();
  }
}
