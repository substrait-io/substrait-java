package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.isthmus.sql.SubstraitSqlValidator;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public SqlToSubstrait() {
    this(null);
  }

  public SqlToSubstrait(FeatureBoard features) {
    super(features);
  }

  /**
   * Converts a SQL statements string into a Substrait proto {@link io.substrait.proto.Plan}.
   *
   * @param sql the SQL statements string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements string
   * @return the Substrait proto {@link io.substrait.proto.Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements string
   * @deprecated use {@link #convert(String, org.apache.calcite.prepare.Prepare.CatalogReader)}
   *     instead to get a {@link Plan} and convert that to a {@link io.substrait.proto.Plan} using
   *     {@link PlanProtoConverter#toProto(Plan)}
   */
  @Deprecated
  public io.substrait.proto.Plan execute(String sql, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    PlanProtoConverter planToProto = new PlanProtoConverter();

    return planToProto.toProto(convert(sql, catalogReader));
  }

  /**
   * Converts a SQL statements string into a Substrait {@link Plan}.
   *
   * @param sql the SQL statements string containing one more SQL statements
   * @param catalogReader the {@link Prepare.CatalogReader} for finding tables/views referenced in
   *     the SQL statements string
   * @return the Substrait {@link Plan}
   * @throws SqlParseException if there is an error while parsing the SQL statements string
   */
  public Plan convert(String sql, Prepare.CatalogReader catalogReader) throws SqlParseException {
    Builder builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
    sqlToRelNode(sql, catalogReader).stream()
        .map(root -> SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION, featureBoard))
        .forEach(root -> builder.addRoots(root));

    return builder.build();
  }

  @VisibleForTesting
  List<RelRoot> sqlToRelNode(String sql, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNodeList parsedList = parser.parseStmtList();
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    List<RelRoot> roots =
        parsedList.stream()
            .map(parsed -> getBestExpRelRoot(converter, parsed))
            .collect(java.util.stream.Collectors.toList());
    return roots;
  }

  protected SqlToRelConverter createSqlToRelConverter(
      SqlValidator validator, Prepare.CatalogReader catalogReader) {
    SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    return converter;
  }

  protected RelRoot getBestExpRelRoot(SqlToRelConverter converter, SqlNode parsed) {
    RelRoot root = converter.convertQuery(parsed, true, true);
    {
      // RelBuilder seems to implicitly use the rule below,
      // need to add to avoid discrepancies in assertFullRoundTrip
      HepProgram program = HepProgram.builder().addRuleInstance(CoreRules.PROJECT_REMOVE).build();
      HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(root.rel);
      root = root.withRel(hepPlanner.findBestExp());
    }
    return root;
  }
}
