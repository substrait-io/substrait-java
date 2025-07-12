package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.ImmutablePlan.Builder;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Plan;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public SqlToSubstrait() {
    this(null);
  }

  public SqlToSubstrait(FeatureBoard features) {
    super(features);
  }

  public Plan execute(String sql, Prepare.CatalogReader catalogReader) throws SqlParseException {
    return executeInner(sql, catalogReader);
  }

  private Plan executeInner(String sql, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    Builder builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
    SubstraitSqlToCalcite.convertSelects(sql, catalogReader).stream()
        .map(root -> SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION, featureBoard))
        .forEach(root -> builder.addRoots(root));

    PlanProtoConverter planToProto = new PlanProtoConverter();
    return planToProto.toProto(builder.build());
  }

  //  @VisibleForTesting
  //  static RelRoot getBestExpRelRoot(SqlToRelConverter converter, SqlNode parsed) {
  //    RelRoot root = converter.convertQuery(parsed, true, true);
  //    {
  //      // RelBuilder seems to implicitly use the rule below,
  //      // need to add to avoid discrepancies in assertFullRoundTrip
  //      HepProgram program =
  // HepProgram.builder().addRuleInstance(CoreRules.PROJECT_REMOVE).build();
  //      HepPlanner hepPlanner = new HepPlanner(program);
  //      hepPlanner.setRoot(root.rel);
  //      root = root.withRel(hepPlanner.findBestExp());
  //    }
  //    return root;
  //  }
}
