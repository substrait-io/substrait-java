package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Plan;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.parser.SqlParseException;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  public SqlToSubstrait() {
    this(null);
  }

  public SqlToSubstrait(FeatureBoard features) {
    super(features);
  }

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    CalciteCatalogReader catalogReader = registerCreateTables(tables);
    return executeInner(sql, catalogReader);
  }

  public Plan execute(String sql, String name, Schema schema) throws SqlParseException {
    CalciteCatalogReader catalogReader = registerSchema(name, schema);
    return executeInner(sql, catalogReader);
  }

  public Plan execute(String sql, Prepare.CatalogReader catalogReader) throws SqlParseException {
    return executeInner(sql, catalogReader);
  }

  private Plan executeInner(String sql, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    var builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
    SubstraitSqlToCalcite.convertSelects(sql, catalogReader).stream()
        .map(root -> SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION, featureBoard))
        .forEach(root -> builder.addRoots(root));

    PlanProtoConverter planToProto = new PlanProtoConverter();
    return planToProto.toProto(builder.build());
  }
}
