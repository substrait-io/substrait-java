package io.substrait.isthmus;

import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
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
    ExtensionCollector functionCollector = new ExtensionCollector();
    var relProtoConverter = new RelProtoConverter(functionCollector);

    List<RelRoot> relRoots = SubstraitSqlToCalcite.convertSelects(sql, catalogReader);

    // TODO: consider case in which one sql passes conversion while others don't
    Plan.Builder plan = Plan.newBuilder();
    relRoots.forEach(
        root -> {
          plan.addRelations(
              PlanRel.newBuilder()
                  .setRoot(
                      relProtoConverter.toProto(
                          SubstraitRelVisitor.convert(root, EXTENSION_COLLECTION, featureBoard))));
        });
    functionCollector.addExtensionsToPlan(plan);
    return plan.build();
  }
}
