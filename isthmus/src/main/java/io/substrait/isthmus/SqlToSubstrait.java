package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
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

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    CalciteCatalogReader catalogReader = registerCreateTables(tables);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    return executeInner(sql, validator, catalogReader);
  }

  public Plan execute(String sql, String name, Schema schema) throws SqlParseException {
    CalciteCatalogReader catalogReader = registerSchema(name, schema);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    return executeInner(sql, validator, catalogReader);
  }

  public Plan execute(String sql, Prepare.CatalogReader catalogReader) throws SqlParseException {
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    return executeInner(sql, validator, catalogReader);
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    Prepare.CatalogReader catalogReader = registerCreateTables(tables);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    return sqlToRelNode(sql, validator, catalogReader);
  }

  private Plan executeInner(String sql, SqlValidator validator, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    var plan = Plan.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();
    var relProtoConverter = new RelProtoConverter(functionCollector);
    // TODO: consider case in which one sql passes conversion while others don't
    sqlToRelNode(sql, validator, catalogReader)
        .forEach(
            root -> {
              plan.addRelations(
                  PlanRel.newBuilder()
                      .setRoot(
                          relProtoConverter.toProto(
                              SubstraitRelVisitor.convert(
                                  root, EXTENSION_COLLECTION, featureBoard))));
            });
    functionCollector.addExtensionsToPlan(plan);
    return plan.build();
  }

  private List<RelRoot> sqlToRelNode(
      String sql, SqlValidator validator, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    var parsedList = parser.parseStmtList();
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    List<RelRoot> roots =
        parsedList.stream()
            .map(parsed -> getBestExpRelRoot(converter, parsed))
            .collect(java.util.stream.Collectors.toList());
    return roots;
  }

  @VisibleForTesting
  SqlToRelConverter createSqlToRelConverter(
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

  @VisibleForTesting
  static RelRoot getBestExpRelRoot(SqlToRelConverter converter, SqlNode parsed) {
    RelRoot root = converter.convertQuery(parsed, true, true);
    {
      var program = HepProgram.builder().build();
      HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(root.rel);
      root = root.withRel(hepPlanner.findBestExp());
    }
    return root;
  }
}
