package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.isthmus.expression.DdlRelBuilder;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.Plan.Version;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Plan;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schema;
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

  List<io.substrait.plan.Plan.Root> sqlToPlanNodes(String sql, List<String> tables)
      throws SqlParseException {
    Prepare.CatalogReader catalogReader = registerCreateTables(tables);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    return sqlToPlanNodes(sql, validator, catalogReader, io.substrait.plan.Plan.builder());
  }

  private Plan executeInner(String sql, SqlValidator validator, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    var builder = io.substrait.plan.Plan.builder();
    builder.version(Version.builder().from(Version.DEFAULT_VERSION).producer("isthmus").build());

    // TODO: consider case in which one sql passes conversion while others don't
    sqlToPlanNodes(sql, validator, catalogReader, builder);
    PlanProtoConverter planToProto = new PlanProtoConverter();

    return planToProto.toProto(builder.build());
  }

  private List<io.substrait.plan.Plan.Root> sqlToPlanNodes(
      String sql,
      SqlValidator validator,
      Prepare.CatalogReader catalogReader,
      ImmutablePlan.Builder builder)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    var parsedList = parser.parseStmtList();
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    // IMPORTANT: parsedList gets filtered in the call below
    List<io.substrait.plan.Plan.Root> ddlRelRoots = ddlSqlToRootNodes(parsedList, converter);

    List<io.substrait.plan.Plan.Root> sqlRelRoots = new LinkedList<>();

    for (RelRoot relRoot : sqlNodesToRelNode(parsedList, converter)) {
      io.substrait.plan.Plan.Root convert =
          SubstraitRelVisitor.convert(relRoot, EXTENSION_COLLECTION, featureBoard);
      sqlRelRoots.add(convert);
    }

    ddlRelRoots.addAll(sqlRelRoots);
    ddlRelRoots.forEach(builder::addRoots);
    return ddlRelRoots;
  }

  private List<io.substrait.plan.Plan.Root> ddlSqlToRootNodes(
      final SqlNodeList sqlNodeList, final SqlToRelConverter converter) throws SqlParseException {

    final DdlRelBuilder ddlRelBuilder =
        new DdlRelBuilder(
            converter, SqlToSubstrait::getBestExpRelRoot, EXTENSION_COLLECTION, featureBoard);

    List<SqlNode> toRemove = new LinkedList<>();
    List<io.substrait.plan.Plan.Root> retVal = new LinkedList<>();
    for (final SqlNode sqlNode : sqlNodeList) {
      final io.substrait.plan.Plan.Root root = sqlNode.accept(ddlRelBuilder);
      if (root != null) {
        retVal.add(root);
        toRemove.add(sqlNode);
      }
    }
    sqlNodeList.removeAll(toRemove);
    return retVal;
  }

  private List<RelRoot> sqlNodesToRelNode(
      final SqlNodeList parsedList, final SqlToRelConverter converter) {
    return parsedList.stream()
        .map(parsed -> getBestExpRelRoot(converter, parsed))
        .collect(java.util.stream.Collectors.toList());
  }

  private List<RelRoot> sqlToRelNode(
      String sql, SqlValidator validator, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    var parsedList = parser.parseStmtList();
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    return sqlNodesToRelNode(parsedList, converter);
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
