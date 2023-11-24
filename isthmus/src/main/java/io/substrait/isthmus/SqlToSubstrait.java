package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.extended.expression.ExtendedExpressionProtoConverter;
import io.substrait.extended.expression.ImmutableExpressionReference;
import io.substrait.extended.expression.ImmutableExtendedExpression;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import java.util.*;
import java.util.function.Function;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** Take a SQL statement and a set of table definitions and return a substrait plan. */
public class SqlToSubstrait extends SqlConverterBase {

  private final ScalarFunctionConverter functionConverter =
      new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), factory);

  private final RexExpressionConverter rexExpressionConverter =
      new RexExpressionConverter(functionConverter);

  public SqlToSubstrait() {
    this(null);
  }

  public SqlToSubstrait(FeatureBoard features) {
    super(features);
  }

  public Plan execute(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    var pair = registerCreateTables(tableLookup);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  public Plan execute(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  public Plan execute(String sql, String name, Schema schema) throws SqlParseException {
    var pair = registerSchema(name, schema);
    return executeInner(sql, factory, pair.left, pair.right);
  }

  /**
   * Process to execute an SQL Expression to convert into an Extended expression protobuf message
   *
   * @param sqlExpression expression defined by the user
   * @param tables of names of table needed to consider to load into memory for catalog, schema,
   *     validate and parse sql
   * @return extended expression protobuf message
   * @throws SqlParseException
   */
  public ExtendedExpression executeSQLExpression(String sqlExpression, List<String> tables)
      throws SqlParseException {
    var result = registerCreateTablesForExtendedExpression(tables);
    return executeInnerSQLExpression(
        sqlExpression,
        result.validator(),
        result.catalogReader(),
        result.nameToTypeMap(),
        result.nameToNodeMap());
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    var pair = registerCreateTables(tables);
    return sqlToRelNode(sql, pair.left, pair.right);
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    var pair = registerCreateTables(tableLookup);
    return sqlToRelNode(sql, pair.left, pair.right);
  }

  private Plan executeInner(
      String sql,
      RelDataTypeFactory factory,
      SqlValidator validator,
      CalciteCatalogReader catalogReader)
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
                          io.substrait.proto.RelRoot.newBuilder()
                              .setInput(
                                  SubstraitRelVisitor.convert(
                                          root, EXTENSION_COLLECTION, featureBoard)
                                      .accept(relProtoConverter))
                              .addAllNames(
                                  TypeConverter.DEFAULT
                                      .toNamedStruct(root.validatedRowType)
                                      .names())));
            });
    functionCollector.addExtensionsToPlan(plan);
    return plan.build();
  }

  private ExtendedExpression executeInnerSQLExpression(
      String sqlExpression,
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    RexNode rexNode =
        sqlToRexNode(sqlExpression, validator, catalogReader, nameToTypeMap, nameToNodeMap);
    io.substrait.expression.Expression.ScalarFunctionInvocation func =
        (io.substrait.expression.Expression.ScalarFunctionInvocation)
            rexNode.accept(rexExpressionConverter);
    NamedStruct namedStruct = TypeConverter.DEFAULT.toNamedStruct(nameToTypeMap);
    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder().referredExpr(func).addOutputNames("output").build();

    List<io.substrait.extended.expression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableExtendedExpression.Builder extendedExpression =
        ImmutableExtendedExpression.builder()
            .referredExpr(expressionReferences)
            .baseSchema(namedStruct);

    return new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());
  }

  private List<RelRoot> sqlToRelNode(
      String sql, SqlValidator validator, CalciteCatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    var parsedList = parser.parseStmtList();
    if (!featureBoard.allowsSqlBatch() && parsedList.size() > 1) {
      throw new UnsupportedOperationException("SQL must contain only a single statement: " + sql);
    }
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    List<RelRoot> roots =
        parsedList.stream()
            .map(parsed -> getBestExpRelRoot(converter, parsed))
            .collect(java.util.stream.Collectors.toList());
    return roots;
  }

  private RexNode sqlToRexNode(
      String sql,
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode sqlNode = parser.parseExpression();
    SqlNode validSQLNode = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    return converter.convertExpression(validSQLNode, nameToNodeMap);
  }

  @VisibleForTesting
  SqlToRelConverter createSqlToRelConverter(
      SqlValidator validator, CalciteCatalogReader catalogReader) {
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
