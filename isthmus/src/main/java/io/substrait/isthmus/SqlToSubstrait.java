package io.substrait.isthmus;

import com.github.bsideup.jabel.Desugar;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.ScalarFunction;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.type.proto.TypeProtoConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
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

  public ExtendedExpression executeExpression(String expr, List<String> tables)
      throws SqlParseException {
    var pair = registerCreateTables(tables);
    return executeInnerExpression(expr, pair.left, pair.right);
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

  private ExtendedExpression executeInnerExpression(
      String sql, SqlValidator validator, CalciteCatalogReader catalogReader)
      throws SqlParseException {
    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();
    sqlToRexNode(sql, validator, catalogReader)
        .forEach(
            rexNode -> {
              // FIXME! Implement it dynamically for more expression types
              ResulTraverseRowExpression result = TraverseRexNode.getRowExpression(rexNode);

              // FIXME! Get output type dynamically:
              // final static Map<String, Type> getTypeCreator = new HashMap<>(){{put("BOOLEAN",
              // TypeCreator.of(true).BOOLEAN);}};
              // getTypeCreator.get(rexNode.getType()).accept(...)
              io.substrait.proto.Type output =
                  TypeCreator.NULLABLE.BOOLEAN.accept(new TypeProtoConverter(functionCollector));

              // FIXME! setFunctionReference, addArguments(index: 0, 1)
              Expression.Builder expressionBuilder =
                  Expression.newBuilder()
                      .setScalarFunction(
                          ScalarFunction.newBuilder()
                              .setFunctionReference(1)
                              .setOutputType(output)
                              .addArguments(
                                  0,
                                  FunctionArgument.newBuilder().setValue(result.referenceBuilder()))
                              .addArguments(
                                  1,
                                  FunctionArgument.newBuilder()
                                      .setValue(result.expressionBuilderLiteral())));
              ExpressionReference.Builder expressionReferenceBuilder =
                  ExpressionReference.newBuilder()
                      .setExpression(expressionBuilder)
                      .addOutputNames(result.ref().getName());

              // FIXME! Get schema dynamically
              // (as the same for Plan with:
              // TypeConverter.DEFAULT.toNamedStruct(rexNode.getType());)
              List<String> columnNames =
                  Arrays.asList("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT");
              List<Type> dataTypes =
                  Arrays.asList(
                      TypeCreator.NULLABLE.I32,
                      TypeCreator.NULLABLE.STRING,
                      TypeCreator.NULLABLE.I32,
                      TypeCreator.NULLABLE.STRING);
              NamedStruct namedStruct =
                  NamedStruct.of(
                      columnNames, Type.Struct.builder().fields(dataTypes).nullable(false).build());

              extendedExpressionBuilder
                  .addReferredExpr(0, expressionReferenceBuilder)
                  .setBaseSchema(namedStruct.toProto(new TypeProtoConverter(functionCollector)));

              // Extensions URI FIXME! Populate/create this dynamically
              HashMap<String, SimpleExtensionURI> extensionUris = new HashMap<>();
              extensionUris.put(
                  "key-001",
                  SimpleExtensionURI.newBuilder()
                      .setExtensionUriAnchor(1)
                      .setUri("/functions_comparison.yaml")
                      .build());

              // Extensions FIXME! Populate/create this dynamically, maybe use rexNode.getKind()
              ArrayList<SimpleExtensionDeclaration> extensions = new ArrayList<>();
              SimpleExtensionDeclaration extensionFunctionLowerThan =
                  SimpleExtensionDeclaration.newBuilder()
                      .setExtensionFunction(
                          SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                              .setFunctionAnchor(1)
                              .setName("gt:any_any")
                              .setExtensionUriReference(1))
                      .build();
              extensions.add(extensionFunctionLowerThan);

              extendedExpressionBuilder.addAllExtensionUris(extensionUris.values());
              extendedExpressionBuilder.addAllExtensions(extensions);
            });
    return extendedExpressionBuilder.build();
  }

  static class TraverseRexNode {
    static RexInputRef ref = null;
    static Expression.Builder referenceBuilder = null;
    static Expression.Builder expressionBuilderLiteral = null;

    static ResulTraverseRowExpression getRowExpression(RexNode rexNode) {

      switch (rexNode.getClass().getSimpleName().toUpperCase()) {
        case "REXCALL":
          for (RexNode rexInternal : ((RexCall) rexNode).operands) {
            getRowExpression(rexInternal);
          }
          ;
          break;
        case "REXINPUTREF":
          ref = (RexInputRef) rexNode;
          referenceBuilder =
              Expression.newBuilder()
                  .setSelection(
                      Expression.FieldReference.newBuilder()
                          .setDirectReference(
                              Expression.ReferenceSegment.newBuilder()
                                  .setStructField(
                                      Expression.ReferenceSegment.StructField.newBuilder()
                                          .setField(ref.getIndex()))));
          break;
        case "REXLITERAL":
          RexLiteral literal = (RexLiteral) rexNode;
          expressionBuilderLiteral =
              Expression.newBuilder()
                  .setLiteral(
                      Expression.Literal.newBuilder().setI32(literal.getValueAs(Integer.class)));
          break;
        default:
          throw new AssertionError(
              "Unsupported type for: " + rexNode.getClass().getSimpleName().toUpperCase());
      }
      ResulTraverseRowExpression result =
          new ResulTraverseRowExpression(ref, referenceBuilder, expressionBuilderLiteral);
      return result;
    }
  }

  @Desugar
  private record ResulTraverseRowExpression(
      RexInputRef ref,
      Expression.Builder referenceBuilder,
      Expression.Builder expressionBuilderLiteral) {}

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

  private List<RexNode> sqlToRexNode(
      String sql, SqlValidator validator, CalciteCatalogReader catalogReader)
      throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode sqlNode = parser.parseExpression();
    Result result = getResult(validator);
    SqlNode validSQLNode =
        validator.validateParameterizedExpression(
            sqlNode,
            result.nameToTypeMap()); // FIXME! It may be optional to include this validation
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    RexNode rexNode = converter.convertExpression(validSQLNode, result.nameToNodeMap());

    return Collections.singletonList(rexNode);
  }

  private static Result getResult(SqlValidator validator) {
    // FIXME! Needs to be created dinamycally, this is for PoC purpose
    HashMap<String, RexNode> nameToNodeMap = new HashMap<>();
    nameToNodeMap.put(
        "N_NATIONKEY",
        new RexInputRef(0, validator.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));
    nameToNodeMap.put(
        "N_REGIONKEY",
        new RexInputRef(1, validator.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));
    final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
    for (Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet()) {
      nameToTypeMap.put(entry.getKey(), entry.getValue().getType());
    }
    Result result = new Result(nameToNodeMap, nameToTypeMap);
    return result;
  }

  private @Desugar record Result(
      HashMap<String, RexNode> nameToNodeMap, Map<String, RelDataType> nameToTypeMap) {}

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
