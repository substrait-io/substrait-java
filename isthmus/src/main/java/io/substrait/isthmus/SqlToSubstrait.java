package io.substrait.isthmus;

import com.github.bsideup.jabel.Desugar;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
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
import io.substrait.type.TypeCreator;
import io.substrait.type.proto.TypeProtoConverter;
import java.io.IOException;
import java.util.*;
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
    var result = registerCreateTables(tables);
    return executeInner(sql, factory, result.validator(), result.catalogReader());
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
    var result = registerCreateTables(tables);
    return executeInnerSQLExpression(
        sqlExpression,
        result.validator(),
        result.catalogReader(),
        result.nameToTypeMap(),
        result.nameToNodeMap());
  }

  // Package protected for testing
  List<RelRoot> sqlToRelNode(String sql, List<String> tables) throws SqlParseException {
    var result = registerCreateTables(tables);
    return sqlToRelNode(sql, result.validator(), result.catalogReader());
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
    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();
    RexNode rexNode =
        sqlToRexNode(sqlExpression, validator, catalogReader, nameToTypeMap, nameToNodeMap);
    ResulTraverseRowExpression result = new TraverseRexNode().getRowExpression(rexNode);
    io.substrait.proto.Type output =
        TypeCreator.NULLABLE.BOOLEAN.accept(new TypeProtoConverter(functionCollector));
    List<FunctionArgument> functionArgumentList = new ArrayList<>();
    result
        .expressionBuilderMap()
        .forEach(
            (k, v) -> {
              System.out.println("k->" + k);
              System.out.println("v->" + v);
              functionArgumentList.add(FunctionArgument.newBuilder().setValue(v).build());
            });

    ScalarFunction.Builder scalarFunctionBuilder =
        ScalarFunction.newBuilder()
            .setFunctionReference(1) // rel_01
            .setOutputType(output)
            .addAllArguments(functionArgumentList);

    Expression.Builder expressionBuilder =
        Expression.newBuilder().setScalarFunction(scalarFunctionBuilder);

    ExpressionReference.Builder expressionReferenceBuilder =
        ExpressionReference.newBuilder()
            .setExpression(expressionBuilder)
            .addOutputNames(result.ref().getName());

    extendedExpressionBuilder.addReferredExpr(0, expressionReferenceBuilder);

    io.substrait.expression.Expression.ScalarFunctionInvocation func =
        (io.substrait.expression.Expression.ScalarFunctionInvocation)
            rexNode.accept(rexExpressionConverter);
    String declaration = func.declaration().key(); // values example: gt:any_any, add:i64_i64

    // this is not mandatory to be defined; it is working without this definition. It is
    // only created here to create a proto message that has the correct semantics
    HashMap<String, SimpleExtensionURI> extensionUris = new HashMap<>();
    SimpleExtensionURI simpleExtensionURI;
    try {
      simpleExtensionURI =
          SimpleExtensionURI.newBuilder()
              .setExtensionUriAnchor(1) // rel_02
              .setUri(
                  SimpleExtension.loadDefaults().scalarFunctions().stream()
                      .filter(s -> s.toString().equalsIgnoreCase(declaration))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  String.format("Failed to get URI resource for %s.", declaration)))
                      .uri())
              .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    extensionUris.put("uri", simpleExtensionURI);

    ArrayList<SimpleExtensionDeclaration> extensions = new ArrayList<>();
    SimpleExtensionDeclaration extensionFunctionLowerThan =
        SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setFunctionAnchor(scalarFunctionBuilder.getFunctionReference()) // rel_01
                    .setName(declaration)
                    .setExtensionUriReference(simpleExtensionURI.getExtensionUriAnchor())) // rel_02
            .build();
    extensions.add(extensionFunctionLowerThan);

    System.out.println(
        "extendedExpressionBuilder.getExtensionUrisList(): "
            + extendedExpressionBuilder.getExtensionUrisList());
    // adding it for semantic purposes, it is not mandatory or needed
    extendedExpressionBuilder.addAllExtensionUris(extensionUris.values());
    extendedExpressionBuilder.addAllExtensions(extensions);

    NamedStruct namedStruct = TypeConverter.DEFAULT.toNamedStruct(nameToTypeMap);
    extendedExpressionBuilder.setBaseSchema(
        namedStruct.toProto(new TypeProtoConverter(functionCollector)));

    /*
        builder.addAllExtensionUris(uris.values());
    builder.addAllExtensions(extensionList);
     */

    return extendedExpressionBuilder.build();
  }

  private ExtendedExpression executeInnerSQLExpressionPojo(
      String sqlExpression,
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();
    RexNode rexNode =
        sqlToRexNode(sqlExpression, validator, catalogReader, nameToTypeMap, nameToNodeMap);
    ResulTraverseRowExpression result = new TraverseRexNode().getRowExpression(rexNode);
    io.substrait.proto.Type output =
        TypeCreator.NULLABLE.BOOLEAN.accept(new TypeProtoConverter(functionCollector));
    List<FunctionArgument> functionArgumentList = new ArrayList<>();
    result
        .expressionBuilderMap()
        .forEach(
            (k, v) -> {
              System.out.println("k->" + k);
              System.out.println("v->" + v);
              functionArgumentList.add(FunctionArgument.newBuilder().setValue(v).build());
            });

    ScalarFunction.Builder scalarFunctionBuilder =
        ScalarFunction.newBuilder()
            .setFunctionReference(1) // rel_01
            .setOutputType(output)
            .addAllArguments(functionArgumentList);

    Expression.Builder expressionBuilder =
        Expression.newBuilder().setScalarFunction(scalarFunctionBuilder);

    ExpressionReference.Builder expressionReferenceBuilder =
        ExpressionReference.newBuilder()
            .setExpression(expressionBuilder)
            .addOutputNames(result.ref().getName());

    extendedExpressionBuilder.addReferredExpr(0, expressionReferenceBuilder);

    io.substrait.expression.Expression.ScalarFunctionInvocation func =
        (io.substrait.expression.Expression.ScalarFunctionInvocation)
            rexNode.accept(rexExpressionConverter);
    String declaration = func.declaration().key(); // values example: gt:any_any, add:i64_i64

    // this is not mandatory to be defined; it is working without this definition. It is
    // only created here to create a proto message that has the correct semantics
    HashMap<String, SimpleExtensionURI> extensionUris = new HashMap<>();
    SimpleExtensionURI simpleExtensionURI;
    try {
      simpleExtensionURI =
          SimpleExtensionURI.newBuilder()
              .setExtensionUriAnchor(1) // rel_02
              .setUri(
                  SimpleExtension.loadDefaults().scalarFunctions().stream()
                      .filter(s -> s.toString().equalsIgnoreCase(declaration))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  String.format("Failed to get URI resource for %s.", declaration)))
                      .uri())
              .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    extensionUris.put("uri", simpleExtensionURI);

    ArrayList<SimpleExtensionDeclaration> extensions = new ArrayList<>();
    SimpleExtensionDeclaration extensionFunctionLowerThan =
        SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setFunctionAnchor(scalarFunctionBuilder.getFunctionReference()) // rel_01
                    .setName(declaration)
                    .setExtensionUriReference(simpleExtensionURI.getExtensionUriAnchor())) // rel_02
            .build();
    extensions.add(extensionFunctionLowerThan);

    System.out.println(
        "extendedExpressionBuilder.getExtensionUrisList(): "
            + extendedExpressionBuilder.getExtensionUrisList());
    // adding it for semantic purposes, it is not mandatory or needed
    extendedExpressionBuilder.addAllExtensionUris(extensionUris.values());
    extendedExpressionBuilder.addAllExtensions(extensions);

    NamedStruct namedStruct = TypeConverter.DEFAULT.toNamedStruct(nameToTypeMap);
    extendedExpressionBuilder.setBaseSchema(
        namedStruct.toProto(new TypeProtoConverter(functionCollector)));

    /*
        builder.addAllExtensionUris(uris.values());
    builder.addAllExtensions(extensionList);
     */

    return extendedExpressionBuilder.build();
  }

  class TraverseRexNode {
    RexInputRef ref = null;
    int control = 0;
    Expression.Builder referenceBuilder = null;
    Expression.Builder literalBuilder = null;
    Map<Integer, Expression.Builder> expressionBuilderMap = new LinkedHashMap<>();

    ResulTraverseRowExpression getRowExpression(RexNode rexNode) {
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
          expressionBuilderMap.put(control, referenceBuilder);
          control++;
          break;
        case "REXLITERAL":
          RexLiteral literal = (RexLiteral) rexNode;
          literalBuilder =
              Expression.newBuilder()
                  .setLiteral(
                      Expression.Literal.newBuilder().setI32(literal.getValueAs(Integer.class)));
          expressionBuilderMap.put(control, literalBuilder);
          control++;
          break;
        default:
          throw new AssertionError(
              "Unsupported type for: " + rexNode.getClass().getSimpleName().toUpperCase());
      }
      return new ResulTraverseRowExpression(
          ref, referenceBuilder, literalBuilder, expressionBuilderMap);
    }
  }

  @Desugar
  private record ResulTraverseRowExpression(
      RexInputRef ref,
      Expression.Builder referenceBuilder,
      Expression.Builder literalBuilder,
      Map<Integer, Expression.Builder> expressionBuilderMap) {}

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
