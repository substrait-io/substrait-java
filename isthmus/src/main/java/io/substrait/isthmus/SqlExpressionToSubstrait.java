package io.substrait.isthmus;

import com.github.bsideup.jabel.Desugar;
import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class SqlExpressionToSubstrait extends SqlConverterBase {

  protected final RexExpressionConverter rexConverter;

  public SqlExpressionToSubstrait() {
    this(FEATURES_DEFAULT, EXTENSION_COLLECTION);
  }

  public SqlExpressionToSubstrait(
      FeatureBoard features, SimpleExtension.ExtensionCollection extensions) {
    super(features);
    ScalarFunctionConverter scalarFunctionConverter =
        new ScalarFunctionConverter(extensions.scalarFunctions(), factory);
    this.rexConverter = new RexExpressionConverter(scalarFunctionConverter);
  }

  @Desugar
  private record Result(
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap) {}

  /**
   * Converts the given SQL expression to an {@link io.substrait.proto.ExtendedExpression }
   *
   * @param sqlExpression a SQL expression
   * @param createStatements table creation statements defining fields referenced by the expression
   * @return a {@link io.substrait.proto.ExtendedExpression }
   * @throws SqlParseException
   */
  public io.substrait.proto.ExtendedExpression convert(
      String sqlExpression, List<String> createStatements) throws SqlParseException {
    return convert(new String[] {sqlExpression}, createStatements);
  }

  /**
   * Converts the given SQL expressions to an {@link io.substrait.proto.ExtendedExpression }
   *
   * @param sqlExpressions an array of SQL expressions
   * @param createStatements table creation statements defining fields referenced by the expression
   * @return a {@link io.substrait.proto.ExtendedExpression }
   * @throws SqlParseException
   */
  public io.substrait.proto.ExtendedExpression convert(
      String[] sqlExpressions, List<String> createStatements) throws SqlParseException {
    var result = registerCreateTablesForExtendedExpression(createStatements);
    return executeInnerSQLExpressions(
        sqlExpressions,
        result.validator(),
        result.catalogReader(),
        result.nameToTypeMap(),
        result.nameToNodeMap());
  }

  private io.substrait.proto.ExtendedExpression executeInnerSQLExpressions(
      String[] sqlExpressions,
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    int columnIndex = 1;
    List<ExtendedExpression.ExpressionReference> expressionReferences = new ArrayList<>();
    RexNode rexNode;
    for (String sqlExpression : sqlExpressions) {
      rexNode =
          sqlToRexNode(
              sqlExpression.trim(), validator, catalogReader, nameToTypeMap, nameToNodeMap);
      ExtendedExpression.ExpressionReference expressionReference =
          ExtendedExpression.ExpressionReference.builder()
              .expression(rexNode.accept(this.rexConverter))
              .addOutputNames("column-" + columnIndex++)
              .build();
      expressionReferences.add(expressionReference);
    }
    NamedStruct namedStruct = toNamedStruct(nameToTypeMap);
    var extendedExpression =
        ExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct);

    return new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());
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
    SqlNode validSqlNode = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    return converter.convertExpression(validSqlNode, nameToNodeMap);
  }

  private Result registerCreateTablesForExtendedExpression(List<String> tables)
      throws SqlParseException {
    Map<String, RelDataType> nameToTypeMap = new LinkedHashMap<>();
    Map<String, RexNode> nameToNodeMap = new HashMap<>();
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    SqlValidator validator = Validator.create(factory, catalogReader, SqlValidator.Config.DEFAULT);
    if (tables != null) {
      for (String tableDef : tables) {
        List<SubstraitTable> tList = parseCreateTable(factory, validator, tableDef);
        for (SubstraitTable t : tList) {
          rootSchema.add(t.getName(), t);
          for (RelDataTypeField field : t.getRowType(factory).getFieldList()) {
            nameToTypeMap.merge( // to validate the sql expression tree
                field.getName(),
                field.getType(),
                (v1, v2) -> {
                  throw new IllegalArgumentException(
                      "There is no support for duplicate column names: " + field.getName());
                });
            nameToNodeMap.merge( // to convert sql expression into RexNode
                field.getName(),
                new RexInputRef(field.getIndex(), field.getType()),
                (v1, v2) -> {
                  throw new IllegalArgumentException(
                      "There is no support for duplicate column names: " + field.getName());
                });
          }
        }
      }
    }
    return new Result(validator, catalogReader, nameToTypeMap, nameToNodeMap);
  }

  private NamedStruct toNamedStruct(Map<String, RelDataType> nameToTypeMap) {
    var names = new ArrayList<String>();
    var types = new ArrayList<Type>();
    for (Map.Entry<String, RelDataType> entry : nameToTypeMap.entrySet()) {
      String k = entry.getKey();
      RelDataType v = entry.getValue();
      names.add(k);
      types.add(TypeConverter.DEFAULT.toSubstrait(v));
    }
    return NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
  }
}
