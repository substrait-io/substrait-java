package io.substrait.isthmus;

import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExtendedExpression.Builder;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlValidator;
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
      final FeatureBoard features, final SimpleExtension.ExtensionCollection extensions) {
    super(features);
    final ScalarFunctionConverter scalarFunctionConverter =
        new ScalarFunctionConverter(extensions.scalarFunctions(), factory);
    this.rexConverter = new RexExpressionConverter(scalarFunctionConverter);
  }

  private static final class Result {
    final SqlValidator validator;
    final CalciteCatalogReader catalogReader;
    final Map<String, RelDataType> nameToTypeMap;
    final Map<String, RexNode> nameToNodeMap;

    Result(
        final SqlValidator validator,
        final CalciteCatalogReader catalogReader,
        final Map<String, RelDataType> nameToTypeMap,
        final Map<String, RexNode> nameToNodeMap) {
      this.validator = validator;
      this.catalogReader = catalogReader;
      this.nameToTypeMap = nameToTypeMap;
      this.nameToNodeMap = nameToNodeMap;
    }
  }

  /**
   * Converts the given SQL expression to an {@link io.substrait.proto.ExtendedExpression }
   *
   * @param sqlExpression a SQL expression
   * @param createStatements table creation statements defining fields referenced by the expression
   * @return a {@link io.substrait.proto.ExtendedExpression }
   * @throws SqlParseException
   */
  public io.substrait.proto.ExtendedExpression convert(
      final String sqlExpression, final List<String> createStatements) throws SqlParseException {
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
      final String[] sqlExpressions, final List<String> createStatements) throws SqlParseException {
    final Result result = registerCreateTablesForExtendedExpression(createStatements);
    return executeInnerSQLExpressions(
        sqlExpressions,
        result.validator,
        result.catalogReader,
        result.nameToTypeMap,
        result.nameToNodeMap);
  }

  private io.substrait.proto.ExtendedExpression executeInnerSQLExpressions(
      final String[] sqlExpressions,
      final SqlValidator validator,
      final CalciteCatalogReader catalogReader,
      final Map<String, RelDataType> nameToTypeMap,
      final Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    int columnIndex = 1;
    final List<ExtendedExpression.ExpressionReference> expressionReferences = new ArrayList<>();
    RexNode rexNode;
    for (final String sqlExpression : sqlExpressions) {
      rexNode =
          sqlToRexNode(
              sqlExpression.trim(), validator, catalogReader, nameToTypeMap, nameToNodeMap);
      final ExtendedExpression.ExpressionReference expressionReference =
          ExtendedExpression.ExpressionReference.builder()
              .expression(rexNode.accept(this.rexConverter))
              .addOutputNames("column-" + columnIndex++)
              .build();
      expressionReferences.add(expressionReference);
    }
    final NamedStruct namedStruct = toNamedStruct(nameToTypeMap);
    final Builder extendedExpression =
        ExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct);

    return new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());
  }

  private RexNode sqlToRexNode(
      final String sql,
      final SqlValidator validator,
      final CalciteCatalogReader catalogReader,
      final Map<String, RelDataType> nameToTypeMap,
      final Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseExpression();
    final SqlNode validSqlNode = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    final SqlToRelConverter converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
    return converter.convertExpression(validSqlNode, nameToNodeMap);
  }

  private Result registerCreateTablesForExtendedExpression(final List<String> tables)
      throws SqlParseException {
    final Map<String, RelDataType> nameToTypeMap = new LinkedHashMap<>();
    final Map<String, RexNode> nameToNodeMap = new HashMap<>();
    final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    if (tables != null) {
      for (final String tableDef : tables) {
        final List<SubstraitTable> tList =
            SubstraitCreateStatementParser.processCreateStatements(tableDef);
        for (final SubstraitTable t : tList) {
          rootSchema.add(t.getName(), t);
          for (final RelDataTypeField field : t.getRowType(factory).getFieldList()) {
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
    final SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return new Result(validator, catalogReader, nameToTypeMap, nameToNodeMap);
  }

  private NamedStruct toNamedStruct(final Map<String, RelDataType> nameToTypeMap) {
    final ArrayList<String> names = new ArrayList<String>();
    final ArrayList<Type> types = new ArrayList<Type>();
    for (final Map.Entry<String, RelDataType> entry : nameToTypeMap.entrySet()) {
      final String k = entry.getKey();
      final RelDataType v = entry.getValue();
      names.add(k);
      types.add(TypeConverter.DEFAULT.toSubstrait(v));
    }
    return NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
  }
}
