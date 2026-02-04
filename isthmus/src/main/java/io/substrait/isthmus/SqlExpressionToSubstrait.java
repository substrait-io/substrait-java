package io.substrait.isthmus;

import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExtendedExpression.Builder;
import io.substrait.extension.DefaultExtensionCatalog;
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

/**
 * Converts SQL expressions to Substrait {@link io.substrait.proto.ExtendedExpression} payloads.
 *
 * <p>Supports optional CREATE TABLE statements to provide schema and column bindings for expression
 * validation and Rex conversion.
 */
public class SqlExpressionToSubstrait extends SqlConverterBase {

  /** Converter for RexNodes to Substrait expressions. */
  protected final RexExpressionConverter rexConverter;

  /** Creates a converter with default features and the default extension catalog. */
  public SqlExpressionToSubstrait() {
    this(FEATURES_DEFAULT, DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Creates a converter with the given feature board and extension collection.
   *
   * @param features feature flags for conversion
   * @param extensions extension functions used during expression conversion
   */
  public SqlExpressionToSubstrait(
      FeatureBoard features, SimpleExtension.ExtensionCollection extensions) {
    super(features, extensions);
    ScalarFunctionConverter scalarFunctionConverter =
        new ScalarFunctionConverter(extensions.scalarFunctions(), factory);
    this.rexConverter = new RexExpressionConverter(scalarFunctionConverter);
  }

  /** Bundled result carrying validator, catalog reader, and name/type and name/node maps. */
  private static final class Result {
    final SqlValidator validator;
    final CalciteCatalogReader catalogReader;
    final Map<String, RelDataType> nameToTypeMap;
    final Map<String, RexNode> nameToNodeMap;

    /**
     * Creates a result bundle.
     *
     * @param validator SQL validator
     * @param catalogReader Calcite catalog reader
     * @param nameToTypeMap mapping from column name to Calcite type
     * @param nameToNodeMap mapping from column name to Rex input ref
     */
    Result(
        SqlValidator validator,
        CalciteCatalogReader catalogReader,
        Map<String, RelDataType> nameToTypeMap,
        Map<String, RexNode> nameToNodeMap) {
      this.validator = validator;
      this.catalogReader = catalogReader;
      this.nameToTypeMap = nameToTypeMap;
      this.nameToNodeMap = nameToNodeMap;
    }
  }

  /**
   * Converts a single SQL expression to a Substrait {@link io.substrait.proto.ExtendedExpression}.
   *
   * @param sqlExpression a SQL expression
   * @param createStatements table creation statements defining fields referenced by the expression
   * @return the Substrait extended expression proto
   * @throws SqlParseException if parsing or validation fails
   */
  public io.substrait.proto.ExtendedExpression convert(
      String sqlExpression, List<String> createStatements) throws SqlParseException {
    return convert(new String[] {sqlExpression}, createStatements);
  }

  /**
   * Converts multiple SQL expressions to a Substrait {@link io.substrait.proto.ExtendedExpression}.
   *
   * @param sqlExpressions array of SQL expressions
   * @param createStatements table creation statements defining fields referenced by the expressions
   * @return the Substrait extended expression proto
   * @throws SqlParseException if parsing or validation fails
   */
  public io.substrait.proto.ExtendedExpression convert(
      String[] sqlExpressions, List<String> createStatements) throws SqlParseException {
    Result result = registerCreateTablesForExtendedExpression(createStatements);
    return executeInnerSQLExpressions(
        sqlExpressions,
        result.validator,
        result.catalogReader,
        result.nameToTypeMap,
        result.nameToNodeMap);
  }

  /**
   * Converts the given SQL expressions using the provided validator/catalog and column bindings.
   *
   * @param sqlExpressions array of SQL expressions
   * @param validator SQL validator
   * @param catalogReader Calcite catalog reader
   * @param nameToTypeMap mapping from column name to Calcite type
   * @param nameToNodeMap mapping from column name to Rex input ref
   * @return the Substrait extended expression proto
   * @throws SqlParseException if parsing or validation fails
   */
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
    Builder extendedExpression =
        ExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct);

    return new ExtendedExpressionProtoConverter().toProto(extendedExpression.build());
  }

  /**
   * Parses and validates a SQL expression, then converts it to a {@link RexNode}.
   *
   * @param sql SQL expression string
   * @param validator SQL validator
   * @param catalogReader Calcite catalog reader
   * @param nameToTypeMap mapping from column name to Calcite type
   * @param nameToNodeMap mapping from column name to Rex input ref
   * @return the converted RexNode
   * @throws SqlParseException if parsing or validation fails
   */
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

  /**
   * Registers tables from CREATE statements and prepares validator, catalog, and column bindings.
   *
   * @param tables list of CREATE TABLE statements; may be null
   * @return result bundle containing validator, catalog reader, and name/type and name/node maps
   * @throws SqlParseException if any CREATE statement is invalid
   */
  private Result registerCreateTablesForExtendedExpression(List<String> tables)
      throws SqlParseException {
    Map<String, RelDataType> nameToTypeMap = new LinkedHashMap<>();
    Map<String, RexNode> nameToNodeMap = new HashMap<>();
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    if (tables != null) {
      for (String tableDef : tables) {
        List<SubstraitTable> tList =
            SubstraitCreateStatementParser.processCreateStatements(tableDef);
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
    SqlValidator validator = new SubstraitSqlValidator(catalogReader);
    return new Result(validator, catalogReader, nameToTypeMap, nameToNodeMap);
  }

  /**
   * Converts a name-to-type map into a {@link NamedStruct} in Substrait types.
   *
   * @param nameToTypeMap mapping from column name to Calcite type
   * @return a {@link NamedStruct} with non-nullable struct type
   */
  private NamedStruct toNamedStruct(Map<String, RelDataType> nameToTypeMap) {
    ArrayList<String> names = new ArrayList<String>();
    ArrayList<Type> types = new ArrayList<Type>();
    for (Map.Entry<String, RelDataType> entry : nameToTypeMap.entrySet()) {
      String k = entry.getKey();
      RelDataType v = entry.getValue();
      names.add(k);
      types.add(TypeConverter.DEFAULT.toSubstrait(v));
    }
    return NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
  }
}
