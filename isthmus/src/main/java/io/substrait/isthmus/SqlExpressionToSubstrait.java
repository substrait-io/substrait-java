package io.substrait.isthmus;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExpressionReference;
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.proto.ExtendedExpression;
import io.substrait.type.NamedStruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class SqlExpressionToSubstrait extends SqlConverterBase {

  public SqlExpressionToSubstrait() {
    this(null);
  }

  protected SqlExpressionToSubstrait(FeatureBoard features) {
    super(features);
  }

  private final ScalarFunctionConverter functionConverter =
      new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), factory);

  private final RexExpressionConverter rexExpressionConverter =
      new RexExpressionConverter(functionConverter);

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

  private ExtendedExpression executeInnerSQLExpression(
      String sqlExpression,
      SqlValidator validator,
      CalciteCatalogReader catalogReader,
      Map<String, RelDataType> nameToTypeMap,
      Map<String, RexNode> nameToNodeMap)
      throws SqlParseException {
    RexNode rexNode =
        sqlToRexNode(sqlExpression, validator, catalogReader, nameToTypeMap, nameToNodeMap);
    NamedStruct namedStruct = TypeConverter.DEFAULT.toNamedStruct(nameToTypeMap);

    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder()
            .expression(rexNode.accept(rexExpressionConverter))
            .addOutputNames("new-column")
            .build();

    List<io.substrait.extendedexpression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableExtendedExpression.Builder extendedExpression =
        ImmutableExtendedExpression.builder()
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
    SqlNode validSQLNode = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    SqlToRelConverter converter = createSqlToRelConverter(validator, catalogReader);
    return converter.convertExpression(validSQLNode, nameToNodeMap);
  }

  @VisibleForTesting
  SqlToRelConverter createSqlToRelConverter(
      SqlValidator validator, CalciteCatalogReader catalogReader) {
    return new SqlToRelConverter(
        null,
        validator,
        catalogReader,
        relOptCluster,
        StandardConvertletTable.INSTANCE,
        converterConfig);
  }
}
