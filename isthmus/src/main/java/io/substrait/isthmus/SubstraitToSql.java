package io.substrait.isthmus;

import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, List<String> tables)
      throws SqlParseException {
    var pair = registerCreateTables(tables);
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, pair.right, parserConfig);
  }

  public RelNode substraitRelToCalciteRel(
      Rel relRoot, Function<List<String>, NamedStruct> tableLookup) throws SqlParseException {
    var pair = registerCreateTables(tableLookup);

    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, pair.right, parserConfig);
  }

  private static final SqlDialect DEFAULT_DIALECT =
      new SqlDialect(
          SqlDialect.EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.SNOWFLAKE)
              .withDatabaseProductName("SNOWFLAKE")
              .withIdentifierQuoteString(null)
              .withNullCollation(NullCollation.HIGH));

  public static String toSql(RelNode root) {
    return toSql(root, DEFAULT_DIALECT);
  }

  public static String toSql(RelNode root, SqlDialect dialect) {
    return toSql(
        root,
        dialect,
        c ->
            c.withAlwaysUseParentheses(false)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0));
  }

  private static String toSql(
      RelNode root, SqlDialect dialect, UnaryOperator<SqlWriterConfig> transform) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect))).getSql();
  }
}
