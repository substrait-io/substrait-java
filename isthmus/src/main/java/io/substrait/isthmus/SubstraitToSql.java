package io.substrait.isthmus;

import io.substrait.relation.Rel;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, List<String> tables)
      throws SqlParseException {
    CalciteCatalogReader catalogReader = registerCreateTables(tables);
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalogReader, parserConfig);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalog, parserConfig);
  }

  // DEFAULT_SQL_DIALECT uses Calcite's EMPTY_CONTEXT with setting:
  //   identifierQuoteString : null, identifierEscapeQuoteString : null
  //   quotedCasing : UNCHANGED, unquotedCasing : TO_UPPER
  //   caseSensitive: true
  //   supportsApproxCountDistinct is true
  private static final SqlDialect DEFAULT_SQL_DIALECT =
      new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
        @Override
        public boolean supportsApproxCountDistinct() {
          return true;
        }
      };

  public static String toSql(RelNode root) {
    return toSql(root, DEFAULT_SQL_DIALECT);
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
