package io.substrait.isthmus.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;

/**
 * {@link SqlDialect} used by Isthmus for parsing
 *
 * <p>Intended primarily for internal testing
 */
public class SubstraitSqlDialect extends SqlDialect {

  public static SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT;

  public static SqlDialect DEFAULT = new SubstraitSqlDialect(DEFAULT_CONTEXT);

  public static SqlString toSql(RelNode relNode) {
    RelToSqlConverter relToSql = new RelToSqlConverter(DEFAULT);
    SqlNode sqlNode = relToSql.visitRoot(relNode).asStatement();
    return sqlNode.toSqlString(
        c ->
            c.withAlwaysUseParentheses(false)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0));
  }

  public SubstraitSqlDialect(Context context) {
    super(context);
  }

  @Override
  public boolean supportsApproxCountDistinct() {
    return true;
  }
}
