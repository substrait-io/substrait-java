package io.substrait.isthmus.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;

/**
 * {@link SqlDialect} implementation used by Isthmus for SQL parsing and generation.
 *
 * <p>Primarily intended for internal testing and conversion of Calcite {@link RelNode} trees to
 * SQL.
 */
public class SubstraitSqlDialect extends SqlDialect {

  /** Default context for the Substrait SQL dialect. */
  public static SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT;

  /** Default instance of Substrait SQL dialect. */
  public static SqlDialect DEFAULT = new SubstraitSqlDialect(DEFAULT_CONTEXT);

  /**
   * Converts a Calcite {@link RelNode} to its SQL representation using the default dialect.
   *
   * @param relNode The Calcite relational node to convert.
   * @return A {@link SqlString} representing the SQL equivalent of the given {@link RelNode}.
   */
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

  /**
   * Constructs a Substrait SQL dialect with the given context.
   *
   * @param context The {@link SqlDialect.Context} providing configuration for SQL generation.
   */
  public SubstraitSqlDialect(Context context) {
    super(context);
  }

  /**
   * Indicates whether this dialect supports approximate COUNT(DISTINCT) operations.
   *
   * @return akways {@code true}, as Substrait SQL dialect supports approximate count distinct.
   */
  @Override
  public boolean supportsApproxCountDistinct() {
    return true;
  }
}
