package io.substrait.isthmus.sql;

import io.substrait.isthmus.calcite.rel.VirtualTable;
import io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;

/**
 * The {@link RelToSqlConverter} isthmus generates SQL with.
 *
 * <p>Calcite's own knows Calcite's own relations and throws an {@link AssertionError} naming
 * anything else, so a {@link VirtualTable} needs a case here. Rewriting the tree before handing it
 * over does not reach one: a subquery's relation is not an input, and {@code
 * org.apache.calcite.rel.rel2sql.SqlImplementor} converts it through {@code visitRoot}, which
 * arrives here like any other position.
 */
public class SubstraitRelToSqlConverter extends RelToSqlConverter {

  /**
   * Constructs a converter generating SQL in the given dialect.
   *
   * @param dialect the dialect to generate
   */
  public SubstraitRelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  /**
   * Converts a virtual table as the projections its rows stand for.
   *
   * <p>Found by the reflective dispatch {@link RelToSqlConverter} is built on, which resolves
   * {@code visit} against this class before the {@link org.apache.calcite.rel.RelNode} overload
   * that throws.
   *
   * @param virtualTable the table to convert
   * @return the SQL for its expansion
   */
  public Result visit(VirtualTable virtualTable) {
    return dispatch(VirtualTableExpansionRule.expand(virtualTable));
  }
}
