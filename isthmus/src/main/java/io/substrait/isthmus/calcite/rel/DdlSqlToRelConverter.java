package io.substrait.isthmus.calcite.rel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * Visitor that converts DDL {@link SqlCall}s to {@link RelRoot}, delegating to specific handlers
 * for supported statements (CREATE TABLE AS SELECT, CREATE VIEW).
 *
 * <p>Non-DDL statements are passed through to {@link SqlToRelConverter#convertQuery(SqlNode,
 * boolean, boolean)}.
 */
public class DdlSqlToRelConverter extends SqlBasicVisitor<RelRoot> {

  /**
   * Registry mapping DDL {@link SqlCall} classes to handler functions that convert them into {@link
   * RelRoot} instances.
   */
  protected final Map<Class<? extends SqlCall>, Function<SqlCall, RelRoot>> ddlHandlers =
      new ConcurrentHashMap<>();

  private final SqlToRelConverter converter;

  private Function<SqlCall, RelRoot> findDdlHandler(final SqlCall call) {
    Class<?> currentClass = call.getClass();
    while (SqlCall.class.isAssignableFrom(currentClass)) {
      final Function<SqlCall, RelRoot> found = ddlHandlers.get(currentClass);
      if (found != null) {
        return found;
      }
      currentClass = currentClass.getSuperclass();
    }
    return null;
  }

  /**
   * Creates a DDL SQL-to-Rel converter using the given {@link SqlToRelConverter}.
   *
   * @param converter the converter used for non-DDL and query parts of DDL (e.g., CTAS)
   */
  public DdlSqlToRelConverter(SqlToRelConverter converter) {
    this.converter = converter;

    ddlHandlers.put(SqlCreateTable.class, sqlCall -> handleCreateTable((SqlCreateTable) sqlCall));
    ddlHandlers.put(SqlCreateView.class, sqlCall -> handleCreateView((SqlCreateView) sqlCall));
  }

  /**
   * Dispatches a {@link SqlCall} to an appropriate DDL handler; falls back to non-DDL handling.
   *
   * @param sqlCall the SQL call node
   * @return the converted relational root
   */
  @Override
  public RelRoot visit(SqlCall sqlCall) {
    Function<SqlCall, RelRoot> ddlHandler = findDdlHandler(sqlCall);
    if (ddlHandler != null) {
      return ddlHandler.apply(sqlCall);
    }
    return handleNonDdl(sqlCall);
  }

  /**
   * Handles non-DDL SQL nodes via the underlying {@link SqlToRelConverter}.
   *
   * @param sqlNode the SQL node to convert
   * @return the converted relational root
   */
  protected RelRoot handleNonDdl(final SqlNode sqlNode) {
    return converter.convertQuery(sqlNode, true, true);
  }

  /**
   * Handles {@code CREATE TABLE AS SELECT} statements.
   *
   * @param sqlCreateTable the CREATE TABLE node
   * @return a {@link RelRoot} wrapping a synthetic {@code CreateTable} relational node
   * @throws IllegalArgumentException if the statement is not a CTAS
   */
  protected RelRoot handleCreateTable(final SqlCreateTable sqlCreateTable) {
    if (sqlCreateTable.query == null) {
      throw new IllegalArgumentException("Only create table as select statements are supported");
    }
    final RelNode input = converter.convertQuery(sqlCreateTable.query, true, true).rel;
    return RelRoot.of(new CreateTable(sqlCreateTable.name.names, input), sqlCreateTable.getKind());
  }

  /**
   * Handles {@code CREATE VIEW} statements.
   *
   * @param sqlCreateView the CREATE VIEW node
   * @return a {@link RelRoot} wrapping a synthetic {@code CreateTable} relational node representing
   *     the view definition
   */
  protected RelRoot handleCreateView(final SqlCreateView sqlCreateView) {
    final RelNode input = converter.convertQuery(sqlCreateView.query, true, true).rel;
    return RelRoot.of(new CreateTable(sqlCreateView.name.names, input), sqlCreateView.getKind());
  }
}
