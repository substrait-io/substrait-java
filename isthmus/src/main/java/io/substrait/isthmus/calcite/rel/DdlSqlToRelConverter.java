package io.substrait.isthmus.calcite.rel;

import io.substrait.relation.AbstractWriteRel.CreateMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
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
   * @throws IllegalArgumentException if the statement is not a CTAS or combines OR REPLACE and IF
   *     NOT EXISTS
   */
  protected RelRoot handleCreateTable(final SqlCreateTable sqlCreateTable) {
    if (sqlCreateTable.query == null) {
      throw new IllegalArgumentException("Only create table as select statements are supported");
    }
    if (sqlCreateTable.getReplace() && sqlCreateTable.ifNotExists) {
      throw new IllegalArgumentException(
          "CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS");
    }
    final CreateMode createMode =
        sqlCreateTable.getReplace()
            ? CreateMode.REPLACE_IF_EXISTS
            : sqlCreateTable.ifNotExists ? CreateMode.IGNORE_IF_EXISTS : CreateMode.ERROR_IF_EXISTS;
    final RelNode input = converter.convertQuery(sqlCreateTable.query, true, true).rel;
    final RelDataType schema = declaredSchema(sqlCreateTable.columnList, input);
    return RelRoot.of(
        schema == null
            ? new CreateTable(sqlCreateTable.name.names, input, createMode)
            : new CreateTable(sqlCreateTable.name.names, schema, input, createMode),
        sqlCreateTable.getKind());
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
    final RelDataType schema = declaredSchema(sqlCreateView.columnList, input);
    return RelRoot.of(
        schema == null
            ? new CreateView(sqlCreateView.name.names, input)
            : new CreateView(sqlCreateView.name.names, schema, input),
        sqlCreateView.getKind());
  }

  /**
   * Returns the schema a {@code CREATE} statement declares for the object it creates, or null where
   * it declares none and the query's own row type is the schema.
   *
   * <p>A column is named by the statement and takes the type the statement gives it; a column named
   * without a type takes the one the query produces for it. Anything else in the list -- a
   * constraint -- describes the table rather than its columns and is left to whatever creates it.
   *
   * @param columnList the column list of the statement, which may be null
   * @param input the relation filling the object
   * @return the declared schema, or null where the statement declares none
   * @throws IllegalArgumentException if the statement declares more columns than the query produces
   */
  protected RelDataType declaredSchema(final SqlNodeList columnList, final RelNode input) {
    if (columnList == null) {
      return null;
    }
    final List<RelDataTypeField> produced = input.getRowType().getFieldList();
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (final SqlNode element : columnList) {
      final SqlIdentifier name;
      RelDataType type = null;
      if (element instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration column = (SqlColumnDeclaration) element;
        name = column.name;
        type =
            converter
                .getCluster()
                .getTypeFactory()
                .createTypeWithNullability(
                    column.dataType.deriveType(converter.validator),
                    column.strategy != ColumnStrategy.NOT_NULLABLE);
      } else if (element instanceof SqlIdentifier) {
        name = (SqlIdentifier) element;
      } else {
        continue;
      }
      if (names.size() >= produced.size()) {
        throw new IllegalArgumentException(
            "The statement declares more columns than the query produces: "
                + name
                + " has nothing to fill it");
      }
      types.add(type == null ? produced.get(names.size()).getType() : type);
      names.add(name.getSimple());
    }
    return names.isEmpty()
        ? null
        : converter.getCluster().getTypeFactory().createStructType(types, names);
  }
}
