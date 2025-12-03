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

public class DdlSqlToRelConverter extends SqlBasicVisitor<RelRoot> {

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

  public DdlSqlToRelConverter(final SqlToRelConverter converter) {
    this.converter = converter;

    ddlHandlers.put(SqlCreateTable.class, sqlCall -> handleCreateTable((SqlCreateTable) sqlCall));
    ddlHandlers.put(SqlCreateView.class, sqlCall -> handleCreateView((SqlCreateView) sqlCall));
  }

  @Override
  public RelRoot visit(final SqlCall sqlCall) {
    final Function<SqlCall, RelRoot> ddlHandler = findDdlHandler(sqlCall);
    if (ddlHandler != null) {
      return ddlHandler.apply(sqlCall);
    }
    return handleNonDdl(sqlCall);
  }

  protected RelRoot handleNonDdl(final SqlNode sqlNode) {
    return converter.convertQuery(sqlNode, true, true);
  }

  protected RelRoot handleCreateTable(final SqlCreateTable sqlCreateTable) {
    if (sqlCreateTable.query == null) {
      throw new IllegalArgumentException("Only create table as select statements are supported");
    }
    final RelNode input = converter.convertQuery(sqlCreateTable.query, true, true).rel;
    return RelRoot.of(new CreateTable(sqlCreateTable.name.names, input), sqlCreateTable.getKind());
  }

  protected RelRoot handleCreateView(final SqlCreateView sqlCreateView) {
    final RelNode input = converter.convertQuery(sqlCreateView.query, true, true).rel;
    return RelRoot.of(new CreateTable(sqlCreateView.name.names, input), sqlCreateView.getKind());
  }
}
