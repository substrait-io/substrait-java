package io.substrait.isthmus.operation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class CalciteOperationBuilder extends SqlBasicVisitor<CalciteOperation> {
  protected final Map<Class<? extends SqlCall>, Function<SqlCall, CalciteOperation>> ddlHandlers =
      new ConcurrentHashMap<>();
  private final SqlToRelConverter converter;
  private final BiFunction<SqlToRelConverter, SqlNode, RelRoot> bestExprRelRootBuilder;

  public CalciteOperationBuilder(
      final SqlToRelConverter converter,
      BiFunction<SqlToRelConverter, SqlNode, RelRoot> bestExprRelRootBuilder) {
    this.converter = converter;
    this.bestExprRelRootBuilder = bestExprRelRootBuilder;

    ddlHandlers.put(SqlCreateTable.class, sqlCall -> handleCreateTable((SqlCreateTable) sqlCall));
    ddlHandlers.put(SqlCreateView.class, sqlCall -> handleCreateView((SqlCreateView) sqlCall));
  }

  private Function<SqlCall, CalciteOperation> findDdlHandler(final SqlCall call) {
    Class<?> currentClass = call.getClass();
    while (SqlCall.class.isAssignableFrom(currentClass)) {
      final Function<SqlCall, CalciteOperation> found = ddlHandlers.get(currentClass);
      if (found != null) {
        return found;
      }
      currentClass = currentClass.getSuperclass();
    }
    return null;
  }

  @Override
  public CalciteOperation visit(final SqlCall sqlCall) {
    Function<SqlCall, CalciteOperation> ddlHandler = findDdlHandler(sqlCall);
    if (ddlHandler != null) {
      return ddlHandler.apply(sqlCall);
    }
    return handleRelationalOperation(sqlCall);
  }

  protected CalciteOperation handleRelationalOperation(final SqlNode sqlNode) {
    return new RelationalOperation(bestExprRelRootBuilder.apply(converter, sqlNode));
  }

  protected CalciteOperation handleCreateTable(final SqlCreateTable sqlCreateTable) {
    if (sqlCreateTable.query == null) {
      throw new IllegalArgumentException("Only create table as select statements are supported");
    }

    final RelRoot queryRelRoot = bestExprRelRootBuilder.apply(converter, sqlCreateTable.query);
    return new CreateTableAs(sqlCreateTable.name.names, queryRelRoot);
  }

  protected CalciteOperation handleCreateView(final SqlCreateView sqlCreateView) {
    final RelRoot queryRelRoot = bestExprRelRootBuilder.apply(converter, sqlCreateView.query);
    return new CreateView(sqlCreateView.name.names, queryRelRoot);
  }
}
