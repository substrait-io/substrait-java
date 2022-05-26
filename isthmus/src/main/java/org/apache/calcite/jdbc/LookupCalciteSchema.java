package org.apache.calcite.jdbc;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LookupCalciteSchema extends SimpleCalciteSchema {
  private final Function<List<String>, Table> lookup;
  private final Map<List<String>, Table> cache = Maps.newHashMap();

  LookupCalciteSchema(
      @Nullable CalciteSchema parent,
      Schema schema,
      String name,
      Function<List<String>, Table> lookup) {
    super(parent, schema, name);
    this.lookup = lookup;
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    final CalciteSchema calciteSchema = new LookupCalciteSchema(this, schema, name, lookup);
    subSchemaMap.put(name, calciteSchema);
    return calciteSchema;
  }

  @Override
  protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
    if (cache.computeIfAbsent(path(schemaName), lookup) != null) {
      return null;
    }
    plus().add(schemaName, AbstractSchema.Factory.INSTANCE.create(null, null, null));
    return super.getSubSchema(schemaName, caseSensitive);
  }

  @Override
  protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
    Table table = cache.computeIfAbsent(path(tableName), lookup);
    if (table == null) {
      return null;
    }
    add(tableName, table);
    return getTable(tableName, caseSensitive);
  }

  public static CalciteSchema createRootSchema(Function<List<String>, Table> lookup) {
    Schema rootSchema = new CalciteConnectionImpl.RootSchema();
    return new LookupCalciteSchema(null, rootSchema, "", lookup);
  }
}
