package io.substrait.isthmus.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/** Basic {@link AbstractSchema} implementation */
public class SubstraitSchema extends AbstractSchema {

  /** Map of table names to their associated tables */
  protected final Map<String, Table> tableMap;

  /** Map of schema names to their associated schemas */
  protected final Map<String, Schema> schemaMap;

  public SubstraitSchema() {
    this.tableMap = new HashMap<>();
    this.schemaMap = new HashMap<>();
  }

  public SubstraitSchema(final Map<String, Table> tableMap) {
    this.tableMap = tableMap;
    this.schemaMap = new HashMap<>();
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    return schemaMap;
  }
}
