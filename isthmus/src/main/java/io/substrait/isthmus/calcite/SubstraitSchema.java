package io.substrait.isthmus.calcite;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Basic {@link AbstractSchema} implementation for associating table names to {@link Table} objects
 */
public class SubstraitSchema extends AbstractSchema {

  /** Maps of table names to their associated tables */
  protected final Map<String, Table> tableMap;

  public SubstraitSchema(Map<String, Table> tableMap) {
    this.tableMap = tableMap;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
