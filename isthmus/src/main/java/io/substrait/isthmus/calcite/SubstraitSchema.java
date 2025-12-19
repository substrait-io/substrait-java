package io.substrait.isthmus.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Basic {@link AbstractSchema} implementation for Substrait.
 *
 * <p>Provides mappings for tables and subschemas, allowing integration with Calcite's schema
 * framework.
 */
public class SubstraitSchema extends AbstractSchema {

  /** Map of table names to their associated tables. */
  protected final Map<String, Table> tableMap;

  /** Map of schema names to their associated schemas. */
  protected final Map<String, Schema> schemaMap;

  /** Creates an empty Substrait schema with no tables or subschemas. */
  public SubstraitSchema() {
    this.tableMap = new HashMap<>();
    this.schemaMap = new HashMap<>();
  }

  /**
   * Creates a Substrait schema with the specified table map.
   *
   * @param tableMap A map of table names to {@link Table} instances.
   */
  public SubstraitSchema(Map<String, Table> tableMap) {
    this.tableMap = tableMap;
    this.schemaMap = new HashMap<>();
  }

  /**
   * Returns the map of table names to tables.
   *
   * @return A {@link Map} of table names to {@link Table} instances.
   */
  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }

  /**
   * Returns the map of schema names to subschemas.
   *
   * @return A {@link Map} of schema names to {@link Schema} instances.
   */
  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    return schemaMap;
  }
}
