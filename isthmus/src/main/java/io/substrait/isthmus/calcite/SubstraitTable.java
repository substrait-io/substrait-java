package io.substrait.isthmus.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Basic {@link AbstractTable} implementation for Substrait.
 *
 * <p>Represents a table with a fixed name and row type, used for schema integration with Calcite.
 */
public class SubstraitTable extends AbstractTable {

  /** The row type of the table. */
  private final RelDataType rowType;

  /** The name of the table. */
  private final String tableName;

  /**
   * Creates a Substrait table with the given name and row type.
   *
   * @param tableName The name of the table.
   * @param rowType The Calcite {@link RelDataType} representing the table's row type.
   */
  public SubstraitTable(String tableName, RelDataType rowType) {
    this.tableName = tableName;
    this.rowType = rowType;
  }

  /**
   * Returns the name of the table.
   *
   * @return The table name
   */
  public String getName() {
    return tableName;
  }

  /**
   * Returns the row type of the table.
   *
   * @param typeFactory The Calcite type factory (ignored in this implementation).
   * @return The {@link RelDataType} representing the table's row type.
   */
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }
}
