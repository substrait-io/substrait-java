package io.substrait.isthmus.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

/** Basic {@link AbstractTable} implementation */
public class SubstraitTable extends AbstractTable {

  private final RelDataType rowType;
  private final String tableName;

  public SubstraitTable(String tableName, RelDataType rowType) {
    this.tableName = tableName;
    this.rowType = rowType;
  }

  public String getName() {
    return tableName;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }
}
