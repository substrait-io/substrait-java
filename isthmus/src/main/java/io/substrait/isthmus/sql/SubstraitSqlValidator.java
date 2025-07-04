package io.substrait.isthmus.sql;

import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class SubstraitSqlValidator extends SqlValidatorImpl {

  static SqlValidator.Config CONFIG = Config.DEFAULT;

  public SubstraitSqlValidator(Prepare.CatalogReader catalogReader) {
    super(SubstraitOperatorTable.INSTANCE, catalogReader, catalogReader.getTypeFactory(), CONFIG);
  }
}
