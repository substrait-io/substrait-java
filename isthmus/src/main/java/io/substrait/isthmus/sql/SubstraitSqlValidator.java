package io.substrait.isthmus.sql;

import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * Custom SQL validator for Substrait SQL dialect.
 *
 * <p>Uses {@link SubstraitOperatorTable} and Calcite's validation framework to validate SQL
 * statements for Substrait-specific operators.
 */
public class SubstraitSqlValidator extends SqlValidatorImpl {

  /** Default configuration for the validator with identifier expansion enabled. */
  static SqlValidator.Config CONFIG = Config.DEFAULT.withIdentifierExpansion(true);

  /**
   * Creates a Substrait SQL validator using the default operator table.
   *
   * @param catalogReader The {@link Prepare.CatalogReader} providing schema and type information.
   */
  public SubstraitSqlValidator(Prepare.CatalogReader catalogReader) {
    super(SubstraitOperatorTable.INSTANCE, catalogReader, catalogReader.getTypeFactory(), CONFIG);
  }

  /**
   * Creates a Substrait SQL validator using a custom operator table.
   *
   * @param catalogReader The {@link Prepare.CatalogReader} providing schema and type information.
   * @param opTable The {@link SqlOperatorTable} containing SQL operators for validation.
   */
  public SubstraitSqlValidator(Prepare.CatalogReader catalogReader, SqlOperatorTable opTable) {
    super(opTable, catalogReader, catalogReader.getTypeFactory(), CONFIG);
  }
}
