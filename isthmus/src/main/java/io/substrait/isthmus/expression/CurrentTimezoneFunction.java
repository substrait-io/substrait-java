package io.substrait.isthmus.expression;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.fun.SqlBaseContextVariable;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Niladic Substrait-specific operator representing the current session timezone (Substrait {@code
 * current_timezone}).
 *
 * <p>Calcite has no built-in operator for the session timezone, so this is modeled on Calcite's own
 * string context variables such as {@code CURRENT_ROLE} / {@code CURRENT_USER} (see {@link
 * org.apache.calcite.sql.fun.SqlStringContextVariable}). Being a {@link SqlBaseContextVariable} it
 * is niladic, has {@link org.apache.calcite.sql.SqlSyntax#FUNCTION_ID} syntax and is a dynamic
 * function (plans referencing it are never cached).
 */
public class CurrentTimezoneFunction extends SqlBaseContextVariable {

  /** Singleton instance used by the Substrait ⇄ Calcite expression converters. */
  public static final CurrentTimezoneFunction INSTANCE = new CurrentTimezoneFunction();

  private CurrentTimezoneFunction() {
    super("CURRENT_TIMEZONE", ReturnTypes.VARCHAR_2000, SqlFunctionCategory.SYSTEM);
  }
}
