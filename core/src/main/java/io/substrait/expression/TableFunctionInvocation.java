package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

/**
 * Represents a table function invocation, including its declaration, arguments, options, and the
 * concrete schema of the rows it produces.
 */
@Value.Immutable
public abstract class TableFunctionInvocation {

  /**
   * Returns the table function variant declaration.
   *
   * @return the function variant declaration
   */
  public abstract SimpleExtension.TableFunctionVariant declaration();

  /**
   * Returns the ordered list of function arguments. Value arguments are evaluated once per
   * invocation and may reference outer rows in scope, such as the left row of a lateral join.
   *
   * @return list of function arguments
   */
  public abstract List<FunctionArg> arguments();

  /**
   * Returns the options applied to this table function.
   *
   * @return list of function options
   */
  public abstract List<FunctionOption> options();

  /**
   * Returns the concrete schema of every row this invocation produces. It is required even when the
   * declaration omits a {@code schema} expression.
   *
   * @return the non-nullable output schema
   */
  public abstract Type.Struct schema();

  /**
   * Validates that the schema is non-nullable and that variadic arguments satisfy the parameter
   * consistency requirement.
   *
   * @throws IllegalArgumentException if validation fails
   */
  @Value.Check
  protected void check() {
    if (schema().nullable()) {
      throw new IllegalArgumentException(
          "Table function " + declaration().key() + " must declare a non-nullable schema");
    }
    VariadicParameterConsistencyValidator.validate(declaration(), arguments());
  }

  /**
   * Creates a builder for {@link TableFunctionInvocation}.
   *
   * @return a new immutable builder
   */
  public static ImmutableTableFunctionInvocation.Builder builder() {
    return ImmutableTableFunctionInvocation.builder();
  }
}
