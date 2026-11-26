package io.substrait.relation;

import io.substrait.expression.TableFunctionInvocation;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/**
 * A leaf relation that invokes a table function and returns its generated rows. It has no input of
 * its own; the arguments of its function may reference outer rows in scope, such as the left row of
 * a {@link LateralJoin}.
 */
@Value.Immutable
public abstract class TableFunction extends ZeroInputRel implements HasExtension {

  /**
   * Returns the table function call whose rows this relation produces.
   *
   * @return the table function invocation
   */
  public abstract TableFunctionInvocation getFunction();

  @Override
  protected Type.Struct deriveRecordType() {
    return getFunction().schema();
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link TableFunction}.
   *
   * @return a new builder
   */
  public static ImmutableTableFunction.Builder builder() {
    return ImmutableTableFunction.builder();
  }
}
