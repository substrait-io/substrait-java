package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A relation that returns a contiguous range of its input rows, skipping an offset and optionally
 * limiting the number of rows returned.
 */
@Value.Immutable
public abstract class Fetch extends SingleInputRel implements HasExtension {

  /**
   * Returns the expression evaluated into the number of leading input rows to skip, if any. An
   * empty value (or an expression evaluating to null) is treated as {@code 0}.
   *
   * @return the optional offset expression
   */
  public abstract Optional<Expression> getOffset();

  /**
   * Returns the expression evaluated into the maximum number of rows to return, if any. An empty
   * value signals that all remaining rows should be returned.
   *
   * @return the optional row count expression
   */
  public abstract Optional<Expression> getCount();

  /**
   * Validates that the offset and count expressions are integer-typed. Both must evaluate to a
   * non-negative integer; {@code i64} is recommended but not required, so any integer width is
   * accepted (spec v0.99.0).
   *
   * @throws IllegalArgumentException if the offset or count expression is not integer-typed
   */
  @Value.Check
  protected void check() {
    requireIntegerType(getOffset(), "offset");
    requireIntegerType(getCount(), "count");
  }

  private static void requireIntegerType(Optional<Expression> expression, String field) {
    expression.ifPresent(
        e -> {
          Type type = e.getType();
          if (!type.isInteger()) {
            throw new IllegalArgumentException(
                "Fetch "
                    + field
                    + " expression must have an integer type (i8, i16, i32 or i64), but got: "
                    + type.accept(new StringTypeVisitor()));
          }
        });
  }

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Fetch}.
   *
   * @return a new builder
   */
  public static ImmutableFetch.Builder builder() {
    return ImmutableFetch.builder();
  }
}
