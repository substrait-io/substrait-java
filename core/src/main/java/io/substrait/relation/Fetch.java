package io.substrait.relation;

import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.OptionalLong;
import org.immutables.value.Value;

/**
 * A relation that returns a contiguous range of its input rows, skipping an offset and optionally
 * limiting the number of rows returned.
 */
@Value.Immutable
public abstract class Fetch extends SingleInputRel implements HasExtension {

  /**
   * Returns the number of leading input rows to skip.
   *
   * @return the offset
   */
  public abstract long getOffset();

  /**
   * Returns the maximum number of rows to return, or empty to return all remaining rows.
   *
   * @return the optional row count limit
   */
  public abstract OptionalLong getCount();

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
