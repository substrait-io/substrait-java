package io.substrait.relation;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * Represents a cross product (Cartesian product) relation that combines all rows from the left
 * input with all rows from the right input.
 */
@Value.Immutable
public abstract class Cross extends BiRel implements HasExtension {

  @Override
  protected Type.Struct deriveRecordType() {
    return TypeCreator.REQUIRED.struct(
        Stream.concat(
            getLeft().getRecordType().fields().stream(),
            getRight().getRecordType().fields().stream()));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a new builder for constructing a Cross relation.
   *
   * @return a new builder instance
   */
  public static ImmutableCross.Builder builder() {
    return ImmutableCross.builder();
  }
}
