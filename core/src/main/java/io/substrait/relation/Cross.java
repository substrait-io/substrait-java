package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class Cross extends BiRel {

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableCross.Builder builder() {
    return ImmutableCross.builder();
  }
}
