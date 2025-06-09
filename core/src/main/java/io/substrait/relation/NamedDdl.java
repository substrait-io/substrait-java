package io.substrait.relation;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class NamedDdl extends AbstractDdlRel {
  public abstract List<String> getNames();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableNamedDdl.Builder builder() {
    return ImmutableNamedDdl.builder();
  }
}
