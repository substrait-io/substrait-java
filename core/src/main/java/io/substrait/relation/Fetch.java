package io.substrait.relation;

import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.OptionalLong;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Fetch extends SingleInputRel implements HasExtension {

  public abstract long getOffset();

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

  public static ImmutableFetch.Builder builder() {
    return ImmutableFetch.builder();
  }
}
