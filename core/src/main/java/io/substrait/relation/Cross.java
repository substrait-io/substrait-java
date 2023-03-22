package io.substrait.relation;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.stream.Stream;
import org.immutables.value.Value;

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
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableCross.Builder builder() {
    return ImmutableCross.builder();
  }
}
