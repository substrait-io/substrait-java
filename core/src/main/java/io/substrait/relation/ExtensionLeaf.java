package io.substrait.relation;

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionLeaf extends ZeroInputRel {

  public abstract Optional<Extension.LeafRelDetail> getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionLeaf.Builder from(Extension.LeafRelDetail detail) {
    return ImmutableExtensionLeaf.builder()
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType());
  }
}
