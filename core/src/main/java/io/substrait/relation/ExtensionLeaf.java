package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionLeaf extends ZeroInputRel {

  public abstract Extension.LeafRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionLeaf.Builder from(final Extension.LeafRelDetail detail) {
    return ImmutableExtensionLeaf.builder()
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType());
  }
}
