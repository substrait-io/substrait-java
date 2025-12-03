package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionSingle extends SingleInputRel {

  public abstract Extension.SingleRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionSingle.Builder from(
      final Extension.SingleRelDetail detail, final Rel input) {
    return ImmutableExtensionSingle.builder()
        .input(input)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(input));
  }

  public static ImmutableExtensionSingle.Builder builder() {
    return ImmutableExtensionSingle.builder();
  }
}
