package io.substrait.relation;

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionMulti extends AbstractRel {

  public abstract Optional<Extension.MultiRelDetail> getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionMulti.Builder builder() {
    return ImmutableExtensionMulti.builder();
  }
}
