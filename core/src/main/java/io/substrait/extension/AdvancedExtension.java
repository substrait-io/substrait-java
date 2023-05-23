package io.substrait.extension;

import io.substrait.relation.Extension;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AdvancedExtension {

  public abstract Optional<Extension.Optimization> getOptimization();

  public abstract Optional<Extension.Enhancement> getEnhancement();

  public io.substrait.proto.AdvancedExtension toProto() {
    var builder = io.substrait.proto.AdvancedExtension.newBuilder();
    getEnhancement().ifPresent(e -> builder.setEnhancement(e.toProto()));
    getOptimization().ifPresent(e -> builder.setOptimization(e.toProto()));
    return builder.build();
  }

  public static ImmutableAdvancedExtension.Builder builder() {
    return ImmutableAdvancedExtension.builder();
  }
}
