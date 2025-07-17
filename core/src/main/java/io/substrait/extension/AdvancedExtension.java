package io.substrait.extension;

import io.substrait.relation.Extension;
import io.substrait.relation.RelProtoConverter;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AdvancedExtension {

  public abstract List<Extension.Optimization> getOptimizations();

  public abstract Optional<Extension.Enhancement> getEnhancement();

  public io.substrait.proto.AdvancedExtension toProto(RelProtoConverter relProtoConverter) {
    io.substrait.proto.AdvancedExtension.Builder builder =
        io.substrait.proto.AdvancedExtension.newBuilder();
    getEnhancement().ifPresent(e -> builder.setEnhancement(e.toProto(relProtoConverter)));
    getOptimizations().forEach(e -> builder.addOptimization(e.toProto(relProtoConverter)));
    return builder.build();
  }

  public static ImmutableAdvancedExtension.Builder builder() {
    return ImmutableAdvancedExtension.builder();
  }
}
