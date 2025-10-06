package io.substrait.extension;

/**
 * Converter from proto to {@link AdvancedExtension}.
 *
 * <p>Extend this class to customize and use with {@link io.substrait.relation.ProtoRelConverter} or
 * {@link io.substrait.plan.ProtoPlanConverter}.
 */
public class ProtoExtensionConverter {
  /**
   * Converts an {@link io.substrait.proto.AdvancedExtension} to {@link AdvancedExtension}.
   *
   * @param proto {@link io.substrait.proto.AdvancedExtension} to convert
   * @return the converted {@link AdvancedExtension}
   */
  public AdvancedExtension fromProto(final io.substrait.proto.AdvancedExtension proto) {
    final io.substrait.extension.ImmutableAdvancedExtension.Builder builder =
        AdvancedExtension.builder();
    if (proto.hasEnhancement()) {
      builder.enhancement(enhancementFromAdvancedExtension(proto.getEnhancement()));
    }
    proto
        .getOptimizationList()
        .forEach(
            optimization ->
                builder.addOptimizations(optimizationFromAdvancedExtension(optimization)));

    return builder.build();
  }

  /**
   * Converts an {@link AdvancedExtension.Optimization} from proto.
   *
   * <p>Override to provide a custom converter for {@link
   * io.substrait.proto.AdvancedExtension#getOptimizationList()} ()} data
   *
   * @param any the proto to convert the {@link AdvancedExtension.Optimization} from
   * @return the converted {@link AdvancedExtension.Optimization}
   */
  protected AdvancedExtension.Optimization optimizationFromAdvancedExtension(
      com.google.protobuf.Any any) {
    throw new UnsupportedOperationException(
        "missing deserialization logic for AdvancedExtension.Optimization");
  }

  /**
   * Converts an {@link AdvancedExtension.Enhancement} from proto.
   *
   * <p>Override to provide a custom converter for {@link
   * io.substrait.proto.AdvancedExtension#getEnhancement()} ()} data
   *
   * @param any the proto to convert the {@link AdvancedExtension.Enhancement} from
   * @return the converted {@link AdvancedExtension.Enhancement}
   */
  protected AdvancedExtension.Enhancement enhancementFromAdvancedExtension(
      com.google.protobuf.Any any) {
    throw new UnsupportedOperationException(
        "missing deserialization logic for AdvancedExtension.Enhancement");
  }
}
