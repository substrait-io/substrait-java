package io.substrait.extension;

import com.google.protobuf.Any;

/**
 * Converter from {@link AdvancedExtension} to proto.
 *
 * <p>Extend this class to customize and use with {@link io.substrait.relation.RelProtoConverter}
 * or @{link {@link io.substrait.plan.PlanProtoConverter}.
 */
public class ExtensionProtoConverter<
    O extends AdvancedExtension.Optimization, E extends AdvancedExtension.Enhancement> {
  /**
   * Converts an {@link AdvancedExtension} to {@link io.substrait.proto.AdvancedExtension}.
   *
   * @param advancedExtension the {@link AdvancedExtension} to convert
   * @return the converted {@link io.substrait.proto.AdvancedExtension}
   */
  public io.substrait.proto.AdvancedExtension toProto(
      final AdvancedExtension<O, E> advancedExtension) {
    final io.substrait.proto.AdvancedExtension.Builder builder =
        io.substrait.proto.AdvancedExtension.newBuilder();
    advancedExtension.getEnhancement().ifPresent(e -> builder.setEnhancement(toProto(e)));
    advancedExtension.getOptimizations().forEach(e -> builder.addOptimization(toProto(e)));
    return builder.build();
  }

  /**
   * Converts an {@link AdvancedExtension.Optimization} to proto.
   *
   * <p>Override to provide a custom converter for {@link AdvancedExtension.Optimization} data.
   *
   * @param optimization the {@link AdvancedExtension.Optimization} to convert
   * @return the converted proto
   */
  protected Any toProto(final O optimization) {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }

  /**
   * Converts an {@link AdvancedExtension.Enhancement} to proto.
   *
   * <p>Override to provide a custom converter for {@link AdvancedExtension.Enhancement} data.
   *
   * @param enhancement the {@link AdvancedExtension.Enhancement} to convert
   * @return the converted proto
   */
  protected Any toProto(final E enhancement) {
    return com.google.protobuf.Any.pack(com.google.protobuf.Empty.getDefaultInstance());
  }
}
