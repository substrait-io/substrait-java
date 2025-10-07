package io.substrait.extension;

import com.google.protobuf.Any;
import org.jspecify.annotations.NonNull;

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
   * @param advancedExtension the {@link AdvancedExtension} to convert, must not be null
   * @return the converted {@link io.substrait.proto.AdvancedExtension}
   */
  public io.substrait.proto.AdvancedExtension toProto(
      @NonNull final AdvancedExtension<O, E> advancedExtension) {
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
   * @param optimization the {@link AdvancedExtension.Optimization} to convert, must not be null
   * @return the converted proto
   */
  protected Any toProto(@NonNull final O optimization) {
    throw new UnsupportedOperationException(
        "missing serialization logic for AdvancedExtension.Optimization");
  }

  /**
   * Converts an {@link AdvancedExtension.Enhancement} to proto.
   *
   * <p>Override to provide a custom converter for {@link AdvancedExtension.Enhancement} data.
   *
   * @param enhancement the {@link AdvancedExtension.Enhancement} to convert, must not be null
   * @return the converted proto
   */
  protected Any toProto(@NonNull final E enhancement) {
    throw new UnsupportedOperationException(
        "missing serialization logic for AdvancedExtension.Enhancement");
  }
}
