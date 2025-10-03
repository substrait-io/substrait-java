package io.substrait.extension;

import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AdvancedExtension {
  public abstract List<AdvancedExtension.Optimization> getOptimizations();

  public abstract Optional<AdvancedExtension.Enhancement> getEnhancement();

  public static ImmutableAdvancedExtension.Builder builder() {
    return ImmutableAdvancedExtension.builder();
  }

  /**
   * Optimization associated with an {@link io.substrait.proto.AdvancedExtension}
   *
   * <p>An optimization is helpful information that doesn't influence semantics. May be ignored by a
   * consumer.
   */
  public interface Optimization {}

  /**
   * Enhancement associated with an {@link io.substrait.proto.AdvancedExtension}
   *
   * <p>An enhancement alters semantics. Cannot be ignored by a consumer.
   */
  public interface Enhancement {}
}
