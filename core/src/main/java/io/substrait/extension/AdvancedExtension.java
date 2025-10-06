package io.substrait.extension;

import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AdvancedExtension<
    O extends AdvancedExtension.Optimization, E extends AdvancedExtension.Enhancement> {
  public abstract List<O> getOptimizations();

  public abstract Optional<E> getEnhancement();

  public static <O extends AdvancedExtension.Optimization, E extends AdvancedExtension.Enhancement>
      ImmutableAdvancedExtension.Builder<O, E> builder() {
    return ImmutableAdvancedExtension.<O, E>builder();
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
