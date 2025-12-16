package io.substrait.extension;

import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Represents advanced extensions that can include optimizations and enhancements.
 *
 * <p>Optimizations provide optional hints that do not affect semantics, while enhancements
 * introduce semantic changes and must be honored by consumers.
 *
 * @param <O> type of optimization
 * @param <E> type of enhancement
 */
@Value.Immutable
public abstract class AdvancedExtension<
    O extends AdvancedExtension.Optimization, E extends AdvancedExtension.Enhancement> {

  /**
   * Returns the list of optimizations associated with this extension.
   *
   * @return list of optimizations
   */
  public abstract List<O> getOptimizations();

  /**
   * Returns the optional enhancement associated with this extension.
   *
   * @return optional enhancement
   */
  public abstract Optional<E> getEnhancement();

  /**
   * Creates a builder for {@link AdvancedExtension}.
   *
   * @param <O> type of optimization
   * @param <E> type of enhancement
   * @return builder instance
   */
  public static <O extends AdvancedExtension.Optimization, E extends AdvancedExtension.Enhancement>
      ImmutableAdvancedExtension.Builder<O, E> builder() {
    return ImmutableAdvancedExtension.<O, E>builder();
  }

  /**
   * Represents an optimization associated with an {@link io.substrait.proto.AdvancedExtension}.
   *
   * <p>An optimization provides helpful information that does not influence semantics and may be
   * ignored by consumers.
   */
  public interface Optimization {}

  /**
   * Represents an enhancement associated with an {@link io.substrait.proto.AdvancedExtension}.
   *
   * <p>An enhancement alters semantics and cannot be ignored by consumers.
   */
  public interface Enhancement {}
}
