package io.substrait.relation.physical;

import io.substrait.relation.HasExtension;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

/**
 * Physical exchange relation with a single input and extension metadata. Provides partitioning and
 * target information for data exchange.
 */
public abstract class AbstractExchangeRel extends SingleInputRel implements HasExtension {

  /**
   * Returns the number of partitions for this exchange.
   *
   * @return the partition count, or {@code null} if unspecified
   */
  public abstract Integer getPartitionCount();

  /**
   * Returns the configured exchange targets.
   *
   * @return the list of exchange targets (never {@code null})
   */
  public abstract List<ExchangeTarget> getTargets();

  /**
   * Derives the output record type from the input.
   *
   * @return the output struct type
   */
  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  /** Exchange target specification: partition IDs and target type. */
  @Value.Immutable
  public abstract static class ExchangeTarget {

    /**
     * Returns the partition IDs served by this target.
     *
     * @return list of partition IDs
     */
    public abstract List<Integer> getPartitionIds();

    /**
     * Returns the target type (e.g., local, remote).
     *
     * @return the target type
     */
    public abstract TargetType getType();

    /**
     * Creates a builder for {@link ImmutableExchangeTarget}.
     *
     * @return a new builder instance
     */
    public static ImmutableExchangeTarget.Builder builder() {
      return ImmutableExchangeTarget.builder();
    }
  }
}
