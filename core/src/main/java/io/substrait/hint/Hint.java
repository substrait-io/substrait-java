package io.substrait.hint;

import io.substrait.proto.RelCommon;
import io.substrait.relation.HasExtension;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/** Optional, non-semantic metadata attached to a relation, such as aliases and statistics. */
@Value.Immutable
public abstract class Hint implements HasExtension {
  /**
   * Returns the alias for the relation, if any.
   *
   * @return the optional alias
   */
  public abstract Optional<String> getAlias();

  /**
   * Returns the suggested output field names.
   *
   * @return the output names
   */
  public abstract List<String> getOutputNames();

  /**
   * Returns the statistics hint for the relation, if any.
   *
   * @return the optional statistics
   */
  public abstract Optional<Stats> getStats();

  /**
   * Returns the runtime constraint hint, if any.
   *
   * @return the optional runtime constraint
   */
  public abstract Optional<RuntimeConstraint> getRuntimeConstraint();

  /**
   * Returns the computations this relation loads (consumes).
   *
   * @return the loaded computations
   */
  public abstract List<LoadedComputation> getLoadedComputations();

  /**
   * Returns the computations this relation saves (produces) for reuse.
   *
   * @return the saved computations
   */
  public abstract List<SavedComputation> getSavedComputations();

  /** The kinds of computation that can be shared between relations via hints. */
  public enum ComputationType {
    /** Unspecified computation type. */
    COMPUTATION_TYPE_UNSPECIFIED(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_UNSPECIFIED),
    /** A hash table. */
    COMPUTATION_TYPE_HASHTABLE(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_HASHTABLE),
    /** A bloom filter. */
    COMPUTATION_TYPE_BLOOM_FILTER(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_BLOOM_FILTER),
    /** An unknown computation type. */
    COMPUTATION_TYPE_UNKNOWN(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_UNKNOWN);

    private final RelCommon.Hint.ComputationType proto;

    ComputationType(RelCommon.Hint.ComputationType compType) {
      this.proto = compType;
    }

    /**
     * Returns the protobuf representation of this computation type.
     *
     * @return the proto computation type
     */
    public RelCommon.Hint.ComputationType toProto() {
      return this.proto;
    }

    /**
     * Returns the {@link ComputationType} matching the given protobuf computation type.
     *
     * @param proto the proto computation type
     * @return the matching computation type
     * @throws IllegalArgumentException if the type is not recognized
     */
    public static ComputationType fromProto(RelCommon.Hint.ComputationType proto) {
      for (final ComputationType compTypePojo : values()) {
        if (compTypePojo.proto == proto) {
          return compTypePojo;
        }
      }
      throw new IllegalArgumentException("Unknown computation type: " + proto);
    }
  }

  /** Estimated statistics for a relation's output. */
  @Value.Immutable
  public abstract static class Stats implements HasExtension {
    /**
     * Returns the estimated number of output rows.
     *
     * @return the estimated row count
     */
    public abstract double rowCount();

    /**
     * Returns the estimated size of a single output record.
     *
     * @return the estimated record size
     */
    public abstract double recordSize();

    /**
     * Creates a builder for {@link Stats}.
     *
     * @return a new builder
     */
    public static ImmutableStats.Builder builder() {
      return ImmutableStats.builder();
    }
  }

  /** A computation produced by a relation and made available for reuse by other relations. */
  @Value.Immutable
  public abstract static class SavedComputation {
    /**
     * Returns the identifier of the saved computation.
     *
     * @return the computation id
     */
    public abstract int computationId();

    /**
     * Returns the type of the saved computation.
     *
     * @return the computation type
     */
    public abstract ComputationType computationType();

    /**
     * Creates a builder for {@link SavedComputation}.
     *
     * @return a new builder
     */
    public static ImmutableSavedComputation.Builder builder() {
      return ImmutableSavedComputation.builder();
    }
  }

  /** A previously saved computation that a relation consumes. */
  @Value.Immutable
  public abstract static class LoadedComputation {
    /**
     * Returns the identifier of the loaded computation.
     *
     * @return the computation id
     */
    public abstract int computationId();

    /**
     * Returns the type of the loaded computation.
     *
     * @return the computation type
     */
    public abstract ComputationType computationType();

    /**
     * Creates a builder for {@link LoadedComputation}.
     *
     * @return a new builder
     */
    public static ImmutableLoadedComputation.Builder builder() {
      return ImmutableLoadedComputation.builder();
    }
  }

  /** Placeholder for runtime constraints associated with a relation. */
  @Value.Immutable
  public abstract static class RuntimeConstraint implements HasExtension {
    // NOTE: marked as todo in substrait spec 0.74.0

    /**
     * Creates a builder for {@link RuntimeConstraint}.
     *
     * @return a new builder
     */
    public static ImmutableRuntimeConstraint.Builder builder() {
      return ImmutableRuntimeConstraint.builder();
    }
  }

  /**
   * Creates a builder for {@link Hint}.
   *
   * @return a new builder
   */
  public static ImmutableHint.Builder builder() {
    return ImmutableHint.builder();
  }
}
