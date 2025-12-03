package io.substrait.hint;

import io.substrait.proto.RelCommon;
import io.substrait.relation.HasExtension;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Hint implements HasExtension {
  public abstract Optional<String> getAlias();

  public abstract List<String> getOutputNames();

  public abstract Optional<Stats> getStats();

  public abstract Optional<RuntimeConstraint> getRuntimeConstraint();

  public abstract List<LoadedComputation> getLoadedComputations();

  public abstract List<SavedComputation> getSavedComputations();

  public enum ComputationType {
    COMPUTATION_TYPE_UNSPECIFIED(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_UNSPECIFIED),
    COMPUTATION_TYPE_HASHTABLE(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_HASHTABLE),
    COMPUTATION_TYPE_BLOOM_FILTER(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_BLOOM_FILTER),
    COMPUTATION_TYPE_UNKNOWN(RelCommon.Hint.ComputationType.COMPUTATION_TYPE_UNKNOWN);

    private final RelCommon.Hint.ComputationType proto;

    ComputationType(final RelCommon.Hint.ComputationType compType) {
      this.proto = compType;
    }

    public RelCommon.Hint.ComputationType toProto() {
      return this.proto;
    }

    public static ComputationType fromProto(final RelCommon.Hint.ComputationType proto) {
      for (final ComputationType compTypePojo : values()) {
        if (compTypePojo.proto == proto) {
          return compTypePojo;
        }
      }
      throw new IllegalArgumentException("Unknown computation type: " + proto);
    }
  }

  @Value.Immutable
  public abstract static class Stats implements HasExtension {
    public abstract double rowCount();

    public abstract double recordSize();

    public static ImmutableStats.Builder builder() {
      return ImmutableStats.builder();
    }
  }

  @Value.Immutable
  public abstract static class SavedComputation {
    public abstract int computationId();

    public abstract ComputationType computationType();

    public static ImmutableSavedComputation.Builder builder() {
      return ImmutableSavedComputation.builder();
    }
  }

  @Value.Immutable
  public abstract static class LoadedComputation {
    public abstract int computationId();

    public abstract ComputationType computationType();

    public static ImmutableLoadedComputation.Builder builder() {
      return ImmutableLoadedComputation.builder();
    }
  }

  @Value.Immutable
  public abstract static class RuntimeConstraint implements HasExtension {
    // NOTE: marked as todo in substrait spec 0.74.0

    public static ImmutableRuntimeConstraint.Builder builder() {
      return ImmutableRuntimeConstraint.builder();
    }
  }

  public static ImmutableHint.Builder builder() {
    return ImmutableHint.builder();
  }
}
