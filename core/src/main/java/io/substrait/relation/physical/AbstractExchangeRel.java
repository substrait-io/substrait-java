package io.substrait.relation.physical;

import io.substrait.relation.HasExtension;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

public abstract class AbstractExchangeRel extends SingleInputRel implements HasExtension {
  public abstract Optional<Integer> getPartitionCount();

  public abstract Optional<List<ExchangeTarget>> getTargets();

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Value.Immutable
  public abstract static class ExchangeTarget {
    public abstract List<Integer> getPartitionIds();

    public abstract TargetType getType();

    public static ImmutableExchangeTarget.Builder builder() {
      return ImmutableExchangeTarget.builder();
    }
  }
}
