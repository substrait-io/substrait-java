package io.substrait.relation.physical;

import io.substrait.relation.HasExtension;
import io.substrait.relation.SingleInputRel;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

public abstract class AbstractExchangeRel extends SingleInputRel implements HasExtension {
  public abstract Integer getPartitionCount();

  public abstract List<ExchangeTarget> getTargets();

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
