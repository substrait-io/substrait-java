package io.substrait.hint;

import io.substrait.proto.RelCommon;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Hint {
  public abstract Optional<String> getAlias();

  public abstract List<String> getOutputNames();

  public RelCommon.Hint toProto() {
    var builder = RelCommon.Hint.newBuilder().addAllOutputNames(getOutputNames());
    getAlias().ifPresent(builder::setAlias);
    return builder.build();
  }

  public static ImmutableHint.Builder builder() {
    return ImmutableHint.builder();
  }
}
