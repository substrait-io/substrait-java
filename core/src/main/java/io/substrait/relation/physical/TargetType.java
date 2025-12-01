package io.substrait.relation.physical;

import org.immutables.value.Value;

@Value.Enclosing
public interface TargetType {

  @Value.Immutable
  abstract class Uri implements TargetType {
    public abstract String getUri();

    public static ImmutableTargetType.Uri.Builder builder() {
      return ImmutableTargetType.Uri.builder();
    }
  }

  @Value.Immutable
  abstract class Extended implements TargetType {
    public abstract com.google.protobuf.Any getExtended();

    public static ImmutableTargetType.Extended.Builder builder() {
      return ImmutableTargetType.Extended.builder();
    }
  }
}
