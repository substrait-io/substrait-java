package io.substrait.relation.physical;

import org.immutables.value.Value;

/** Identifies the destination of an exchange relation. */
@Value.Enclosing
public interface TargetType {

  /** A target identified by a URI. */
  @Value.Immutable
  abstract class Uri implements TargetType {
    /**
     * Returns the URI identifying the target.
     *
     * @return the target URI
     */
    public abstract String getUri();

    /**
     * Creates a builder for {@link Uri}.
     *
     * @return a new builder
     */
    public static ImmutableTargetType.Uri.Builder builder() {
      return ImmutableTargetType.Uri.builder();
    }
  }

  /** A target described by an extension-defined message. */
  @Value.Immutable
  abstract class Extended implements TargetType {
    /**
     * Returns the extension-defined message describing the target.
     *
     * @return the extension target message
     */
    public abstract com.google.protobuf.Any getExtended();

    /**
     * Creates a builder for {@link Extended}.
     *
     * @return a new builder
     */
    public static ImmutableTargetType.Extended.Builder builder() {
      return ImmutableTargetType.Extended.builder();
    }
  }
}
