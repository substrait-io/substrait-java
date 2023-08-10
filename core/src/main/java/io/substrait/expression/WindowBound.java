package io.substrait.expression;

import org.immutables.value.Value;

@Value.Enclosing
public interface WindowBound {

  public BoundedKind boundedKind();

  enum BoundedKind {
    UNBOUNDED,
    BOUNDED,
    CURRENT_ROW
  }

  enum Direction {
    PRECEDING,
    FOLLOWING
  }

  public static CurrentRowWindowBound CURRENT_ROW =
      ImmutableWindowBound.CurrentRowWindowBound.builder().build();

  @Value.Immutable
  abstract static class UnboundedWindowBound implements WindowBound {
    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.UNBOUNDED;
    }

    public abstract Direction direction();

    public static ImmutableWindowBound.UnboundedWindowBound.Builder builder() {
      return ImmutableWindowBound.UnboundedWindowBound.builder();
    }
  }

  @Value.Immutable
  abstract static class BoundedWindowBound implements WindowBound {

    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.BOUNDED;
    }

    public abstract Direction direction();

    public abstract Expression offset();

    public static ImmutableWindowBound.BoundedWindowBound.Builder builder() {
      return ImmutableWindowBound.BoundedWindowBound.builder();
    }
  }

  @Value.Immutable
  static class CurrentRowWindowBound implements WindowBound {
    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.CURRENT_ROW;
    }
  }
}
