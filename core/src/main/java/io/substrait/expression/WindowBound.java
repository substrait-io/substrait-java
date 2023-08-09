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

  @Value.Immutable
  abstract static class UnboundedWindowBound implements WindowBound {
    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.UNBOUNDED;
    }

    public abstract Direction direction();
  }

  @Value.Immutable
  abstract static class BoundedWindowBound implements WindowBound {

    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.BOUNDED;
    }

    public abstract Direction direction();

    public abstract Expression offset();
  }

  @Value.Immutable
  static class CurrentRowWindowBound implements WindowBound {
    @Override
    public BoundedKind boundedKind() {
      return BoundedKind.CURRENT_ROW;
    }
  }
}
