package io.substrait.expression;

import org.immutables.value.Value;

@Value.Enclosing
public interface WindowBound {

  interface WindowBoundVisitor<R, E extends Throwable> {
    R visit(Preceding preceding);

    R visit(Following following);

    R visit(CurrentRow currentRow);

    R visit(Unbounded unbounded);
  }

  <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor);

  CurrentRow CURRENT_ROW = ImmutableWindowBound.CurrentRow.builder().build();
  Unbounded UNBOUNDED = ImmutableWindowBound.Unbounded.builder().build();

  @Value.Immutable
  abstract class Preceding implements WindowBound {
    public abstract long offset();

    public static Preceding of(long offset) {
      return ImmutableWindowBound.Preceding.builder().offset(offset).build();
    }

    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Following implements WindowBound {
    public abstract long offset();

    public static Following of(long offset) {
      return ImmutableWindowBound.Following.builder().offset(offset).build();
    }

    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class CurrentRow implements WindowBound {
    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Unbounded implements WindowBound {
    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }
}
