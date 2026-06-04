package io.substrait.relation.physical;

import io.substrait.expression.FieldReference;
import org.immutables.value.Value;

/**
 * A single key comparison used by {@link HashJoin} and {@link MergeJoin}.
 *
 * <p>Models the {@code substrait.ComparisonJoinKey} protobuf message: a {@code left} and {@code
 * right} {@link FieldReference} together with the {@link ComparisonType} describing how the two are
 * compared.
 */
@Value.Enclosing
@Value.Immutable
public abstract class ComparisonJoinKey {

  public abstract FieldReference getLeft();

  public abstract FieldReference getRight();

  public abstract ComparisonType getComparison();

  public static ImmutableComparisonJoinKey.Builder builder() {
    return ImmutableComparisonJoinKey.builder();
  }

  /** Convenience factory for the common case of a {@link SimpleComparison}. */
  public static ComparisonJoinKey of(
      FieldReference left, FieldReference right, SimpleComparisonType type) {
    return builder().left(left).right(right).comparison(SimpleComparison.of(type)).build();
  }

  /**
   * Describes how the two keys of a {@link ComparisonJoinKey} are compared. Models the {@code
   * ComparisonType} oneof, which is either a {@link SimpleComparison} or a {@link
   * CustomComparison}.
   */
  public interface ComparisonType {
    <R, E extends Throwable> R accept(ComparisonTypeVisitor<R, E> visitor) throws E;
  }

  public interface ComparisonTypeVisitor<R, E extends Throwable> {
    R visit(SimpleComparison simpleComparison) throws E;

    R visit(CustomComparison customComparison) throws E;
  }

  /** One of the predefined {@link SimpleComparisonType} behaviors. */
  @Value.Immutable
  public abstract static class SimpleComparison implements ComparisonType {
    public abstract SimpleComparisonType getType();

    public static SimpleComparison of(SimpleComparisonType type) {
      return ImmutableComparisonJoinKey.SimpleComparison.builder().type(type).build();
    }

    @Override
    public <R, E extends Throwable> R accept(ComparisonTypeVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /**
   * A custom comparison behavior, given by a reference to a binary function with a boolean return
   * type.
   */
  @Value.Immutable
  public abstract static class CustomComparison implements ComparisonType {
    public abstract int getCustomFunctionReference();

    public static CustomComparison of(int customFunctionReference) {
      return ImmutableComparisonJoinKey.CustomComparison.builder()
          .customFunctionReference(customFunctionReference)
          .build();
    }

    @Override
    public <R, E extends Throwable> R accept(ComparisonTypeVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /**
   * The predefined comparison behaviors, mapping {@code ComparisonJoinKey.SimpleComparisonType}.
   */
  public enum SimpleComparisonType {
    UNSPECIFIED(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_UNSPECIFIED),
    EQ(io.substrait.proto.ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_EQ),
    IS_NOT_DISTINCT_FROM(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM),
    MIGHT_EQUAL(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_MIGHT_EQUAL);

    private final io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto;

    SimpleComparisonType(io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto) {
      this.proto = proto;
    }

    public static SimpleComparisonType fromProto(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto) {
      for (SimpleComparisonType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }

    public io.substrait.proto.ComparisonJoinKey.SimpleComparisonType toProto() {
      return proto;
    }
  }
}
