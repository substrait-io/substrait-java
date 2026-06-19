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

  /**
   * Returns the left-side key field of this comparison.
   *
   * @return the left key field reference
   */
  public abstract FieldReference getLeft();

  /**
   * Returns the right-side key field of this comparison.
   *
   * @return the right key field reference
   */
  public abstract FieldReference getRight();

  /**
   * Returns how the left and right keys are compared.
   *
   * @return the comparison type
   */
  public abstract ComparisonType getComparison();

  /**
   * Creates a builder for {@link ComparisonJoinKey}.
   *
   * @return a new builder
   */
  public static ImmutableComparisonJoinKey.Builder builder() {
    return ImmutableComparisonJoinKey.builder();
  }

  /**
   * Convenience factory for the common case of a {@link SimpleComparison}.
   *
   * @param left the left key field reference
   * @param right the right key field reference
   * @param type the simple comparison behavior
   * @return a new comparison join key
   */
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
    /**
     * Accepts a visitor, dispatching to the overload matching this comparison type.
     *
     * @param visitor the visitor to accept
     * @param <R> the visitor return type
     * @param <E> the type of exception the visitor may throw
     * @return the visitor result
     * @throws E if the visitor fails
     */
    <R, E extends Throwable> R accept(ComparisonTypeVisitor<R, E> visitor) throws E;
  }

  /**
   * Visitor over the {@link ComparisonType} variants.
   *
   * @param <R> the visitor return type
   * @param <E> the type of exception the visitor may throw
   */
  public interface ComparisonTypeVisitor<R, E extends Throwable> {
    /**
     * Visits a {@link SimpleComparison}.
     *
     * @param simpleComparison the simple comparison to visit
     * @return the visitor result
     * @throws E if the visit fails
     */
    R visit(SimpleComparison simpleComparison) throws E;

    /**
     * Visits a {@link CustomComparison}.
     *
     * @param customComparison the custom comparison to visit
     * @return the visitor result
     * @throws E if the visit fails
     */
    R visit(CustomComparison customComparison) throws E;
  }

  /** One of the predefined {@link SimpleComparisonType} behaviors. */
  @Value.Immutable
  public abstract static class SimpleComparison implements ComparisonType {
    /**
     * Returns the predefined comparison behavior.
     *
     * @return the simple comparison type
     */
    public abstract SimpleComparisonType getType();

    /**
     * Creates a {@link SimpleComparison} with the given behavior.
     *
     * @param type the simple comparison behavior
     * @return a new simple comparison
     */
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
    /**
     * Returns the reference to the binary boolean-returning comparison function.
     *
     * @return the custom function reference
     */
    public abstract int getCustomFunctionReference();

    /**
     * Creates a {@link CustomComparison} referencing the given comparison function.
     *
     * @param customFunctionReference the reference to the comparison function
     * @return a new custom comparison
     */
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
    /** Unspecified or unknown comparison behavior. */
    UNSPECIFIED(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_UNSPECIFIED),
    /** Keys match when they are equal. */
    EQ(io.substrait.proto.ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_EQ),
    /** Keys match when they are not distinct, i.e. equal or both null. */
    IS_NOT_DISTINCT_FROM(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM),
    /** Keys may match; equality is not guaranteed and must be rechecked. */
    MIGHT_EQUAL(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType
            .SIMPLE_COMPARISON_TYPE_MIGHT_EQUAL);

    private final io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto;

    SimpleComparisonType(io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto) {
      this.proto = proto;
    }

    /**
     * Returns the {@link SimpleComparisonType} matching the given protobuf comparison type.
     *
     * @param proto the proto comparison type
     * @return the matching comparison type
     * @throws IllegalArgumentException if the type is not recognized
     */
    public static SimpleComparisonType fromProto(
        io.substrait.proto.ComparisonJoinKey.SimpleComparisonType proto) {
      for (SimpleComparisonType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }

    /**
     * Returns the protobuf representation of this comparison type.
     *
     * @return the proto comparison type
     */
    public io.substrait.proto.ComparisonJoinKey.SimpleComparisonType toProto() {
      return proto;
    }
  }
}
