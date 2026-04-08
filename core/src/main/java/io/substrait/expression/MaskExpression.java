package io.substrait.expression;

import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A mask expression that selectively removes fields from complex types (struct, list, map).
 *
 * <p>This corresponds to the {@code Expression.MaskExpression} message in the Substrait protobuf
 * specification. It is used in {@code ReadRel} to describe column projection — the subset of a
 * relation's schema that should actually be read.
 *
 * @see <a href="https://substrait.io/expressions/field_references/">Substrait Field References</a>
 */
@Value.Enclosing
public interface MaskExpression {

  /** The top-level struct selection describing which fields to include. */
  StructSelect getSelect();

  /**
   * When {@code true}, a struct that has only a single selected field will <em>not</em> be
   * unwrapped into its child type.
   */
  @Value.Default
  default boolean getMaintainSingularStruct() {
    return false;
  }

  static ImmutableMaskExpression.Mask.Builder builder() {
    return ImmutableMaskExpression.Mask.builder();
  }

  // ---------------------------------------------------------------------------
  // Top-level MaskExpression value
  // ---------------------------------------------------------------------------

  /** The concrete mask expression value holding the top-level struct selection and options. */
  @Value.Immutable
  interface Mask extends MaskExpression {}

  // ---------------------------------------------------------------------------
  // Select – a union of StructSelect | ListSelect | MapSelect
  // ---------------------------------------------------------------------------

  /** A selection on a complex type – one of StructSelect, ListSelect, or MapSelect. */
  interface Select {
    <R, C extends VisitationContext, E extends Throwable> R accept(
        MaskExpressionVisitor<R, C, E> visitor, C context) throws E;
  }

  // ---------------------------------------------------------------------------
  // Struct selection
  // ---------------------------------------------------------------------------

  /** Selects a subset of fields from a struct type. */
  @Value.Immutable
  interface StructSelect extends Select {
    List<StructItem> getStructItems();

    static ImmutableMaskExpression.StructSelect.Builder builder() {
      return ImmutableMaskExpression.StructSelect.builder();
    }

    @Override
    default <R, C extends VisitationContext, E extends Throwable> R accept(
        MaskExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Selects a single field from a struct, with an optional nested child selection. */
  @Value.Immutable
  interface StructItem {
    /** Zero-based field index within the struct. */
    int getField();

    /** Optional child selection for nested complex types. */
    Optional<Select> getChild();

    static ImmutableMaskExpression.StructItem.Builder builder() {
      return ImmutableMaskExpression.StructItem.builder();
    }

    static StructItem of(int field) {
      return builder().field(field).build();
    }

    static StructItem of(int field, Select child) {
      return builder().field(field).child(child).build();
    }
  }

  // ---------------------------------------------------------------------------
  // List selection
  // ---------------------------------------------------------------------------

  /** Selects elements from a list type by index or slice. */
  @Value.Immutable
  interface ListSelect extends Select {
    List<ListSelectItem> getSelection();

    /** Optional child selection applied to each selected element. */
    Optional<Select> getChild();

    static ImmutableMaskExpression.ListSelect.Builder builder() {
      return ImmutableMaskExpression.ListSelect.builder();
    }

    @Override
    default <R, C extends VisitationContext, E extends Throwable> R accept(
        MaskExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** A single selection within a list – either an element or a slice. */
  @Value.Immutable
  interface ListSelectItem {
    Optional<ListElement> getItem();

    Optional<ListSlice> getSlice();

    static ImmutableMaskExpression.ListSelectItem.Builder builder() {
      return ImmutableMaskExpression.ListSelectItem.builder();
    }

    static ListSelectItem ofItem(ListElement element) {
      return builder().item(element).build();
    }

    static ListSelectItem ofSlice(ListSlice slice) {
      return builder().slice(slice).build();
    }
  }

  /** Selects a single element from a list by zero-based index. */
  @Value.Immutable
  interface ListElement {
    int getField();

    static ImmutableMaskExpression.ListElement.Builder builder() {
      return ImmutableMaskExpression.ListElement.builder();
    }

    static ListElement of(int field) {
      return builder().field(field).build();
    }
  }

  /** Selects a contiguous range of elements from a list. */
  @Value.Immutable
  interface ListSlice {
    int getStart();

    int getEnd();

    static ImmutableMaskExpression.ListSlice.Builder builder() {
      return ImmutableMaskExpression.ListSlice.builder();
    }

    static ListSlice of(int start, int end) {
      return builder().start(start).end(end).build();
    }
  }

  // ---------------------------------------------------------------------------
  // Map selection
  // ---------------------------------------------------------------------------

  /** Selects entries from a map type by exact key or key expression. */
  @Value.Immutable
  interface MapSelect extends Select {
    Optional<MapKey> getKey();

    Optional<MapKeyExpression> getExpression();

    /** Optional child selection applied to each selected map value. */
    Optional<Select> getChild();

    static ImmutableMaskExpression.MapSelect.Builder builder() {
      return ImmutableMaskExpression.MapSelect.builder();
    }

    static MapSelect ofKey(MapKey key) {
      return builder().key(key).build();
    }

    static MapSelect ofExpression(MapKeyExpression expression) {
      return builder().expression(expression).build();
    }

    @Override
    default <R, C extends VisitationContext, E extends Throwable> R accept(
        MaskExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Selects a map entry by an exact key match. */
  @Value.Immutable
  interface MapKey {
    String getMapKey();

    static ImmutableMaskExpression.MapKey.Builder builder() {
      return ImmutableMaskExpression.MapKey.builder();
    }

    static MapKey of(String mapKey) {
      return builder().mapKey(mapKey).build();
    }
  }

  /** Selects map entries by a wildcard key expression. */
  @Value.Immutable
  interface MapKeyExpression {
    String getMapKeyExpression();

    static ImmutableMaskExpression.MapKeyExpression.Builder builder() {
      return ImmutableMaskExpression.MapKeyExpression.builder();
    }

    static MapKeyExpression of(String mapKeyExpression) {
      return builder().mapKeyExpression(mapKeyExpression).build();
    }
  }
}
