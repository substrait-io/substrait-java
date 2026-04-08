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

  /**
   * Creates a new builder for constructing a MaskExpression.
   *
   * @return a new builder instance
   */
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
    /**
     * Accepts a visitor to process this select node.
     *
     * @param <R> the return type of the visitor
     * @param <C> the context type
     * @param <E> the exception type that may be thrown
     * @param visitor the visitor to accept
     * @param context the visitation context
     * @return the result of the visitation
     * @throws E if an error occurs during visitation
     */
    <R, C extends VisitationContext, E extends Throwable> R accept(
        MaskExpressionVisitor<R, C, E> visitor, C context) throws E;
  }

  // ---------------------------------------------------------------------------
  // Struct selection
  // ---------------------------------------------------------------------------

  /** Selects a subset of fields from a struct type. */
  @Value.Immutable
  interface StructSelect extends Select {
    /**
     * Returns the list of struct items being selected.
     *
     * @return the list of struct items
     */
    List<StructItem> getStructItems();

    /**
     * Creates a new builder for constructing a StructSelect.
     *
     * @return a new builder instance
     */
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

    /**
     * Creates a new builder for constructing a StructItem.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.StructItem.Builder builder() {
      return ImmutableMaskExpression.StructItem.builder();
    }

    /**
     * Creates a StructItem for a single field with no nested selection.
     *
     * @param field the zero-based field index within the struct
     * @return a new StructItem instance
     */
    static StructItem of(int field) {
      return builder().field(field).build();
    }

    /**
     * Creates a StructItem for a single field with an optional nested selection.
     *
     * @param field the zero-based field index within the struct
     * @param child the nested child selection for complex types
     * @return a new StructItem instance
     */
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
    /**
     * Returns the list of selection items (individual elements or slices).
     *
     * @return the list of selection items
     */
    List<ListSelectItem> getSelection();

    /** Optional child selection applied to each selected element. */
    Optional<Select> getChild();

    /**
     * Creates a new builder for constructing a ListSelect.
     *
     * @return a new builder instance
     */
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
    /**
     * Returns the optional list element selection.
     *
     * @return the optional list element
     */
    Optional<ListElement> getItem();

    /**
     * Returns the optional list slice selection.
     *
     * @return the optional list slice
     */
    Optional<ListSlice> getSlice();

    /**
     * Creates a new builder for constructing a ListSelectItem.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.ListSelectItem.Builder builder() {
      return ImmutableMaskExpression.ListSelectItem.builder();
    }

    /**
     * Creates a ListSelectItem for a single element selection.
     *
     * @param element the list element to select
     * @return a new ListSelectItem instance
     */
    static ListSelectItem ofItem(ListElement element) {
      return builder().item(element).build();
    }

    /**
     * Creates a ListSelectItem for a slice selection.
     *
     * @param slice the list slice to select
     * @return a new ListSelectItem instance
     */
    static ListSelectItem ofSlice(ListSlice slice) {
      return builder().slice(slice).build();
    }
  }

  /** Selects a single element from a list by zero-based index. */
  @Value.Immutable
  interface ListElement {
    /**
     * Returns the zero-based element index within the list.
     *
     * @return the element index
     */
    int getField();

    /**
     * Creates a new builder for constructing a ListElement.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.ListElement.Builder builder() {
      return ImmutableMaskExpression.ListElement.builder();
    }

    /**
     * Creates a ListElement for a single element selection.
     *
     * @param field the zero-based element index within the list
     * @return a new ListElement instance
     */
    static ListElement of(int field) {
      return builder().field(field).build();
    }
  }

  /** Selects a contiguous range of elements from a list. */
  @Value.Immutable
  interface ListSlice {
    /**
     * Returns the zero-based start index of the slice (inclusive).
     *
     * @return the start index
     */
    int getStart();

    /**
     * Returns the zero-based end index of the slice (exclusive).
     *
     * @return the end index
     */
    int getEnd();

    /**
     * Creates a new builder for constructing a ListSlice.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.ListSlice.Builder builder() {
      return ImmutableMaskExpression.ListSlice.builder();
    }

    /**
     * Creates a ListSlice for a contiguous range of elements.
     *
     * @param start the zero-based start index (inclusive)
     * @param end the zero-based end index (exclusive)
     * @return a new ListSlice instance
     */
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
    /**
     * Returns the optional exact key for map selection.
     *
     * @return the optional map key
     */
    Optional<MapKey> getKey();

    /**
     * Returns the optional key expression for wildcard map selection.
     *
     * @return the optional map key expression
     */
    Optional<MapKeyExpression> getExpression();

    /** Optional child selection applied to each selected map value. */
    Optional<Select> getChild();

    /**
     * Creates a new builder for constructing a MapSelect.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.MapSelect.Builder builder() {
      return ImmutableMaskExpression.MapSelect.builder();
    }

    /**
     * Creates a MapSelect for a single key selection.
     *
     * @param key the exact key to select
     * @return a new MapSelect instance
     */
    static MapSelect ofKey(MapKey key) {
      return builder().key(key).build();
    }

    /**
     * Creates a MapSelect for a wildcard key expression selection.
     *
     * @param expression the key expression to select
     * @return a new MapSelect instance
     */
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
    /**
     * Returns the map key string for exact matching.
     *
     * @return the map key
     */
    String getMapKey();

    /**
     * Creates a new builder for constructing a MapKey.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.MapKey.Builder builder() {
      return ImmutableMaskExpression.MapKey.builder();
    }

    /**
     * Creates a MapKey for exact key matching.
     *
     * @param mapKey the key string to match
     * @return a new MapKey instance
     */
    static MapKey of(String mapKey) {
      return builder().mapKey(mapKey).build();
    }
  }

  /** Selects map entries by a wildcard key expression. */
  @Value.Immutable
  interface MapKeyExpression {
    /**
     * Returns the wildcard key expression string.
     *
     * @return the map key expression
     */
    String getMapKeyExpression();

    /**
     * Creates a new builder for constructing a MapKeyExpression.
     *
     * @return a new builder instance
     */
    static ImmutableMaskExpression.MapKeyExpression.Builder builder() {
      return ImmutableMaskExpression.MapKeyExpression.builder();
    }

    /**
     * Creates a MapKeyExpression for wildcard key matching.
     *
     * @param mapKeyExpression the wildcard expression string
     * @return a new MapKeyExpression instance
     */
    static MapKeyExpression of(String mapKeyExpression) {
      return builder().mapKeyExpression(mapKeyExpression).build();
    }
  }
}
