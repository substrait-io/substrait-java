package io.substrait.relation;

import io.substrait.extension.AdvancedExtension;
import io.substrait.hint.Hint;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.immutables.value.Value;

/** Base type for all Substrait relational operators. */
public interface Rel {
  /**
   * Returns the output field remapping applied to this relation, if any.
   *
   * @return the optional output mapping
   */
  Optional<Remap> getRemap();

  /**
   * @return the {@link AdvancedExtension} associated with a {@link io.substrait.proto.RelCommon}
   *     message, if present
   */
  Optional<AdvancedExtension> getCommonExtension();

  /**
   * Returns the plan-wide unique anchor identifying this relation, if set.
   *
   * <p>This corresponds to {@link io.substrait.proto.RelCommon#getRelAnchor()} and is required when
   * this relation is the binding point for an id-based outer reference (see {@link
   * io.substrait.expression.FieldReference#outerReferenceRelReference()}). When set it must be
   * unique across all relations within a plan and {@code >= 1}.
   *
   * @return the optional relation anchor
   */
  Optional<Integer> getRelAnchor();

  /**
   * Returns a copy of this relation with its {@link #getRelAnchor() relation anchor} set to the
   * given value.
   *
   * <p>Overridden by the generated Immutables {@code withRelAnchor(int)} on every concrete
   * relation, this provides a type-agnostic way to stamp an anchor onto an arbitrary relation (used
   * by {@link OuterReferenceConverter} when assigning anchors during {@code steps_out →
   * rel_reference} conversion). Custom {@link Rel} implementations that are not Immutables-backed
   * inherit this throwing default.
   *
   * @param relAnchor the plan-wide unique anchor to set (must be {@code >= 1})
   * @return a copy of this relation carrying the given anchor
   */
  default Rel withRelAnchor(int relAnchor) {
    throw new UnsupportedOperationException(
        getClass() + " does not support setting a relation anchor");
  }

  /**
   * Returns a copy of this relation with its {@link #getRelAnchor() relation anchor} set to the
   * given optional value ({@link Optional#empty()} clears it).
   *
   * <p>Like {@link #withRelAnchor(int)}, this is overridden by the generated Immutables {@code
   * withRelAnchor(Optional)} and provides a type-agnostic way to set or clear the anchor (used by
   * {@link OuterReferenceConverter} when stripping anchors during {@code rel_reference → steps_out}
   * conversion). Custom {@link Rel} implementations that are not Immutables-backed inherit this
   * throwing default.
   *
   * @param relAnchor the anchor to set, or empty to clear it
   * @return a copy of this relation carrying the given anchor
   */
  default Rel withRelAnchor(Optional<Integer> relAnchor) {
    throw new UnsupportedOperationException(
        getClass() + " does not support setting a relation anchor");
  }

  /**
   * Returns the record type (schema) produced by this relation.
   *
   * @return the struct type representing the output schema
   */
  Type.Struct getRecordType();

  /**
   * Returns the input relations for this relation.
   *
   * @return list of input relations (empty for leaf relations)
   */
  List<Rel> getInputs();

  /**
   * Returns the hint associated with this relation, if any.
   *
   * @return the optional hint
   */
  Optional<Hint> getHint();

  /**
   * Reorders and/or selects a relation's output fields by index, producing the emitted record type.
   */
  @Value.Immutable
  abstract class Remap {
    /**
     * Returns the output field indices, in emission order.
     *
     * @return the field indices
     */
    public abstract List<Integer> indices();

    /**
     * Applies this remap to the given record type, selecting and reordering fields by index.
     *
     * @param initial the record type to remap
     * @return the remapped record type
     */
    public Type.Struct remap(Type.Struct initial) {
      List<Type> types = initial.fields();
      return TypeCreator.of(initial.nullable()).struct(indices().stream().map(i -> types.get(i)));
    }

    /**
     * Creates a remap that emits the given field indices in order.
     *
     * @param fields the field indices to emit
     * @return the remap
     */
    public static Remap of(Iterable<Integer> fields) {
      return ImmutableRemap.builder().addAllIndices(fields).build();
    }

    /**
     * Creates a remap that emits a contiguous range of field indices.
     *
     * @param start the first field index to emit
     * @param length the number of consecutive fields to emit
     * @return the remap
     */
    public static Remap offset(int start, int length) {
      return of(
          IntStream.range(start, start + length)
              .mapToObj(i -> i)
              .collect(java.util.stream.Collectors.toList()));
    }
  }

  /**
   * Accepts a visitor for this relation.
   *
   * @param <O> the return type
   * @param <C> the visitation context type
   * @param <E> the exception type that may be thrown
   * @param visitor the visitor
   * @param context the visitation context
   * @return the result of the visit
   * @throws E if the visit fails
   */
  <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E;
}
