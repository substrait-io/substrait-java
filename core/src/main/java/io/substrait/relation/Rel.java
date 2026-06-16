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
