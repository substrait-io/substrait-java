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

public interface Rel {
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

  Optional<Hint> getHint();

  @Value.Immutable
  abstract class Remap {
    public abstract List<Integer> indices();

    public Type.Struct remap(Type.Struct initial) {
      List<Type> types = initial.fields();
      return TypeCreator.of(initial.nullable()).struct(indices().stream().map(i -> types.get(i)));
    }

    public static Remap of(Iterable<Integer> fields) {
      return ImmutableRemap.builder().addAllIndices(fields).build();
    }

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
