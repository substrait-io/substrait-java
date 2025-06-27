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

  Type.Struct getRecordType();

  List<Rel> getInputs();

  Optional<Hint> getHint();

  @Value.Immutable
  public abstract static class Remap {
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

  <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E;
}
