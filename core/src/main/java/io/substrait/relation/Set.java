package io.substrait.relation;

import io.substrait.proto.SetRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Set extends AbstractRel implements HasExtension {
  public abstract SetOp getSetOp();

  public static enum SetOp {
    UNKNOWN(SetRel.SetOp.SET_OP_UNSPECIFIED),
    MINUS_PRIMARY(SetRel.SetOp.SET_OP_MINUS_PRIMARY),
    MINUS_PRIMARY_ALL(SetRel.SetOp.SET_OP_MINUS_PRIMARY_ALL),
    MINUS_MULTISET(SetRel.SetOp.SET_OP_MINUS_MULTISET),
    INTERSECTION_PRIMARY(SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY),
    INTERSECTION_MULTISET(SetRel.SetOp.SET_OP_INTERSECTION_MULTISET),
    INTERSECTION_MULTISET_ALL(SetRel.SetOp.SET_OP_INTERSECTION_MULTISET_ALL),
    UNION_DISTINCT(SetRel.SetOp.SET_OP_UNION_DISTINCT),
    UNION_ALL(SetRel.SetOp.SET_OP_UNION_ALL);

    private SetRel.SetOp proto;

    SetOp(SetRel.SetOp proto) {
      this.proto = proto;
    }

    public SetRel.SetOp toProto() {
      return proto;
    }

    public static SetOp fromProto(SetRel.SetOp proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown operation: " + proto);
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    // We intentionally don't validate that the types match to maintain backwards
    // compatibility. We should, but then we'll need to handle things like VARCHAR
    // vs FIXEDCHAR (comes up in Isthmus tests). We also don't recurse into nullability
    // of the inner fields, in case the type itself is a struct or list or map.

    List<Type.Struct> inputRecordTypes =
        getInputs().stream().map(Rel::getRecordType).collect(Collectors.toList());
    if (inputRecordTypes.isEmpty()) {
      throw new IllegalArgumentException("Set operation must have at least one input");
    }
    Type.Struct first = inputRecordTypes.get(0);
    List<Type.Struct> rest = inputRecordTypes.subList(1, inputRecordTypes.size());

    int numFields = first.fields().size();
    if (rest.stream().anyMatch(t -> t.fields().size() != numFields)) {
      throw new IllegalArgumentException("Set's input records have different number of fields");
    }

    // As defined in https://substrait.io/relations/logical_relations/#set-operation-types
    return switch (getSetOp()) {
      case UNKNOWN -> first; // alternative would be to throw an exception
      case MINUS_PRIMARY, MINUS_PRIMARY_ALL, MINUS_MULTISET -> first;
      case INTERSECTION_PRIMARY -> coalesceNullabilityIntersectionPrimary(first, rest);
      case INTERSECTION_MULTISET, INTERSECTION_MULTISET_ALL -> coalesceNullabilityIntersection(
          first, rest);
      case UNION_DISTINCT, UNION_ALL -> coalesceNullabilityUnion(first, rest);
    };
  }

  /** If field is nullable in any of the inputs, it's nullable in the output */
  private Type.Struct coalesceNullabilityUnion(Type.Struct first, List<Type.Struct> rest) {

    List<Type> fields = new ArrayList<>();
    for (int i = 0; i < first.fields().size(); i++) {
      Type typeA = first.fields().get(i);
      int finalI = i;
      fields.add(
          rest.stream()
              .map(struct -> struct.fields().get(finalI))
              .reduce(
                  typeA, (curr, next) -> next.nullable() ? TypeCreator.asNullable(curr) : curr));
    }
    return Type.Struct.builder().fields(fields).nullable(first.nullable()).build();
  }

  /**
   * If field is nullable in primary input and in any of the other of the inputs, it's nullable in
   * the output
   */
  private Type.Struct coalesceNullabilityIntersectionPrimary(
      Type.Struct first, List<Type.Struct> rest) {

    List<Type> fields = new ArrayList<>();
    for (int i = 0; i < first.fields().size(); i++) {
      Type typeA = first.fields().get(i);
      if (!typeA.nullable()) {
        // Just to make this case explicit and to short-circuit, logic below would work without too
        fields.add(typeA);
        continue;
      }
      int finalI = i;
      boolean anyOtherIsNullable = rest.stream().anyMatch(t -> t.fields().get(finalI).nullable());
      fields.add(anyOtherIsNullable ? typeA : TypeCreator.asNotNullable(typeA));
    }
    return Type.Struct.builder().fields(fields).nullable(first.nullable()).build();
  }

  /** If field is required in any of the inputs, it's required in the output */
  private Type.Struct coalesceNullabilityIntersection(Type.Struct first, List<Type.Struct> rest) {
    List<Type> fields = new ArrayList<>();
    for (int i = 0; i < first.fields().size(); i++) {
      Type typeA = first.fields().get(i);
      if (!typeA.nullable()) {
        // Just to make this case explicit and to short-circuit, logic below would work without too
        fields.add(typeA);
        continue;
      }
      int finalI = i;
      boolean anyOtherIsRequired = rest.stream().anyMatch(t -> !t.fields().get(finalI).nullable());
      fields.add(anyOtherIsRequired ? TypeCreator.asNotNullable(typeA) : typeA);
    }
    return Type.Struct.builder().fields(fields).nullable(first.nullable()).build();
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableSet.Builder builder() {
    return ImmutableSet.builder();
  }
}
