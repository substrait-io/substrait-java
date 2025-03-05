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
      case INTERSECTION_PRIMARY -> coalesceNullability(first, rest, true);
      case INTERSECTION_MULTISET,
          INTERSECTION_MULTISET_ALL,
          UNION_DISTINCT,
          UNION_ALL -> coalesceNullability(first, rest, false);
    };
  }

  private Type.Struct coalesceNullability(
      Type.Struct first, List<Type.Struct> rest, boolean prioritizeFirst) {
    List<Type> fields = new ArrayList<>();
    for (int i = 0; i < first.fields().size(); i++) {
      Type typeA = first.fields().get(i);
      int finalI = i;
      boolean anyOtherIsNullable = rest.stream().anyMatch(t -> t.fields().get(finalI).nullable());
      if (prioritizeFirst && !anyOtherIsNullable) {
        // For INTERSECTION_PRIMARY: if no other field is nullable, type shouldn't be nullable
        fields.add(TypeCreator.asNotNullable(typeA));
      } else if (!prioritizeFirst && anyOtherIsNullable) {
        // For other INTERSECTIONs and UNIONs: if any other field is nullable, type should be
        // nullable
        fields.add(TypeCreator.asNullable(typeA));
      } else {
        // Can keep nullability as-is
        fields.add(typeA);
      }
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
