package io.substrait.relation;

import io.substrait.proto.SetRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * A relation that combines its inputs using a set operation such as union, intersection or minus.
 */
@Value.Immutable
public abstract class Set extends AbstractRel implements HasExtension {
  /**
   * Returns the set operation applied to the inputs.
   *
   * @return the set operation
   */
  public abstract SetOp getSetOp();

  /** The kinds of set operations supported by a {@link Set} relation. */
  public enum SetOp {
    /** Unspecified or unknown set operation. */
    UNKNOWN(SetRel.SetOp.SET_OP_UNSPECIFIED),
    /** Rows of the primary input not present in any other input, with duplicates removed. */
    MINUS_PRIMARY(SetRel.SetOp.SET_OP_MINUS_PRIMARY),
    /** Rows of the primary input not present in any other input, retaining duplicates. */
    MINUS_PRIMARY_ALL(SetRel.SetOp.SET_OP_MINUS_PRIMARY_ALL),
    /** Multiset difference of the primary input against the other inputs. */
    MINUS_MULTISET(SetRel.SetOp.SET_OP_MINUS_MULTISET),
    /** Rows present in the primary input and at least one other input, with duplicates removed. */
    INTERSECTION_PRIMARY(SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY),
    /** Rows present in all inputs, with duplicates removed. */
    INTERSECTION_MULTISET(SetRel.SetOp.SET_OP_INTERSECTION_MULTISET),
    /** Rows present in all inputs, retaining duplicates. */
    INTERSECTION_MULTISET_ALL(SetRel.SetOp.SET_OP_INTERSECTION_MULTISET_ALL),
    /** Union of all inputs with duplicates removed. */
    UNION_DISTINCT(SetRel.SetOp.SET_OP_UNION_DISTINCT),
    /** Union of all inputs retaining duplicates. */
    UNION_ALL(SetRel.SetOp.SET_OP_UNION_ALL);

    private SetRel.SetOp proto;

    SetOp(SetRel.SetOp proto) {
      this.proto = proto;
    }

    /**
     * Returns the protobuf representation of this set operation.
     *
     * @return the proto set operation
     */
    public SetRel.SetOp toProto() {
      return proto;
    }

    /**
     * Returns the {@link SetOp} matching the given protobuf set operation.
     *
     * @param proto the proto set operation
     * @return the matching set operation
     * @throws IllegalArgumentException if the operation is not recognized
     */
    public static SetOp fromProto(SetRel.SetOp proto) {
      for (SetOp v : values()) {
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
    switch (getSetOp()) {
      case UNKNOWN:
        return first; // alternative would be to throw an exception
      case MINUS_PRIMARY:
      case MINUS_PRIMARY_ALL:
      case MINUS_MULTISET:
        return first;
      case INTERSECTION_PRIMARY:
        return coalesceNullabilityIntersectionPrimary(first, rest);
      case INTERSECTION_MULTISET:
      case INTERSECTION_MULTISET_ALL:
        return coalesceNullabilityIntersection(first, rest);
      case UNION_DISTINCT:
      case UNION_ALL:
        return coalesceNullabilityUnion(first, rest);
      default:
        throw new UnsupportedOperationException("Unexpected set operation: " + getSetOp());
    }
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
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Set}.
   *
   * @return a new builder
   */
  public static ImmutableSet.Builder builder() {
    return ImmutableSet.builder();
  }
}
