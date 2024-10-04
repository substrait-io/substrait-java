package io.substrait.relation;

import io.substrait.proto.SetRel;
import io.substrait.type.Type;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Set extends AbstractRel implements HasExtension {
  public abstract Set.SetOp getSetOp();

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

    public static Set.SetOp fromProto(SetRel.SetOp proto) {
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
    return getInputs().get(0).getRecordType();
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableSet.Builder builder() {
    return ImmutableSet.builder();
  }
}
