package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.relation.BiRel;
import io.substrait.relation.HasExtension;
import io.substrait.relation.RelVisitor;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class NestedLoopJoin extends BiRel implements HasExtension {

  public abstract Expression getCondition();

  public abstract JoinType getJoinType();

  public enum JoinType {
    UNKNOWN(NestedLoopJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(NestedLoopJoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(NestedLoopJoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT),
    LEFT_SEMI(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    RIGHT_SEMI(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    LEFT_ANTI(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    RIGHT_ANTI(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private NestedLoopJoinRel.JoinType proto;

    JoinType(final NestedLoopJoinRel.JoinType proto) {
      this.proto = proto;
    }

    public NestedLoopJoinRel.JoinType toProto() {
      return proto;
    }

    public static JoinType fromProto(final NestedLoopJoinRel.JoinType proto) {
      for (final JoinType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    final Stream<Type> leftTypes = getLeftTypes();
    final Stream<Type> rightTypes = getRightTypes();
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  private Stream<Type> getLeftTypes() {
    switch (getJoinType()) {
      case RIGHT:
      case OUTER:
        return getLeft().getRecordType().fields().stream().map(TypeCreator::asNullable);
      case RIGHT_ANTI:
      case RIGHT_SEMI:
        return Stream.empty();
      default:
        return getLeft().getRecordType().fields().stream();
    }
  }

  private Stream<Type> getRightTypes() {
    switch (getJoinType()) {
      case LEFT:
      case OUTER:
        return getRight().getRecordType().fields().stream().map(TypeCreator::asNullable);
      case LEFT_ANTI:
      case LEFT_SEMI:
        return Stream.empty();
      default:
        return getRight().getRecordType().fields().stream();
    }
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableNestedLoopJoin.Builder builder() {
    return ImmutableNestedLoopJoin.builder();
  }
}
