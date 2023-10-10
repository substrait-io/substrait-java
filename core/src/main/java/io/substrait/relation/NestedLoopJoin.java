package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class NestedLoopJoin extends BiRel implements HasExtension {

  public abstract Optional<Expression> getCondition();

  public abstract JoinType getJoinType();

  public static enum JoinType {
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

    JoinType(NestedLoopJoinRel.JoinType proto) {
      this.proto = proto;
    }

    public NestedLoopJoinRel.JoinType toProto() {
      return proto;
    }

    public static JoinType fromProto(NestedLoopJoinRel.JoinType proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    Stream<Type> leftTypes =
        switch (getJoinType()) {
          case RIGHT, RIGHT_SEMI, RIGHT_ANTI, OUTER -> getLeft().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          default -> getLeft().getRecordType().fields().stream();
        };
    Stream<Type> rightTypes =
        switch (getJoinType()) {
          case LEFT, LEFT_SEMI, LEFT_ANTI, OUTER -> getRight().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          default -> getRight().getRecordType().fields().stream();
        };
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableNestedLoopJoin.Builder builder() {
    return ImmutableNestedLoopJoin.builder();
  }
}
