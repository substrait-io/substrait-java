package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.proto.HashJoinRel;
import io.substrait.relation.BiRel;
import io.substrait.relation.HasExtension;
import io.substrait.relation.RelVisitor;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class HashJoin extends BiRel implements HasExtension {

  public abstract List<FieldReference> getLeftKeys();

  public abstract List<FieldReference> getRightKeys();

  public abstract JoinType getJoinType();

  public abstract Optional<Expression> getPostJoinFilter();

  public static enum JoinType {
    UNKNOWN(HashJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(HashJoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(HashJoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(HashJoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(HashJoinRel.JoinType.JOIN_TYPE_RIGHT),
    LEFT_SEMI(HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    RIGHT_SEMI(HashJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    LEFT_ANTI(HashJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    RIGHT_ANTI(HashJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private HashJoinRel.JoinType proto;

    JoinType(HashJoinRel.JoinType proto) {
      this.proto = proto;
    }

    public static JoinType fromProto(HashJoinRel.JoinType proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }

    public HashJoinRel.JoinType toProto() {
      return proto;
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    Stream<Type> leftTypes =
        switch (getJoinType()) {
          case RIGHT, OUTER -> getLeft().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          case RIGHT_ANTI, RIGHT_SEMI -> Stream.empty();
          default -> getLeft().getRecordType().fields().stream();
        };
    Stream<Type> rightTypes =
        switch (getJoinType()) {
          case LEFT, OUTER -> getRight().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          case LEFT_ANTI, LEFT_SEMI -> Stream.empty();
          default -> getRight().getRecordType().fields().stream();
        };
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableHashJoin.Builder builder() {
    return ImmutableHashJoin.builder();
  }
}
