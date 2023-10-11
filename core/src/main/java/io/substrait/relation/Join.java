package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.JoinRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Join extends BiRel implements HasExtension {

  public abstract Optional<Expression> getCondition();

  public abstract Optional<Expression> getPostJoinFilter();

  public abstract JoinType getJoinType();

  public static enum JoinType {
    UNKNOWN(JoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(JoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(JoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(JoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(JoinRel.JoinType.JOIN_TYPE_RIGHT),
    SEMI(JoinRel.JoinType.JOIN_TYPE_SEMI),
    ANTI(JoinRel.JoinType.JOIN_TYPE_ANTI);

    private JoinRel.JoinType proto;

    JoinType(JoinRel.JoinType proto) {
      this.proto = proto;
    }

    public JoinRel.JoinType toProto() {
      return proto;
    }

    public static JoinType fromProto(JoinRel.JoinType proto) {
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
          case RIGHT, OUTER -> getLeft().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          default -> getLeft().getRecordType().fields().stream();
        };
    Stream<Type> rightTypes =
        switch (getJoinType()) {
          case LEFT, OUTER -> getRight().getRecordType().fields().stream()
              .map(TypeCreator::asNullable);
          default -> getRight().getRecordType().fields().stream();
        };
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableJoin.Builder builder() {
    return ImmutableJoin.builder();
  }
}
