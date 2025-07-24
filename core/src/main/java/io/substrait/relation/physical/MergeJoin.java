package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.proto.MergeJoinRel;
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
public abstract class MergeJoin extends BiRel implements HasExtension {

  public abstract List<FieldReference> getLeftKeys();

  public abstract List<FieldReference> getRightKeys();

  public abstract JoinType getJoinType();

  public abstract Optional<Expression> getPostJoinFilter();

  public enum JoinType {
    UNKNOWN(MergeJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(MergeJoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(MergeJoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(MergeJoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT),
    LEFT_SEMI(MergeJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    RIGHT_SEMI(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    LEFT_ANTI(MergeJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    RIGHT_ANTI(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private MergeJoinRel.JoinType proto;

    JoinType(MergeJoinRel.JoinType proto) {
      this.proto = proto;
    }

    public static JoinType fromProto(MergeJoinRel.JoinType proto) {
      for (JoinType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }

    public MergeJoinRel.JoinType toProto() {
      return proto;
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    Stream<Type> leftTypes = getLeftTypes();
    Stream<Type> rightTypes = getRightTypes();
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  private Stream<Type> getLeftTypes() {
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
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableMergeJoin.Builder builder() {
    return ImmutableMergeJoin.builder();
  }
}
