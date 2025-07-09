package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.JoinRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Join extends BiRel implements HasExtension {

  public abstract Optional<Expression> getCondition();

  public abstract Optional<Expression> getPostJoinFilter();

  public abstract JoinType getJoinType();

  public enum JoinType {
    UNKNOWN(JoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(JoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(JoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(JoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(JoinRel.JoinType.JOIN_TYPE_RIGHT),
    LEFT_SEMI(JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    LEFT_ANTI(JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    LEFT_SINGLE(JoinRel.JoinType.JOIN_TYPE_LEFT_SINGLE),
    RIGHT_SEMI(JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    RIGHT_ANTI(JoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI),
    RIGHT_SINGLE(JoinRel.JoinType.JOIN_TYPE_RIGHT_SINGLE),
    LEFT_MARK(JoinRel.JoinType.JOIN_TYPE_LEFT_MARK),
    RIGHT_MARK(JoinRel.JoinType.JOIN_TYPE_RIGHT_MARK),
    // deprecated values last to not get them looked up first in fromProto()
    /** use {@link #LEFT_SEMI} instead */
    @Deprecated
    SEMI(JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    /** use {@link #LEFT_ANTI} instead */
    @Deprecated
    ANTI(JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI);

    private JoinRel.JoinType proto;

    JoinType(JoinRel.JoinType proto) {
      this.proto = proto;
    }

    public JoinRel.JoinType toProto() {
      return proto;
    }

    public static JoinType fromProto(JoinRel.JoinType proto) {
      for (JoinType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
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
      case RIGHT:
      case OUTER:
      case RIGHT_SINGLE:
        return getLeft().getRecordType().fields().stream().map(TypeCreator::asNullable);
      case RIGHT_SEMI:
      case RIGHT_ANTI:
        return Stream.of(); // these are right joins which ignore left side columns
      case RIGHT_MARK:
        return Stream.of(
            TypeCreator.REQUIRED
                .BOOLEAN); // right mark join keeps all fields from right and adds a boolean mark
        // field
      default:
        return getLeft().getRecordType().fields().stream();
    }
  }

  private Stream<Type> getRightTypes() {
    switch (getJoinType()) {
      case LEFT:
      case OUTER:
      case LEFT_SINGLE:
        return getRight().getRecordType().fields().stream().map(TypeCreator::asNullable);
      case SEMI:
      case ANTI:
      case LEFT_SEMI:
      case LEFT_ANTI:
        return Stream.of(); // these are left joins which ignore right side columns
      case LEFT_MARK:
        return Stream.of(
            TypeCreator.REQUIRED
                .BOOLEAN); // left mark join keeps all fields from left and adds a boolean mark
        // field
      default:
        return getRight().getRecordType().fields().stream();
    }
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableJoin.Builder builder() {
    return ImmutableJoin.builder();
  }
}
