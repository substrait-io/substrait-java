package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.JoinRel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

/** A binary join relation combining a left and right input according to a {@link JoinType}. */
@Value.Immutable
public abstract class Join extends BiRel implements HasExtension {

  /**
   * Returns the join condition evaluated against pairs of left and right rows, if any.
   *
   * @return the optional join condition
   */
  public abstract Optional<Expression> getCondition();

  /**
   * Returns the filter applied to the join output after the join is performed, if any.
   *
   * @return the optional post-join filter
   */
  public abstract Optional<Expression> getPostJoinFilter();

  /**
   * Returns the type of join to perform.
   *
   * @return the join type
   */
  public abstract JoinType getJoinType();

  /** The kinds of join supported by a {@link Join} relation. */
  public enum JoinType {
    /** Unspecified or unknown join type. */
    UNKNOWN(JoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    /** Inner join: only matching left/right row pairs. */
    INNER(JoinRel.JoinType.JOIN_TYPE_INNER),
    /** Full outer join: all rows from both sides, with non-matches padded with nulls. */
    OUTER(JoinRel.JoinType.JOIN_TYPE_OUTER),
    /** Left outer join: all left rows, with non-matching right columns padded with nulls. */
    LEFT(JoinRel.JoinType.JOIN_TYPE_LEFT),
    /** Right outer join: all right rows, with non-matching left columns padded with nulls. */
    RIGHT(JoinRel.JoinType.JOIN_TYPE_RIGHT),
    /** Left semi join: left rows that have at least one match on the right. */
    LEFT_SEMI(JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    /** Left anti join: left rows that have no match on the right. */
    LEFT_ANTI(JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    /** Left single join: each left row paired with at most one matching right row. */
    LEFT_SINGLE(JoinRel.JoinType.JOIN_TYPE_LEFT_SINGLE),
    /** Right semi join: right rows that have at least one match on the left. */
    RIGHT_SEMI(JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    /** Right anti join: right rows that have no match on the left. */
    RIGHT_ANTI(JoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI),
    /** Right single join: each right row paired with at most one matching left row. */
    RIGHT_SINGLE(JoinRel.JoinType.JOIN_TYPE_RIGHT_SINGLE),
    /** Left mark join: left rows with an appended boolean column marking whether a match exists. */
    LEFT_MARK(JoinRel.JoinType.JOIN_TYPE_LEFT_MARK),
    /**
     * Right mark join: right rows with an appended boolean column marking whether a match exists.
     */
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

    /**
     * Returns the protobuf representation of this join type.
     *
     * @return the proto join type
     */
    public JoinRel.JoinType toProto() {
      return proto;
    }

    /**
     * Returns the {@link JoinType} matching the given protobuf join type.
     *
     * @param proto the proto join type
     * @return the matching join type
     * @throws IllegalArgumentException if the type is not recognized
     */
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
    Stream<Type> markType = getMarkType();
    return TypeCreator.REQUIRED.struct(Stream.of(leftTypes, rightTypes, markType).flatMap(s -> s));
  }

  private Stream<Type> getLeftTypes() {
    switch (getJoinType()) {
      case RIGHT:
      case OUTER:
      case RIGHT_SINGLE:
        return getLeft().getRecordType().fields().stream().map(TypeCreator::asNullable);
      case RIGHT_SEMI:
      case RIGHT_ANTI:
      case RIGHT_MARK:
        // these joins ignore left side columns
        return Stream.of();
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
      case LEFT_MARK:
        // these joins ignore right side columns
        return Stream.of();
      default:
        return getRight().getRecordType().fields().stream();
    }
  }

  private Stream<Type> getMarkType() {
    // Mark joins append a nullable boolean "mark" column at the end of the
    // emitted side. The column is nullable because the match state is 3-valued:
    //  - true  : at least one partner matched
    //  - false : no partner, and no NULL-producing comparisons
    //  - NULL  : no partner, but some comparison produced NULL
    switch (getJoinType()) {
      case LEFT_MARK:
      case RIGHT_MARK:
        return Stream.of(TypeCreator.NULLABLE.BOOLEAN);
      default:
        return Stream.of();
    }
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Join}.
   *
   * @return a new builder
   */
  public static ImmutableJoin.Builder builder() {
    return ImmutableJoin.builder();
  }
}
