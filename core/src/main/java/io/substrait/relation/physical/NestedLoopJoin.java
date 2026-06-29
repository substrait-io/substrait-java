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

/** A physical join relation that evaluates the condition for every pair of left and right rows. */
@Value.Immutable
public abstract class NestedLoopJoin extends BiRel implements HasExtension {

  /**
   * Returns the condition evaluated against each pair of left and right rows.
   *
   * @return the join condition
   */
  public abstract Expression getCondition();

  /**
   * Returns the type of join to perform.
   *
   * @return the join type
   */
  public abstract JoinType getJoinType();

  /** The kinds of join supported by a {@link NestedLoopJoin} relation. */
  public enum JoinType {
    /** Unspecified or unknown join type. */
    UNKNOWN(NestedLoopJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    /** Inner join: only matching left/right row pairs. */
    INNER(NestedLoopJoinRel.JoinType.JOIN_TYPE_INNER),
    /** Full outer join: all rows from both sides, with non-matches padded with nulls. */
    OUTER(NestedLoopJoinRel.JoinType.JOIN_TYPE_OUTER),
    /** Left outer join: all left rows, with non-matching right columns padded with nulls. */
    LEFT(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT),
    /** Right outer join: all right rows, with non-matching left columns padded with nulls. */
    RIGHT(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT),
    /** Left semi join: left rows that have at least one match on the right. */
    LEFT_SEMI(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    /** Right semi join: right rows that have at least one match on the left. */
    RIGHT_SEMI(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    /** Left anti join: left rows that have no match on the right. */
    LEFT_ANTI(NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    /** Right anti join: right rows that have no match on the left. */
    RIGHT_ANTI(NestedLoopJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private NestedLoopJoinRel.JoinType proto;

    JoinType(NestedLoopJoinRel.JoinType proto) {
      this.proto = proto;
    }

    /**
     * Returns the protobuf representation of this join type.
     *
     * @return the proto join type
     */
    public NestedLoopJoinRel.JoinType toProto() {
      return proto;
    }

    /**
     * Returns the {@link JoinType} matching the given protobuf join type.
     *
     * @param proto the proto join type
     * @return the matching join type
     * @throws IllegalArgumentException if the type is not recognized
     */
    public static JoinType fromProto(NestedLoopJoinRel.JoinType proto) {
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
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link NestedLoopJoin}.
   *
   * @return a new builder
   */
  public static ImmutableNestedLoopJoin.Builder builder() {
    return ImmutableNestedLoopJoin.builder();
  }
}
