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

/** A physical join relation that matches rows by merging two inputs sorted on their key columns. */
@Value.Immutable
public abstract class MergeJoin extends BiRel implements HasExtension {

  /**
   * Returns the comparison join keys matching left and right key fields.
   *
   * @return the join keys
   */
  public abstract List<ComparisonJoinKey> getKeys();

  /**
   * Returns the left-side key fields the left input is sorted on.
   *
   * @return the left join keys
   * @deprecated the left-hand sides of the join {@link #getKeys()}; use {@link #getKeys()} instead.
   */
  @Deprecated
  public List<FieldReference> getLeftKeys() {
    return getKeys().stream()
        .map(ComparisonJoinKey::getLeft)
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Returns the right-side key fields the right input is sorted on.
   *
   * @return the right join keys
   * @deprecated the right-hand sides of the join {@link #getKeys()}; use {@link #getKeys()}
   *     instead.
   */
  @Deprecated
  public List<FieldReference> getRightKeys() {
    return getKeys().stream()
        .map(ComparisonJoinKey::getRight)
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Returns the type of join to perform.
   *
   * @return the join type
   */
  public abstract JoinType getJoinType();

  /**
   * Returns the filter applied to the join output after the join is performed, if any.
   *
   * @return the optional post-join filter
   */
  public abstract Optional<Expression> getPostJoinFilter();

  /**
   * Returns the residual filter evaluated on each candidate key-match, if any. A candidate is only
   * considered a match when every {@link #getKeys() key} comparison and this expression evaluate to
   * true.
   *
   * @return the optional residual filter expression
   */
  public abstract Optional<Expression> getResidualExpression();

  /** The kinds of join supported by a {@link MergeJoin} relation. */
  public enum JoinType {
    /** Unspecified or unknown join type. */
    UNKNOWN(MergeJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    /** Inner join: only matching left/right row pairs. */
    INNER(MergeJoinRel.JoinType.JOIN_TYPE_INNER),
    /** Full outer join: all rows from both sides, with non-matches padded with nulls. */
    OUTER(MergeJoinRel.JoinType.JOIN_TYPE_OUTER),
    /** Left outer join: all left rows, with non-matching right columns padded with nulls. */
    LEFT(MergeJoinRel.JoinType.JOIN_TYPE_LEFT),
    /** Right outer join: all right rows, with non-matching left columns padded with nulls. */
    RIGHT(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT),
    /** Left semi join: left rows that have at least one match on the right. */
    LEFT_SEMI(MergeJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    /** Right semi join: right rows that have at least one match on the left. */
    RIGHT_SEMI(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    /** Left anti join: left rows that have no match on the right. */
    LEFT_ANTI(MergeJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    /** Right anti join: right rows that have no match on the left. */
    RIGHT_ANTI(MergeJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private MergeJoinRel.JoinType proto;

    JoinType(MergeJoinRel.JoinType proto) {
      this.proto = proto;
    }

    /**
     * Returns the {@link JoinType} matching the given protobuf join type.
     *
     * @param proto the proto join type
     * @return the matching join type
     * @throws IllegalArgumentException if the type is not recognized
     */
    public static JoinType fromProto(MergeJoinRel.JoinType proto) {
      for (JoinType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }

    /**
     * Returns the protobuf representation of this join type.
     *
     * @return the proto join type
     */
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

  /**
   * Creates a builder for {@link MergeJoin}.
   *
   * @return a new builder
   */
  public static ImmutableMergeJoin.Builder builder() {
    return ImmutableMergeJoin.builder();
  }
}
