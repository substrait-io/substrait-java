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

/** A physical join relation that matches rows by equality of hashed key columns. */
@Value.Immutable
public abstract class HashJoin extends BiRel implements HasExtension {

  /**
   * Returns the comparison join keys matching left and right key fields.
   *
   * @return the join keys
   */
  public abstract List<ComparisonJoinKey> getKeys();

  /**
   * Returns the left-side key fields used to build the hash table.
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
   * Returns the right-side key fields probed against the hash table.
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

  /** The kinds of join supported by a {@link HashJoin} relation. */
  public enum JoinType {
    /** Unspecified or unknown join type. */
    UNKNOWN(HashJoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    /** Inner join: only matching left/right row pairs. */
    INNER(HashJoinRel.JoinType.JOIN_TYPE_INNER),
    /** Full outer join: all rows from both sides, with non-matches padded with nulls. */
    OUTER(HashJoinRel.JoinType.JOIN_TYPE_OUTER),
    /** Left outer join: all left rows, with non-matching right columns padded with nulls. */
    LEFT(HashJoinRel.JoinType.JOIN_TYPE_LEFT),
    /** Right outer join: all right rows, with non-matching left columns padded with nulls. */
    RIGHT(HashJoinRel.JoinType.JOIN_TYPE_RIGHT),
    /** Left semi join: left rows that have at least one match on the right. */
    LEFT_SEMI(HashJoinRel.JoinType.JOIN_TYPE_LEFT_SEMI),
    /** Right semi join: right rows that have at least one match on the left. */
    RIGHT_SEMI(HashJoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI),
    /** Left anti join: left rows that have no match on the right. */
    LEFT_ANTI(HashJoinRel.JoinType.JOIN_TYPE_LEFT_ANTI),
    /** Right anti join: right rows that have no match on the left. */
    RIGHT_ANTI(HashJoinRel.JoinType.JOIN_TYPE_RIGHT_ANTI);

    private HashJoinRel.JoinType proto;

    JoinType(HashJoinRel.JoinType proto) {
      this.proto = proto;
    }

    /**
     * Returns the {@link JoinType} matching the given protobuf join type.
     *
     * @param proto the proto join type
     * @return the matching join type
     * @throws IllegalArgumentException if the type is not recognized
     */
    public static JoinType fromProto(HashJoinRel.JoinType proto) {
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
    public HashJoinRel.JoinType toProto() {
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
   * Creates a builder for {@link HashJoin}.
   *
   * @return a new builder
   */
  public static ImmutableHashJoin.Builder builder() {
    return ImmutableHashJoin.builder();
  }
}
