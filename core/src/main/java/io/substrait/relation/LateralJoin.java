package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A binary lateral join relation. Semantically identical to a {@link Join}, except the right input
 * is evaluated once per row of the left input and may reference fields of the current left row via
 * an outer reference to this relation's {@link #getRelAnchor() rel anchor}.
 *
 * <p>Only inner and left-oriented {@link Join.JoinType join types} are valid for lateral joins:
 * {@code INNER}, {@code LEFT}, {@code LEFT_SEMI}, {@code LEFT_ANTI}, {@code LEFT_SINGLE}, and
 * {@code LEFT_MARK}.
 */
@Value.Immutable
public abstract class LateralJoin extends BiRel implements HasExtension {

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
  public abstract Join.JoinType getJoinType();

  /**
   * Validates the lateral-join-specific invariants: the join type must be inner or left-oriented,
   * and a rel anchor must be set so the right input can reference the current left row.
   */
  @Value.Check
  protected void check() {
    switch (getJoinType()) {
      case INNER:
      case LEFT:
      case LEFT_SEMI:
      case LEFT_ANTI:
      case LEFT_SINGLE:
      case LEFT_MARK:
        break;
      default:
        throw new IllegalArgumentException(
            "Lateral join only supports INNER and left-oriented join types "
                + "(INNER, LEFT, LEFT_SEMI, LEFT_ANTI, LEFT_SINGLE, LEFT_MARK); got "
                + getJoinType());
    }
    if (!getRelAnchor().isPresent()) {
      throw new IllegalArgumentException(
          "Lateral join must set a rel anchor so its right input can reference the current "
              + "left row via an outer reference");
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    return Join.deriveRecordType(getJoinType(), getLeft(), getRight());
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link LateralJoin}.
   *
   * @return a new builder
   */
  public static ImmutableLateralJoin.Builder builder() {
    return ImmutableLateralJoin.builder();
  }
}
