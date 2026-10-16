package io.substrait.expression.proto;

import io.substrait.proto.Expression.RexTypeCase;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.PlanRel;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * The detached subtrees of one {@code PlanRel} or {@code ExtendedExpression}, resolving the {@code
 * Expression.detached_expression_ordinal} references in it against its {@code detached_expressions}
 * (spec v0.105.0).
 *
 * <p>Detachment is an encoding of the expression tree, so an ordinal is resolved by converting its
 * entry in place of the reference: the entry binds to the input schema and correlation scope of the
 * referencing site. Resolution enforces the rules the spec places on the portion of the table
 * reachable from the container: an ordinal is in range, refers to an entry that is not itself an
 * ordinal, is referenced at most once, and is not part of a cycle. Entries that nothing references
 * are neither converted nor validated.
 *
 * <p>An instance records which entries it has resolved, so it serves one conversion of one
 * container. The converted model inlines every resolved entry and keeps no table. The spec forbids
 * a consumer that retains an opaque {@code google.protobuf.Any} payload from dropping the entries
 * that payload's ordinals refer to, so a converter must not retain such a payload uninterpreted
 * when it holds ordinals.
 */
public final class DetachedSubtrees {
  private static final DetachedSubtrees NONE = new DetachedSubtrees(Collections.emptyList(), false);

  private final List<io.substrait.proto.Expression> entries;
  private final boolean inContainer;
  private final BitSet referenced = new BitSet();
  private final BitSet resolving = new BitSet();

  private DetachedSubtrees(List<io.substrait.proto.Expression> entries, boolean inContainer) {
    this.entries = entries;
    this.inContainer = inContainer;
  }

  /**
   * Returns the resolver for expressions converted outside any {@code PlanRel} or {@code
   * ExtendedExpression}, where every detached expression ordinal is invalid.
   *
   * @return a resolver that rejects every ordinal
   */
  public static DetachedSubtrees none() {
    return NONE;
  }

  /**
   * Creates a resolver over the detached subtrees of a {@code PlanRel}.
   *
   * @param planRel the container whose ordinals are resolved
   * @return a resolver for that container
   */
  public static DetachedSubtrees of(PlanRel planRel) {
    return new DetachedSubtrees(planRel.getDetachedExpressionsList(), true);
  }

  /**
   * Creates a resolver over the detached subtrees of an {@code ExtendedExpression}.
   *
   * @param extendedExpression the container whose ordinals are resolved
   * @return a resolver for that container
   */
  public static DetachedSubtrees of(ExtendedExpression extendedExpression) {
    return new DetachedSubtrees(extendedExpression.getDetachedExpressionsList(), true);
  }

  /**
   * Returns the {@code detached_expressions} entries a retained opaque payload may still refer to:
   * every entry up to the last one this resolver has not resolved. A resolved entry is referenced
   * only from the tree that inlined it, so dropping the trailing run of resolved entries removes no
   * payload's target and renumbers nothing.
   *
   * @return the leading entries to retain, indexed by ordinal
   */
  public List<io.substrait.proto.Expression> unresolvedPrefix() {
    int end = entries.size();
    while (end > 0 && referenced.get(end - 1)) {
      end--;
    }
    return entries.subList(0, end);
  }

  /**
   * Converts the {@code detached_expressions} entry an ordinal refers to.
   *
   * @param ordinal the {@code detached_expression_ordinal}, an unsigned 32-bit value
   * @param converter converts the entry, resolving any ordinals within it through this resolver
   * @param <T> the converted type
   * @return the converted entry
   * @throws IllegalArgumentException if the reference breaks one of the spec's rules
   */
  public <T> T resolveExpression(
      int ordinal, Function<io.substrait.proto.Expression, T> converter) {
    long index = Integer.toUnsignedLong(ordinal);
    if (!inContainer) {
      throw new IllegalArgumentException(
          String.format(
              "Detached expression ordinal %d is outside a PlanRel or ExtendedExpression", index));
    }
    if (index >= entries.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Detached expression ordinal %d is out of range for %d detached expressions",
              index, entries.size()));
    }
    int i = (int) index;
    if (resolving.get(i)) {
      throw new IllegalArgumentException(
          String.format("Detached expression %d references itself through a cycle", i));
    }
    if (referenced.get(i)) {
      throw new IllegalArgumentException(
          String.format("Detached expression %d is referenced more than once", i));
    }
    io.substrait.proto.Expression entry = entries.get(i);
    if (entry.getRexTypeCase() == RexTypeCase.DETACHED_EXPRESSION_ORDINAL) {
      throw new IllegalArgumentException(
          String.format("Detached expression %d is itself a detached expression ordinal", i));
    }
    referenced.set(i);
    resolving.set(i);
    try {
      return converter.apply(entry);
    } finally {
      resolving.clear(i);
    }
  }
}
