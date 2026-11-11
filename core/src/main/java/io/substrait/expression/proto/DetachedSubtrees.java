package io.substrait.expression.proto;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Resolves expression and relation ordinals in one {@code PlanRel} or {@code ExtendedExpression}.
 * Each entry is converted in the schema and correlation scope of its reference. Only reachable
 * entries are validated. Each may be referenced once, including across expression and relation
 * subtrees.
 */
public final class DetachedSubtrees {
  private static final DetachedSubtrees NONE =
      new DetachedSubtrees(null, Collections.emptyList(), Collections.emptyList());

  private final Table<Expression> expressions;
  private final Table<Rel> relations;
  private final Message container;

  private DetachedSubtrees(Message container, List<Expression> expressions, List<Rel> relations) {
    this.container = container;
    this.expressions =
        new Table<>("expression", expressions, Expression::hasDetachedExpressionOrdinal);
    this.relations = new Table<>("relation", relations, Rel::hasDetachedRelOrdinal);
  }

  /**
   * Returns the resolver for standalone conversion, where every ordinal is invalid.
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
    return new DetachedSubtrees(
        planRel, planRel.getDetachedExpressionsList(), planRel.getDetachedRelsList());
  }

  /**
   * Creates a resolver over the detached subtrees of an {@code ExtendedExpression}.
   *
   * @param extendedExpression the container whose ordinals are resolved
   * @return a resolver for that container
   */
  public static DetachedSubtrees of(ExtendedExpression extendedExpression) {
    return new DetachedSubtrees(
        extendedExpression,
        extendedExpression.getDetachedExpressionsList(),
        extendedExpression.getDetachedRelsList());
  }

  /**
   * Returns the expression table prefix that an opaque payload may reference.
   *
   * @return retained expressions at their original ordinals
   */
  public List<Expression> unresolvedPrefix() {
    return expressions.unresolvedPrefix();
  }

  /**
   * Returns the relation table prefix that an opaque payload may reference.
   *
   * @return retained relations at their original ordinals
   */
  public List<Rel> unresolvedRelationPrefix() {
    return relations.unresolvedPrefix();
  }

  /**
   * Reports whether a reachable opaque payload could reference entries that conversion did not
   * inline. Custom extension converters can retain any such payload.
   *
   * @return whether the container needs to retain detached entries
   */
  public boolean needsRetainedEntries() {
    return (!unresolvedPrefix().isEmpty() || !unresolvedRelationPrefix().isEmpty())
        && containsAny(container);
  }

  private boolean containsAny(Message message) {
    if (message instanceof Any) {
      return true;
    }
    for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      if (field.isRepeated()) {
        int count = message.getRepeatedFieldCount(field);
        for (int i = 0; i < count; i++) {
          if (isReachable(message, field, i)
              && containsAny((Message) message.getRepeatedField(field, i))) {
            return true;
          }
        }
      } else if (message.hasField(field) && containsAny((Message) message.getField(field))) {
        return true;
      }
    }
    return false;
  }

  private boolean isReachable(Message message, FieldDescriptor field, int index) {
    if (message == container) {
      switch (field.getName()) {
        case "detached_expressions":
          return expressions.referenced.get(index);
        case "detached_rels":
          return relations.referenced.get(index);
        default:
          break;
      }
    }
    return true;
  }

  /**
   * Resolves a detached expression at its reference site.
   *
   * @param ordinal the unsigned expression ordinal
   * @param converter converts the target in the current scope
   * @param <T> the converted type
   * @return the converted expression
   */
  public <T> T resolveExpression(int ordinal, Function<Expression, T> converter) {
    return expressions.resolve(ordinal, converter);
  }

  /**
   * Resolves a detached relation at its reference site.
   *
   * @param ordinal the unsigned relation ordinal
   * @param converter converts the target in the current scope
   * @param <T> the converted type
   * @return the converted relation
   */
  public <T> T resolveRelation(int ordinal, Function<Rel, T> converter) {
    return relations.resolve(ordinal, converter);
  }

  private final class Table<M> {
    private final String kind;
    private final List<M> entries;
    private final Predicate<M> isOrdinal;
    private final BitSet referenced = new BitSet();
    private final BitSet resolving = new BitSet();

    Table(String kind, List<M> entries, Predicate<M> isOrdinal) {
      this.kind = kind;
      this.entries = entries;
      this.isOrdinal = isOrdinal;
    }

    List<M> unresolvedPrefix() {
      // Trimming referenced entries from the end keeps every possible payload target at its index.
      int last = referenced.previousClearBit(entries.size() - 1);
      return entries.subList(0, last + 1);
    }

    <T> T resolve(int ordinal, Function<M, T> converter) {
      long index = Integer.toUnsignedLong(ordinal);
      if (container == null) {
        throw new IllegalArgumentException(
            String.format(
                "Detached %s ordinal %d is outside a PlanRel or ExtendedExpression", kind, index));
      }
      if (index >= entries.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Detached %s ordinal %d is out of range for %d detached entries",
                kind, index, entries.size()));
      }
      if (resolving.get(ordinal)) {
        throw new IllegalArgumentException(
            String.format("Detached %s %d references itself through a cycle", kind, index));
      }
      if (referenced.get(ordinal)) {
        throw new IllegalArgumentException(
            String.format("Detached %s %d is referenced more than once", kind, index));
      }
      M entry = entries.get(ordinal);
      if (isOrdinal.test(entry)) {
        throw new IllegalArgumentException(
            String.format("Detached %s %d is itself a detached %s ordinal", kind, index, kind));
      }
      referenced.set(ordinal);
      resolving.set(ordinal);
      try {
        return converter.apply(entry);
      } finally {
        resolving.clear(ordinal);
      }
    }
  }
}
