package io.substrait.relation;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * Represents an aggregate relation that groups input rows and computes aggregate functions.
 * Supports multiple grouping sets and measures.
 */
@Value.Immutable
public abstract class Aggregate extends SingleInputRel implements HasExtension {

  /**
   * Returns the list of grouping sets for this aggregate.
   *
   * @return list of grouping sets
   */
  public abstract List<Grouping> getGroupings();

  /**
   * Returns the list of aggregate measures to compute.
   *
   * @return list of measures
   */
  public abstract List<Measure> getMeasures();

  @Override
  protected Type.Struct deriveRecordType() {
    // If there's only one grouping set (or none), the nullability rule doesn't apply.
    if (getGroupings().size() <= 1) {
      final Stream<Type> groupingTypes =
          getGroupings().stream()
              .flatMap(g -> g.getExpressions().stream())
              .map(Expression::getType);
      final Stream<Type> measureTypes = getMeasures().stream().map(t -> t.getFunction().getType());
      return TypeCreator.REQUIRED.struct(Stream.concat(groupingTypes, measureTypes));
    }

    final LinkedHashSet<Expression> uniqueGroupingExpressions =
        getGroupings().stream()
            .flatMap(g -> g.getExpressions().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));

    // For each unique grouping expression, determine its final nullability based on the spec.
    final Stream<Type> groupingTypes =
        uniqueGroupingExpressions.stream()
            .map(
                expr -> {
                  // the code below implements the following statement from the spec
                  // (https://substrait.io/relations/logical_relations/#aggregate-operation):
                  // "The values for the grouping expression columns that are not
                  // part of the grouping set for a particular record will be set to null."
                  final boolean appearsInAllSets =
                      getGroupings().stream().allMatch(g -> g.getExpressions().contains(expr));
                  if (appearsInAllSets) {
                    return expr.getType();
                  } else {
                    return TypeCreator.asNullable(expr.getType());
                  }
                });

    final Stream<Type> measureTypes = getMeasures().stream().map(t -> t.getFunction().getType());

    // an aggregate relation with more than one grouping set receives an extra i32 column on the
    // right-hand side per spec:
    // https://substrait.io/relations/logical_relations/#aggregate-operation
    final Stream<Type> groupingSetIndex = Stream.of(TypeCreator.REQUIRED.I32);

    return TypeCreator.REQUIRED.struct(
        Stream.concat(Stream.concat(groupingTypes, measureTypes), groupingSetIndex));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /** Represents a grouping set - a set of expressions to group by. */
  @Value.Immutable
  public abstract static class Grouping {
    /**
     * Returns the list of expressions in this grouping set.
     *
     * @return list of grouping expressions
     */
    public abstract List<Expression> getExpressions();

    /**
     * Creates a new builder for constructing a Grouping.
     *
     * @return a new builder instance
     */
    public static ImmutableGrouping.Builder builder() {
      return ImmutableGrouping.builder();
    }
  }

  /** Represents an aggregate measure - an aggregate function to compute. */
  @Value.Immutable
  public abstract static class Measure {
    /**
     * Returns the aggregate function invocation for this measure.
     *
     * @return the aggregate function
     */
    public abstract AggregateFunctionInvocation getFunction();

    /**
     * Returns an optional filter to apply before computing the aggregate.
     *
     * @return the pre-measure filter, if present
     */
    public abstract Optional<Expression> getPreMeasureFilter();

    /**
     * Creates a new builder for constructing a Measure.
     *
     * @return a new builder instance
     */
    public static ImmutableMeasure.Builder builder() {
      return ImmutableMeasure.builder();
    }
  }

  /**
   * Creates a new builder for constructing an Aggregate relation.
   *
   * @return a new builder instance
   */
  public static ImmutableAggregate.Builder builder() {
    return ImmutableAggregate.builder();
  }
}
