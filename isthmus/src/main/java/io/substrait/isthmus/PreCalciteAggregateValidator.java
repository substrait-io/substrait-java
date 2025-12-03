package io.substrait.isthmus;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Project;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Not all Substrait {@link Aggregate} rels are convertable to {@link
 * org.apache.calcite.rel.core.Aggregate} rels
 *
 * <p>The code in this class can:
 *
 * <ul>
 *   <li>Check for these cases
 *   <li>Rewrite the Substrait {@link Aggregate} such that it can be converted to Calcite
 * </ul>
 */
public class PreCalciteAggregateValidator {

  /**
   * Checks that the given {@link Aggregate} is valid for use in Calcite
   *
   * @param aggregate
   * @return
   */
  public static boolean isValidCalciteAggregate(final Aggregate aggregate) {
    return aggregate.getMeasures().stream()
            .allMatch(PreCalciteAggregateValidator::isValidCalciteMeasure)
        && aggregate.getGroupings().stream()
            .allMatch(PreCalciteAggregateValidator::isValidCalciteGrouping);
  }

  /**
   * Checks that all expressions present in the given {@link Aggregate.Measure} are {@link
   * FieldReference}s, as Calcite expects all expressions in {@link
   * org.apache.calcite.rel.core.Aggregate}s to be field references.
   *
   * @return true if the {@code measure} can be converted to a Calcite equivalent without changes,
   *     false otherwise.
   */
  private static boolean isValidCalciteMeasure(final Aggregate.Measure measure) {
    return
    // all function arguments to measures must be field references
    measure.getFunction().arguments().stream().allMatch(farg -> isSimpleFieldReference(farg))
        &&
        // all sort fields must be field references
        measure.getFunction().sort().stream().allMatch(sf -> isSimpleFieldReference(sf.expr()))
        &&
        // pre-measure filter must be a field reference
        measure.getPreMeasureFilter().map(f -> isSimpleFieldReference(f)).orElse(true);
  }

  /**
   * Checks that all expressions present in the given {@link Aggregate.Grouping} are {@link
   * FieldReference}s, as Calcite expects all expressions in {@link
   * org.apache.calcite.rel.core.Aggregate}s to be field references.
   *
   * <p>Additionally, checks that all grouping fields are specified in ascending order.
   *
   * @return true if the {@code grouping} can be converted to a Calcite equivalent without changes,
   *     false otherwise.
   */
  private static boolean isValidCalciteGrouping(final Aggregate.Grouping grouping) {
    if (!grouping.getExpressions().stream().allMatch(e -> isSimpleFieldReference(e))) {
      // all grouping expressions must be field references
      return false;
    }

    // Calcite stores grouping fields in an ImmutableBitSet and does not track the order of the
    // grouping fields. The output record shape that Calcite generates ALWAYS has the groupings in
    // ascending field order. This causes issues with Substrait in cases where the grouping fields
    // in Substrait are not defined in ascending order.

    // For example, if a grouping is defined as (0, 2, 1) in Substrait, Calcite will output it as
    // (0, 1, 2), which means that the Calcite output will no longer line up with the expectations
    // of the Substrait plan.
    final List<Integer> groupingFields =
        grouping.getExpressions().stream()
            // isSimpleFieldReference above guarantees that the expr is a FieldReference
            .map(expr -> getFieldRefOffset((FieldReference) expr))
            .collect(Collectors.toList());

    return isOrdered(groupingFields);
  }

  private static boolean isSimpleFieldReference(final FunctionArg e) {
    if (!(e instanceof FieldReference)) {
      return false;
    }

    final List<FieldReference.ReferenceSegment> segments = ((FieldReference) e).segments();
    return segments.size() == 1 && segments.get(0) instanceof FieldReference.StructField;
  }

  private static int getFieldRefOffset(final FieldReference fr) {
    return ((FieldReference.StructField) fr.segments().get(0)).offset();
  }

  private static boolean isOrdered(final List<Integer> list) {
    for (int i = 1; i < list.size(); i++) {
      if (list.get(i - 1) > list.get(i)) {
        return false;
      }
    }
    return true;
  }

  public static class PreCalciteAggregateTransformer {

    // New expressions to include in the project before the aggregate
    private final List<Expression> newExpressions;

    // Tracks the offset of the next expression added
    private int expressionOffset;

    private PreCalciteAggregateTransformer(final Aggregate aggregate) {
      this.newExpressions = new ArrayList<>();
      // The Substrait project output includes all input fields, followed by expressions
      this.expressionOffset = aggregate.getInput().getRecordType().fields().size();
    }

    /**
     * Transforms an {@link Aggregate} that cannot be handled by Calcite into an equivalent that can
     * be handled by:
     *
     * <ul>
     *   <li>Moving all non-field references into a project before the aggregation
     *   <li>Adding all groupings to this project so that they are referenced in "order"
     * </ul>
     */
    public static Aggregate transformToValidCalciteAggregate(final Aggregate aggregate) {
      final PreCalciteAggregateTransformer at = new PreCalciteAggregateTransformer(aggregate);

      final List<Aggregate.Measure> newMeasures =
          aggregate.getMeasures().stream().map(at::updateMeasure).collect(Collectors.toList());
      final List<Aggregate.Grouping> newGroupings =
          aggregate.getGroupings().stream().map(at::updateGrouping).collect(Collectors.toList());

      final Project preAggregateProject =
          Project.builder().input(aggregate.getInput()).expressions(at.newExpressions).build();

      return Aggregate.builder()
          .from(aggregate)
          .input(preAggregateProject)
          .measures(newMeasures)
          .groupings(newGroupings)
          .build();
    }

    private Aggregate.Measure updateMeasure(final Aggregate.Measure measure) {
      final AggregateFunctionInvocation oldAggregateFunctionInvocation = measure.getFunction();

      final List<Expression> newFunctionArgs =
          oldAggregateFunctionInvocation.arguments().stream()
              .map(this::projectOutNonFieldReference)
              .collect(Collectors.toList());

      final List<Expression.SortField> newSortFields =
          oldAggregateFunctionInvocation.sort().stream()
              .map(
                  sf ->
                      Expression.SortField.builder()
                          .from(sf)
                          .expr(projectOutNonFieldReference(sf.expr()))
                          .build())
              .collect(Collectors.toList());

      final Optional<Expression> newPreMeasureFilter =
          measure.getPreMeasureFilter().map(this::projectOutNonFieldReference);

      final AggregateFunctionInvocation newAggregateFunctionInvocation =
          AggregateFunctionInvocation.builder()
              .from(oldAggregateFunctionInvocation)
              .arguments(newFunctionArgs)
              .sort(newSortFields)
              .build();

      return Aggregate.Measure.builder()
          .function(newAggregateFunctionInvocation)
          .preMeasureFilter(newPreMeasureFilter)
          .build();
    }

    private Aggregate.Grouping updateGrouping(final Aggregate.Grouping grouping) {
      // project out all groupings unconditionally, even field references
      // this ensures that out of order groupings are re-projected into in order groupings
      final List<Expression> newGroupingExpressions =
          grouping.getExpressions().stream().map(this::projectOut).collect(Collectors.toList());
      return Aggregate.Grouping.builder().expressions(newGroupingExpressions).build();
    }

    private Expression projectOutNonFieldReference(final FunctionArg farg) {
      if ((farg instanceof Expression)) {
        return projectOutNonFieldReference((Expression) farg);
      } else {
        throw new IllegalArgumentException("cannot handle non-expression argument for aggregate");
      }
    }

    private Expression projectOutNonFieldReference(final Expression expr) {
      if (isSimpleFieldReference(expr)) {
        return expr;
      }
      return projectOut(expr);
    }

    /**
     * Adds a new expression to the project at {@link
     * PreCalciteAggregateTransformer#expressionOffset} and returns a field reference to the new
     * expression
     */
    private Expression projectOut(final Expression expr) {
      newExpressions.add(expr);
      return FieldReference.builder()
          // create a field reference to the new expression, then update the expression offset
          .addSegments(FieldReference.StructField.of(expressionOffset++))
          .type(expr.getType())
          .build();
    }
  }
}
