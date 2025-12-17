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
 * Validates and rewrites Substrait {@link Aggregate} relations for compatibility with Calcite
 * {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Check if an {@link Aggregate} can be converted directly to Calcite
 *   <li>Rewrite invalid aggregates into a form acceptable by Calcite
 * </ul>
 */
public class PreCalciteAggregateValidator {

  /**
   * Checks whether the given {@link Aggregate} is valid for Calcite conversion.
   *
   * @param aggregate the Substrait aggregate relation
   * @return {@code true} if valid for Calcite, {@code false} otherwise
   */
  public static boolean isValidCalciteAggregate(Aggregate aggregate) {
    return aggregate.getMeasures().stream()
            .allMatch(PreCalciteAggregateValidator::isValidCalciteMeasure)
        && aggregate.getGroupings().stream()
            .allMatch(PreCalciteAggregateValidator::isValidCalciteGrouping);
  }

  /**
   * Checks if an {@link Aggregate.Measure} uses only {@link FieldReference}s for arguments, sort
   * fields, and pre-measure filter.
   *
   * @param measure the aggregate measure to validate
   * @return {@code true} if valid, {@code false} otherwise
   */
  private static boolean isValidCalciteMeasure(Aggregate.Measure measure) {
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
   * Checks if an {@link Aggregate.Grouping} uses only {@link FieldReference}s and ensures grouping
   * fields are in ascending order.
   *
   * @param grouping the aggregate grouping to validate
   * @return {@code true} if valid, {@code false} otherwise
   */
  private static boolean isValidCalciteGrouping(Aggregate.Grouping grouping) {
    if (!grouping.getExpressions().stream().allMatch(e -> isSimpleFieldReference(e))) {
      return false;
    }

    List<Integer> groupingFields =
        grouping.getExpressions().stream()
            .map(expr -> getFieldRefOffset((FieldReference) expr))
            .collect(Collectors.toList());

    return isOrdered(groupingFields);
  }

  private static boolean isSimpleFieldReference(FunctionArg e) {
    if (!(e instanceof FieldReference)) {
      return false;
    }

    List<FieldReference.ReferenceSegment> segments = ((FieldReference) e).segments();
    return segments.size() == 1 && segments.get(0) instanceof FieldReference.StructField;
  }

  private static int getFieldRefOffset(FieldReference fr) {
    return ((FieldReference.StructField) fr.segments().get(0)).offset();
  }

  private static boolean isOrdered(List<Integer> list) {
    for (int i = 1; i < list.size(); i++) {
      if (list.get(i - 1) > list.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Transforms invalid aggregates into Calcite-compatible form by projecting non-field expressions
   * and reordering groupings.
   */
  public static class PreCalciteAggregateTransformer {

    // New expressions to include in the project before the aggregate
    private final List<Expression> newExpressions;

    // Tracks the offset of the next expression added
    private int expressionOffset;

    private PreCalciteAggregateTransformer(Aggregate aggregate) {
      this.newExpressions = new ArrayList<>();
      this.expressionOffset = aggregate.getInput().getRecordType().fields().size();
    }

    /**
     * Rewrites an {@link Aggregate} so that it can be converted to Calcite by:
     *
     * <ul>
     *   <li>Projecting non-field references before aggregation
     *   <li>Ensuring groupings are in ascending order
     * </ul>
     *
     * @param aggregate the original Substrait aggregate
     * @return a transformed Calcite-compatible aggregate
     */
    public static Aggregate transformToValidCalciteAggregate(Aggregate aggregate) {
      PreCalciteAggregateTransformer at = new PreCalciteAggregateTransformer(aggregate);

      List<Aggregate.Measure> newMeasures =
          aggregate.getMeasures().stream().map(at::updateMeasure).collect(Collectors.toList());
      List<Aggregate.Grouping> newGroupings =
          aggregate.getGroupings().stream().map(at::updateGrouping).collect(Collectors.toList());

      Project preAggregateProject =
          Project.builder().input(aggregate.getInput()).expressions(at.newExpressions).build();

      return Aggregate.builder()
          .from(aggregate)
          .input(preAggregateProject)
          .measures(newMeasures)
          .groupings(newGroupings)
          .build();
    }

    private Aggregate.Measure updateMeasure(Aggregate.Measure measure) {
      AggregateFunctionInvocation oldAggregateFunctionInvocation = measure.getFunction();

      List<Expression> newFunctionArgs =
          oldAggregateFunctionInvocation.arguments().stream()
              .map(this::projectOutNonFieldReference)
              .collect(Collectors.toList());

      List<Expression.SortField> newSortFields =
          oldAggregateFunctionInvocation.sort().stream()
              .map(
                  sf ->
                      Expression.SortField.builder()
                          .from(sf)
                          .expr(projectOutNonFieldReference(sf.expr()))
                          .build())
              .collect(Collectors.toList());

      Optional<Expression> newPreMeasureFilter =
          measure.getPreMeasureFilter().map(this::projectOutNonFieldReference);

      AggregateFunctionInvocation newAggregateFunctionInvocation =
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

    private Aggregate.Grouping updateGrouping(Aggregate.Grouping grouping) {
      List<Expression> newGroupingExpressions =
          grouping.getExpressions().stream().map(this::projectOut).collect(Collectors.toList());
      return Aggregate.Grouping.builder().expressions(newGroupingExpressions).build();
    }

    private Expression projectOutNonFieldReference(FunctionArg farg) {
      if ((farg instanceof Expression)) {
        return projectOutNonFieldReference((Expression) farg);
      } else {
        throw new IllegalArgumentException("cannot handle non-expression argument for aggregate");
      }
    }

    private Expression projectOutNonFieldReference(Expression expr) {
      if (isSimpleFieldReference(expr)) {
        return expr;
      }
      return projectOut(expr);
    }

    /**
     * Adds a new expression to the pre-aggregate project and returns a field reference pointing to
     * it.
     *
     * @param expr the expression to project out
     * @return a {@link FieldReference} to the projected expression
     */
    private Expression projectOut(Expression expr) {
      newExpressions.add(expr);
      return FieldReference.builder()
          .addSegments(FieldReference.StructField.of(expressionOffset++))
          .type(expr.getType())
          .build();
    }
  }
}
