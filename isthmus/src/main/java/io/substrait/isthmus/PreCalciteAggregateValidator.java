package io.substrait.isthmus;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Project;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
            .allMatch(PreCalciteAggregateValidator::isValidCalciteGrouping)
        && aLoneGroupingSetNamesEachExpressionOnce(aggregate);
  }

  /**
   * Checks that an aggregate holding one grouping set does not name an expression in it twice.
   *
   * <p>Calcite holds a grouping set in an {@link org.apache.calcite.util.ImmutableBitSet}, which
   * cannot hold a field twice, while a lone grouping set gives each mention a column of its own --
   * {@code Aggregate.deriveRecordType} dedups the grouping expressions only across several sets. So
   * a repeat has to reach Calcite as two columns of the relation underneath, which is what the
   * transformer makes of it.
   *
   * @param aggregate the aggregate relation
   * @return {@code true} if valid, {@code false} otherwise
   */
  private static boolean aLoneGroupingSetNamesEachExpressionOnce(Aggregate aggregate) {
    if (aggregate.getGroupings().size() != 1) {
      return true;
    }
    List<Expression> expressions = aggregate.getGroupings().get(0).getExpressions();
    return new HashSet<>(expressions).size() == expressions.size();
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
    // all value (Expression) function arguments to measures must be field references; non-value
    // arguments such as the std_dev/variance "distribution" enum argument are exempt
    measure.getFunction().arguments().stream()
            .filter(farg -> farg instanceof Expression)
            .allMatch(farg -> isSimpleFieldReference(farg))
        &&
        // all sort fields must be field references
        measure.getFunction().sort().stream().allMatch(sf -> isSimpleFieldReference(sf.expr()))
        &&
        // pre-measure filter must be a field reference
        measure.getPreMeasureFilter().map(f -> isSimpleFieldReference(f)).orElse(true);
  }

  /**
   * Checks if an {@link Aggregate.Grouping} uses only {@link FieldReference}s.
   *
   * <p>The order the fields are grouped in is not a reason to rewrite the aggregate. Calcite holds
   * a grouping set in an {@link org.apache.calcite.util.ImmutableBitSet} and emits its grouping
   * columns in ascending field order whatever order they were declared in, so a plan grouping on
   * (0, 2, 1) reaches Calcite as (0, 1, 2); the conversion carries the declared order in the emit
   * mapping instead.
   *
   * @param grouping the aggregate grouping to validate
   * @return {@code true} if valid, {@code false} otherwise
   */
  private static boolean isValidCalciteGrouping(Aggregate.Grouping grouping) {
    return grouping.getExpressions().stream().allMatch(e -> isSimpleFieldReference(e));
  }

  private static boolean isSimpleFieldReference(FunctionArg e) {
    if (!(e instanceof FieldReference)) {
      return false;
    }

    List<FieldReference.ReferenceSegment> segments = ((FieldReference) e).segments();
    return segments.size() == 1 && segments.get(0) instanceof FieldReference.StructField;
  }

  /**
   * Transforms invalid aggregates into Calcite-compatible form by projecting out the grouping
   * expressions Calcite cannot hold as they are.
   */
  public static class PreCalciteAggregateTransformer {

    // New expressions to include in the project before the aggregate
    private final List<Expression> newExpressions;

    // The field reference each grouping expression was projected out to, kept only where the
    // aggregate's own record type shares a column between mentions of one expression: with several
    // grouping sets a field grouped on by two of them is one column of the output, so it has to
    // stay one column of the project underneath -- two copies of it would each be missing from a
    // set, and Calcite would make both nullable. A lone grouping set gives every mention a column
    // of its own, so there the map is not consulted.
    private final Map<Expression, Expression> projectedGroupingExpressions;

    private final boolean groupingColumnsAreShared;

    // Tracks the offset of the next expression added
    private int expressionOffset;

    private PreCalciteAggregateTransformer(Aggregate aggregate) {
      this.newExpressions = new ArrayList<>();
      this.projectedGroupingExpressions = new HashMap<>();
      this.groupingColumnsAreShared = aggregate.getGroupings().size() > 1;
      this.expressionOffset = aggregate.getInput().getRecordType().fields().size();
    }

    /**
     * Rewrites an {@link Aggregate} so that it can be converted to Calcite by projecting the
     * grouping expressions and the measures' non-field arguments out before the aggregation, so
     * that each is a field reference of its own.
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

      List<FunctionArg> newFunctionArgs =
          oldAggregateFunctionInvocation.arguments().stream()
              .map(this::projectOutNonFieldReferenceArg)
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
          grouping.getExpressions().stream()
              .map(
                  expr ->
                      groupingColumnsAreShared
                          ? projectedGroupingExpressions.computeIfAbsent(expr, this::projectOut)
                          : projectOut(expr))
              .collect(Collectors.toList());
      return Aggregate.Grouping.builder().expressions(newGroupingExpressions).build();
    }

    private FunctionArg projectOutNonFieldReferenceArg(FunctionArg farg) {
      if ((farg instanceof Expression)) {
        return projectOutNonFieldReference((Expression) farg);
      } else {
        // Non-value arguments (e.g. the std_dev/variance "distribution" enum argument) are not
        // field references and are passed through unchanged.
        return farg;
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
