package io.substrait.dsl;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.Cast;
import io.substrait.expression.Expression.FailureBehavior;
import io.substrait.expression.Expression.IfClause;
import io.substrait.expression.Expression.IfThen;
import io.substrait.expression.Expression.PredicateOp;
import io.substrait.expression.Expression.SingleOrList;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.Expression.SwitchClause;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ToTypeString;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Aggregate.Measure;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Expand;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubstraitBuilder {
  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  private final SimpleExtension.ExtensionCollection extensions;

  public SubstraitBuilder(final SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
  }

  // Relations
  public Aggregate.Measure measure(final AggregateFunctionInvocation aggFn) {
    return Aggregate.Measure.builder().function(aggFn).build();
  }

  public Aggregate.Measure measure(
      final AggregateFunctionInvocation aggFn, final Expression preMeasureFilter) {
    return Aggregate.Measure.builder().function(aggFn).preMeasureFilter(preMeasureFilter).build();
  }

  public Aggregate aggregate(
      final Function<Rel, Aggregate.Grouping> groupingFn,
      final Function<Rel, List<Aggregate.Measure>> measuresFn,
      final Rel input) {
    final Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.empty(), input);
  }

  public Aggregate aggregate(
      final Function<Rel, Aggregate.Grouping> groupingFn,
      final Function<Rel, List<Aggregate.Measure>> measuresFn,
      final Rel.Remap remap,
      final Rel input) {
    final Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.of(remap), input);
  }

  private Aggregate aggregate(
      final Function<Rel, List<Aggregate.Grouping>> groupingsFn,
      final Function<Rel, List<Aggregate.Measure>> measuresFn,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    final List<Aggregate.Grouping> groupings = groupingsFn.apply(input);
    final List<Aggregate.Measure> measures = measuresFn.apply(input);
    return Aggregate.builder()
        .groupings(groupings)
        .measures(measures)
        .remap(remap)
        .input(input)
        .build();
  }

  public Cross cross(final Rel left, final Rel right) {
    return cross(left, right, Optional.empty());
  }

  public Cross cross(final Rel left, final Rel right, final Rel.Remap remap) {
    return cross(left, right, Optional.of(remap));
  }

  private Cross cross(final Rel left, final Rel right, final Optional<Rel.Remap> remap) {
    return Cross.builder().left(left).right(right).remap(remap).build();
  }

  public Fetch fetch(final long offset, final long count, final Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.empty(), input);
  }

  public Fetch fetch(final long offset, final long count, final Rel.Remap remap, final Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.of(remap), input);
  }

  public Fetch limit(final long limit, final Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.empty(), input);
  }

  public Fetch limit(final long limit, final Rel.Remap remap, final Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.of(remap), input);
  }

  public Fetch offset(final long offset, final Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.empty(), input);
  }

  public Fetch offset(final long offset, final Rel.Remap remap, final Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.of(remap), input);
  }

  private Fetch fetch(
      final long offset,
      final OptionalLong count,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    return Fetch.builder().offset(offset).count(count).input(input).remap(remap).build();
  }

  public Filter filter(final Function<Rel, Expression> conditionFn, final Rel input) {
    return filter(conditionFn, Optional.empty(), input);
  }

  public Filter filter(
      final Function<Rel, Expression> conditionFn, final Rel.Remap remap, final Rel input) {
    return filter(conditionFn, Optional.of(remap), input);
  }

  private Filter filter(
      final Function<Rel, Expression> conditionFn,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    final Expression condition = conditionFn.apply(input);
    return Filter.builder().input(input).condition(condition).remap(remap).build();
  }

  public static final class JoinInput {
    private final Rel left;
    private final Rel right;

    JoinInput(final Rel left, final Rel right) {
      this.left = left;
      this.right = right;
    }

    public Rel left() {
      return left;
    }

    public Rel right() {
      return right;
    }
  }

  public Join innerJoin(
      final Function<JoinInput, Expression> conditionFn, final Rel left, final Rel right) {
    return join(conditionFn, Join.JoinType.INNER, left, right);
  }

  public Join innerJoin(
      final Function<JoinInput, Expression> conditionFn,
      final Rel.Remap remap,
      final Rel left,
      final Rel right) {
    return join(conditionFn, Join.JoinType.INNER, remap, left, right);
  }

  public Join join(
      final Function<JoinInput, Expression> conditionFn,
      final Join.JoinType joinType,
      final Rel left,
      final Rel right) {
    return join(conditionFn, joinType, Optional.empty(), left, right);
  }

  public Join join(
      final Function<JoinInput, Expression> conditionFn,
      final Join.JoinType joinType,
      final Rel.Remap remap,
      final Rel left,
      final Rel right) {
    return join(conditionFn, joinType, Optional.of(remap), left, right);
  }

  private Join join(
      final Function<JoinInput, Expression> conditionFn,
      final Join.JoinType joinType,
      final Optional<Rel.Remap> remap,
      final Rel left,
      final Rel right) {
    final Expression condition = conditionFn.apply(new JoinInput(left, right));
    return Join.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public HashJoin hashJoin(
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final HashJoin.JoinType joinType,
      final Rel left,
      final Rel right) {
    return hashJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  public HashJoin hashJoin(
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final HashJoin.JoinType joinType,
      final Optional<Rel.Remap> remap,
      final Rel left,
      final Rel right) {
    return HashJoin.builder()
        .left(left)
        .right(right)
        .leftKeys(
            this.fieldReferences(left, leftKeys.stream().mapToInt(Integer::intValue).toArray()))
        .rightKeys(
            this.fieldReferences(right, rightKeys.stream().mapToInt(Integer::intValue).toArray()))
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public MergeJoin mergeJoin(
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final MergeJoin.JoinType joinType,
      final Rel left,
      final Rel right) {
    return mergeJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  public MergeJoin mergeJoin(
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final MergeJoin.JoinType joinType,
      final Optional<Rel.Remap> remap,
      final Rel left,
      final Rel right) {
    return MergeJoin.builder()
        .left(left)
        .right(right)
        .leftKeys(
            this.fieldReferences(left, leftKeys.stream().mapToInt(Integer::intValue).toArray()))
        .rightKeys(
            this.fieldReferences(right, rightKeys.stream().mapToInt(Integer::intValue).toArray()))
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public NestedLoopJoin nestedLoopJoin(
      final Function<JoinInput, Expression> conditionFn,
      final NestedLoopJoin.JoinType joinType,
      final Rel left,
      final Rel right) {
    return nestedLoopJoin(conditionFn, joinType, Optional.empty(), left, right);
  }

  private NestedLoopJoin nestedLoopJoin(
      final Function<JoinInput, Expression> conditionFn,
      final NestedLoopJoin.JoinType joinType,
      final Optional<Rel.Remap> remap,
      final Rel left,
      final Rel right) {
    final Expression condition = conditionFn.apply(new JoinInput(left, right));
    return NestedLoopJoin.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public NamedScan namedScan(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final Iterable<Type> types) {
    return namedScan(tableName, columnNames, types, Optional.empty());
  }

  public NamedScan namedScan(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final Iterable<Type> types,
      final Rel.Remap remap) {
    return namedScan(tableName, columnNames, types, Optional.of(remap));
  }

  private NamedScan namedScan(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final Iterable<Type> types,
      final Optional<Rel.Remap> remap) {
    final Type.Struct struct = Type.Struct.builder().addAllFields(types).nullable(false).build();
    final NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
    return NamedScan.builder().names(tableName).initialSchema(namedStruct).remap(remap).build();
  }

  public EmptyScan emptyScan() {
    return EmptyScan.builder()
        .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct()))
        .build();
  }

  public NamedWrite namedWrite(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final AbstractWriteRel.WriteOp op,
      final AbstractWriteRel.CreateMode createMode,
      final AbstractWriteRel.OutputMode outputMode,
      final Rel input) {
    return namedWrite(tableName, columnNames, op, createMode, outputMode, input, Optional.empty());
  }

  public NamedWrite namedWrite(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final AbstractWriteRel.WriteOp op,
      final AbstractWriteRel.CreateMode createMode,
      final AbstractWriteRel.OutputMode outputMode,
      final Rel input,
      final Rel.Remap remap) {
    return namedWrite(
        tableName, columnNames, op, createMode, outputMode, input, Optional.of(remap));
  }

  private NamedWrite namedWrite(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final AbstractWriteRel.WriteOp op,
      final AbstractWriteRel.CreateMode createMode,
      final AbstractWriteRel.OutputMode outputMode,
      final Rel input,
      final Optional<Rel.Remap> remap) {
    final Type.Struct struct = input.getRecordType();
    final NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
    return NamedWrite.builder()
        .names(tableName)
        .tableSchema(namedStruct)
        .operation(op)
        .createMode(createMode)
        .outputMode(outputMode)
        .input(input)
        .remap(remap)
        .build();
  }

  public NamedUpdate namedUpdate(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final List<NamedUpdate.TransformExpression> transformations,
      final Expression condition,
      final boolean nullable) {
    return namedUpdate(
        tableName, columnNames, transformations, condition, nullable, Optional.empty());
  }

  public NamedUpdate namedUpdate(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final List<NamedUpdate.TransformExpression> transformations,
      final Expression condition,
      final boolean nullable,
      final Rel.Remap remap) {
    return namedUpdate(
        tableName, columnNames, transformations, condition, nullable, Optional.of(remap));
  }

  private NamedUpdate namedUpdate(
      final Iterable<String> tableName,
      final Iterable<String> columnNames,
      final List<NamedUpdate.TransformExpression> transformations,
      final Expression condition,
      final boolean nullable,
      final Optional<Rel.Remap> remap) {
    final List<Type> types =
        transformations.stream()
            .map(t -> t.getTransformation().getType())
            .collect(Collectors.toList());
    final Type.Struct struct = Type.Struct.builder().fields(types).nullable(nullable).build();
    final NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
    return NamedUpdate.builder()
        .names(tableName)
        .tableSchema(namedStruct)
        .transformations(transformations)
        .condition(condition)
        .remap(remap)
        .build();
  }

  public Project project(
      final Function<Rel, Iterable<? extends Expression>> expressionsFn, final Rel input) {
    return project(expressionsFn, Optional.empty(), input);
  }

  public Project project(
      final Function<Rel, Iterable<? extends Expression>> expressionsFn,
      final Rel.Remap remap,
      final Rel input) {
    return project(expressionsFn, Optional.of(remap), input);
  }

  private Project project(
      final Function<Rel, Iterable<? extends Expression>> expressionsFn,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    final Iterable<? extends Expression> expressions = expressionsFn.apply(input);
    return Project.builder().input(input).expressions(expressions).remap(remap).build();
  }

  public Expand expand(
      final Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn, final Rel input) {
    return expand(fieldsFn, Optional.empty(), input);
  }

  public Expand expand(
      final Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn,
      final Rel.Remap remap,
      final Rel input) {
    return expand(fieldsFn, Optional.of(remap), input);
  }

  private Expand expand(
      final Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    final Iterable<? extends Expand.ExpandField> fields = fieldsFn.apply(input);
    return Expand.builder().input(input).fields(fields).remap(remap).build();
  }

  public Set set(final Set.SetOp op, final Rel... inputs) {
    return set(op, Optional.empty(), inputs);
  }

  public Set set(final Set.SetOp op, final Rel.Remap remap, final Rel... inputs) {
    return set(op, Optional.of(remap), inputs);
  }

  private Set set(final Set.SetOp op, final Optional<Rel.Remap> remap, final Rel... inputs) {
    return Set.builder().setOp(op).remap(remap).addAllInputs(Arrays.asList(inputs)).build();
  }

  public Sort sort(
      final Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn, final Rel input) {
    return sort(sortFieldFn, Optional.empty(), input);
  }

  public Sort sort(
      final Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn,
      final Rel.Remap remap,
      final Rel input) {
    return sort(sortFieldFn, Optional.of(remap), input);
  }

  private Sort sort(
      final Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn,
      final Optional<Rel.Remap> remap,
      final Rel input) {
    final Iterable<? extends Expression.SortField> condition = sortFieldFn.apply(input);
    return Sort.builder().input(input).sortFields(condition).remap(remap).build();
  }

  // Expressions

  public Expression.BoolLiteral bool(final boolean v) {
    return Expression.BoolLiteral.builder().value(v).build();
  }

  public Expression.I32Literal i32(final int v) {
    return Expression.I32Literal.builder().value(v).build();
  }

  public Expression.FP64Literal fp64(final double v) {
    return Expression.FP64Literal.builder().value(v).build();
  }

  public Expression.StrLiteral str(final String s) {
    return Expression.StrLiteral.builder().value(s).build();
  }

  public Expression cast(final Expression input, final Type type) {
    return Cast.builder()
        .input(input)
        .type(type)
        .failureBehavior(FailureBehavior.UNSPECIFIED)
        .build();
  }

  public FieldReference fieldReference(final Rel input, final int index) {
    return FieldReference.newInputRelReference(index, input);
  }

  public List<FieldReference> fieldReferences(final Rel input, final int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(input, index))
        .collect(java.util.stream.Collectors.toList());
  }

  public FieldReference fieldReference(final List<Rel> inputs, final int index) {
    return FieldReference.newInputRelReference(index, inputs);
  }

  public List<FieldReference> fieldReferences(final List<Rel> inputs, final int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(inputs, index))
        .collect(java.util.stream.Collectors.toList());
  }

  public IfThen ifThen(final Iterable<? extends IfClause> ifClauses, final Expression elseClause) {
    return IfThen.builder().addAllIfClauses(ifClauses).elseClause(elseClause).build();
  }

  public IfClause ifClause(final Expression condition, final Expression then) {
    return IfClause.builder().condition(condition).then(then).build();
  }

  public Expression singleOrList(final Expression condition, final Expression... options) {
    return SingleOrList.builder().condition(condition).addOptions(options).build();
  }

  public Expression.InPredicate inPredicate(final Rel haystack, final Expression... needles) {
    return Expression.InPredicate.builder()
        .addAllNeedles(Arrays.asList(needles))
        .haystack(haystack)
        .build();
  }

  public List<Expression.SortField> sortFields(final Rel input, final int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(
            index ->
                Expression.SortField.builder()
                    .expr(FieldReference.newInputRelReference(index, input))
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build())
        .collect(java.util.stream.Collectors.toList());
  }

  public Expression.SortField sortField(
      final Expression expression, final Expression.SortDirection sortDirection) {
    return Expression.SortField.builder().expr(expression).direction(sortDirection).build();
  }

  public SwitchClause switchClause(final Expression.Literal condition, final Expression then) {
    return SwitchClause.builder().condition(condition).then(then).build();
  }

  public Switch switchExpression(
      final Expression match,
      final Iterable<? extends SwitchClause> clauses,
      final Expression defaultClause) {
    return Switch.builder()
        .match(match)
        .addAllSwitchClauses(clauses)
        .defaultClause(defaultClause)
        .build();
  }

  // Aggregate Functions

  public AggregateFunctionInvocation aggregateFn(
      final String urn, final String key, final Type outputType, final Expression... args) {
    final SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(SimpleExtension.FunctionAnchor.of(urn, key));
    return AggregateFunctionInvocation.builder()
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .outputType(outputType)
        .declaration(declaration)
        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
        .invocation(Expression.AggregationInvocation.ALL)
        .build();
  }

  public Aggregate.Grouping grouping(final Rel input, final int... indexes) {
    final List<FieldReference> columns = fieldReferences(input, indexes);
    return Aggregate.Grouping.builder().addAllExpressions(columns).build();
  }

  public Aggregate.Grouping grouping(final Expression... expressions) {
    return Aggregate.Grouping.builder().addExpressions(expressions).build();
  }

  public Aggregate.Measure count(final Rel input, final int field) {
    final SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:any"));
    return measure(
        AggregateFunctionInvocation.builder()
            .arguments(fieldReferences(input, field))
            .outputType(R.I64)
            .declaration(declaration)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .build());
  }

  /**
   * Returns a {@link Measure} representing the equivalent of a {@code COUNT(*)} SQL aggregation.
   *
   * @return the {@link Measure} representing {@code COUNT(*)}
   */
  public Measure countStar() {
    final SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:"));
    return measure(
        AggregateFunctionInvocation.builder()
            .outputType(R.I64)
            .declaration(declaration)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .build());
  }

  public Aggregate.Measure min(final Rel input, final int field) {
    return min(fieldReference(input, field));
  }

  public Aggregate.Measure min(final Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "min",
        // min output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure max(final Rel input, final int field) {
    return max(fieldReference(input, field));
  }

  public Aggregate.Measure max(final Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "max",
        // max output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure avg(final Rel input, final int field) {
    return avg(fieldReference(input, field));
  }

  public Aggregate.Measure avg(final Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "avg",
        // avg output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure sum(final Rel input, final int field) {
    return sum(fieldReference(input, field));
  }

  public Aggregate.Measure sum(final Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "sum",
        // sum output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure sum0(final Rel input, final int field) {
    return sum(fieldReference(input, field));
  }

  public Aggregate.Measure sum0(final Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "sum0",
        // sum0 output is always NOT NULL I64
        R.I64);
  }

  private Aggregate.Measure singleArgumentArithmeticAggregate(
      final Expression expr, final String functionName, final Type outputType) {
    final String typeString = ToTypeString.apply(expr.getType());
    final SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
                String.format("%s:%s", functionName, typeString)));
    return measure(
        AggregateFunctionInvocation.builder()
            .arguments(Arrays.asList(expr))
            .outputType(outputType)
            .declaration(declaration)
            // INITIAL_TO_RESULT is the most restrictive aggregation phase type,
            // as it does not allow decomposition. Use it as the default for now.
            // TODO: set this per function
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .build());
  }

  // Scalar Functions

  public Expression.ScalarFunctionInvocation negate(final Expression expr) {
    // output type of negate is the same as the input type
    final Type outputType = expr.getType();
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        String.format("negate:%s", ToTypeString.apply(outputType)),
        outputType,
        expr);
  }

  public Expression.ScalarFunctionInvocation add(final Expression left, final Expression right) {
    return arithmeticFunction("add", left, right);
  }

  public Expression.ScalarFunctionInvocation subtract(
      final Expression left, final Expression right) {
    return arithmeticFunction("substract", left, right);
  }

  public Expression.ScalarFunctionInvocation multiply(
      final Expression left, final Expression right) {
    return arithmeticFunction("multiply", left, right);
  }

  public Expression.ScalarFunctionInvocation divide(final Expression left, final Expression right) {
    return arithmeticFunction("divide", left, right);
  }

  private Expression.ScalarFunctionInvocation arithmeticFunction(
      final String fname, final Expression left, final Expression right) {
    final String leftTypeStr = ToTypeString.apply(left.getType());
    final String rightTypeStr = ToTypeString.apply(right.getType());
    final String key = String.format("%s:%s_%s", fname, leftTypeStr, rightTypeStr);

    final boolean isOutputNullable = left.getType().nullable() || right.getType().nullable();
    Type outputType = left.getType();
    outputType =
        isOutputNullable
            ? TypeCreator.asNullable(outputType)
            : TypeCreator.asNotNullable(outputType);

    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, key, outputType, left, right);
  }

  public Expression.ScalarFunctionInvocation equal(final Expression left, final Expression right) {
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "equal:any_any", R.BOOLEAN, left, right);
  }

  public Expression.ScalarFunctionInvocation and(final Expression... args) {
    // If any arg is nullable, the output of and is potentially nullable
    // For example: false and null = null
    final boolean isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
    final Type outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "and:bool", outputType, args);
  }

  public Expression.ScalarFunctionInvocation or(final Expression... args) {
    // If any arg is nullable, the output of or is potentially nullable
    // For example: false or null = null
    final boolean isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
    final Type outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "or:bool", outputType, args);
  }

  public Expression.ScalarFunctionInvocation scalarFn(
      final String urn, final String key, final Type outputType, final FunctionArg... args) {
    final SimpleExtension.ScalarFunctionVariant declaration =
        extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(urn, key));
    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .build();
  }

  public Expression.WindowFunctionInvocation windowFn(
      final String urn,
      final String key,
      final Type outputType,
      final Expression.AggregationPhase aggregationPhase,
      final Expression.AggregationInvocation invocation,
      final Expression.WindowBoundsType boundsType,
      final WindowBound lowerBound,
      final WindowBound upperBound,
      final Expression... args) {
    final SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(SimpleExtension.FunctionAnchor.of(urn, key));
    return Expression.WindowFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .aggregationPhase(aggregationPhase)
        .invocation(invocation)
        .boundsType(boundsType)
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .build();
  }

  // Types

  public Type.UserDefined userDefinedType(final String urn, final String typeName) {
    return Type.UserDefined.builder().urn(urn).name(typeName).nullable(false).build();
  }

  // Misc

  public Plan.Root root(final Rel rel) {
    return Plan.Root.builder().input(rel).build();
  }

  public Plan plan(final Plan.Root root) {
    return Plan.builder().addRoots(root).build();
  }

  public Rel.Remap remap(final Integer... fields) {
    return Rel.Remap.of(Arrays.asList(fields));
  }

  public Expression scalarSubquery(final Rel input, final Type type) {
    return Expression.ScalarSubquery.builder().input(input).type(type).build();
  }

  public Expression exists(final Rel rel) {
    return Expression.SetPredicate.builder()
        .tuples(rel)
        .predicateOp(PredicateOp.PREDICATE_OP_EXISTS)
        .build();
  }
}
