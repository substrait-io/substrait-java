package io.substrait.dsl;

import com.github.bsideup.jabel.Desugar;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.FailureBehavior;
import io.substrait.expression.Expression.IfClause;
import io.substrait.expression.Expression.IfThen;
import io.substrait.expression.Expression.SwitchClause;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression.Cast;
import io.substrait.expression.ImmutableExpression.SingleOrList;
import io.substrait.expression.ImmutableExpression.Switch;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ToTypeString;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.ImmutableType;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
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

  public SubstraitBuilder(SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
  }

  // Relations
  public Aggregate.Measure measure(AggregateFunctionInvocation aggFn) {
    return Aggregate.Measure.builder().function(aggFn).build();
  }

  public Aggregate.Measure measure(AggregateFunctionInvocation aggFn, Expression preMeasureFilter) {
    return Aggregate.Measure.builder().function(aggFn).preMeasureFilter(preMeasureFilter).build();
  }

  public Aggregate aggregate(
      Function<Rel, Aggregate.Grouping> groupingFn,
      Function<Rel, List<Aggregate.Measure>> measuresFn,
      Rel input) {
    Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.empty(), input);
  }

  public Aggregate aggregate(
      Function<Rel, Aggregate.Grouping> groupingFn,
      Function<Rel, List<Aggregate.Measure>> measuresFn,
      Rel.Remap remap,
      Rel input) {
    Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.of(remap), input);
  }

  private Aggregate aggregate(
      Function<Rel, List<Aggregate.Grouping>> groupingsFn,
      Function<Rel, List<Aggregate.Measure>> measuresFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    var groupings = groupingsFn.apply(input);
    var measures = measuresFn.apply(input);
    return Aggregate.builder()
        .groupings(groupings)
        .measures(measures)
        .remap(remap)
        .input(input)
        .build();
  }

  public Cross cross(Rel left, Rel right) {
    return cross(left, right, Optional.empty());
  }

  public Cross cross(Rel left, Rel right, Rel.Remap remap) {
    return cross(left, right, Optional.of(remap));
  }

  private Cross cross(Rel left, Rel right, Optional<Rel.Remap> remap) {
    return Cross.builder().left(left).right(right).remap(remap).build();
  }

  public Fetch fetch(long offset, long count, Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.empty(), input);
  }

  public Fetch fetch(long offset, long count, Rel.Remap remap, Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.of(remap), input);
  }

  public Fetch limit(long limit, Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.empty(), input);
  }

  public Fetch limit(long limit, Rel.Remap remap, Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.of(remap), input);
  }

  public Fetch offset(long offset, Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.empty(), input);
  }

  public Fetch offset(long offset, Rel.Remap remap, Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.of(remap), input);
  }

  private Fetch fetch(long offset, OptionalLong count, Optional<Rel.Remap> remap, Rel input) {
    return Fetch.builder().offset(offset).count(count).input(input).remap(remap).build();
  }

  public Filter filter(Function<Rel, Expression> conditionFn, Rel input) {
    return filter(conditionFn, Optional.empty(), input);
  }

  public Filter filter(Function<Rel, Expression> conditionFn, Rel.Remap remap, Rel input) {
    return filter(conditionFn, Optional.of(remap), input);
  }

  private Filter filter(
      Function<Rel, Expression> conditionFn, Optional<Rel.Remap> remap, Rel input) {
    var condition = conditionFn.apply(input);
    return Filter.builder().input(input).condition(condition).remap(remap).build();
  }

  @Desugar
  public record JoinInput(Rel left, Rel right) {}

  public Join innerJoin(Function<JoinInput, Expression> conditionFn, Rel left, Rel right) {
    return join(conditionFn, Join.JoinType.INNER, left, right);
  }

  public Join innerJoin(
      Function<JoinInput, Expression> conditionFn, Rel.Remap remap, Rel left, Rel right) {
    return join(conditionFn, Join.JoinType.INNER, remap, left, right);
  }

  public Join join(
      Function<JoinInput, Expression> conditionFn, Join.JoinType joinType, Rel left, Rel right) {
    return join(conditionFn, joinType, Optional.empty(), left, right);
  }

  public Join join(
      Function<JoinInput, Expression> conditionFn,
      Join.JoinType joinType,
      Rel.Remap remap,
      Rel left,
      Rel right) {
    return join(conditionFn, joinType, Optional.of(remap), left, right);
  }

  private Join join(
      Function<JoinInput, Expression> conditionFn,
      Join.JoinType joinType,
      Optional<Rel.Remap> remap,
      Rel left,
      Rel right) {
    var condition = conditionFn.apply(new JoinInput(left, right));
    return Join.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public HashJoin hashJoin(
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      HashJoin.JoinType joinType,
      Rel left,
      Rel right) {
    return hashJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  public HashJoin hashJoin(
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      HashJoin.JoinType joinType,
      Optional<Rel.Remap> remap,
      Rel left,
      Rel right) {
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
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      MergeJoin.JoinType joinType,
      Rel left,
      Rel right) {
    return mergeJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  public MergeJoin mergeJoin(
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      MergeJoin.JoinType joinType,
      Optional<Rel.Remap> remap,
      Rel left,
      Rel right) {
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
      Function<JoinInput, Expression> conditionFn,
      NestedLoopJoin.JoinType joinType,
      Rel left,
      Rel right) {
    return nestedLoopJoin(conditionFn, joinType, Optional.empty(), left, right);
  }

  private NestedLoopJoin nestedLoopJoin(
      Function<JoinInput, Expression> conditionFn,
      NestedLoopJoin.JoinType joinType,
      Optional<Rel.Remap> remap,
      Rel left,
      Rel right) {
    var condition = conditionFn.apply(new JoinInput(left, right));
    return NestedLoopJoin.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  public NamedScan namedScan(
      Iterable<String> tableName, Iterable<String> columnNames, Iterable<Type> types) {
    return namedScan(tableName, columnNames, types, Optional.empty());
  }

  public NamedScan namedScan(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      Iterable<Type> types,
      Rel.Remap remap) {
    return namedScan(tableName, columnNames, types, Optional.of(remap));
  }

  private NamedScan namedScan(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      Iterable<Type> types,
      Optional<Rel.Remap> remap) {
    var struct = Type.Struct.builder().addAllFields(types).nullable(false).build();
    var namedStruct = NamedStruct.of(columnNames, struct);
    return NamedScan.builder().names(tableName).initialSchema(namedStruct).remap(remap).build();
  }

  public VirtualTableScan virtualTableScan(
      List<String> dfsNames, List<Expression.StructLiteral> rows) {
    return VirtualTableScan.builder().dfsNames(dfsNames).rows(rows).build();
  }

  public Project project(Function<Rel, Iterable<? extends Expression>> expressionsFn, Rel input) {
    return project(expressionsFn, Optional.empty(), input);
  }

  public Project project(
      Function<Rel, Iterable<? extends Expression>> expressionsFn, Rel.Remap remap, Rel input) {
    return project(expressionsFn, Optional.of(remap), input);
  }

  private Project project(
      Function<Rel, Iterable<? extends Expression>> expressionsFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    var expressions = expressionsFn.apply(input);
    return Project.builder().input(input).expressions(expressions).remap(remap).build();
  }

  public Set set(Set.SetOp op, Rel... inputs) {
    return set(op, Optional.empty(), inputs);
  }

  public Set set(Set.SetOp op, Rel.Remap remap, Rel... inputs) {
    return set(op, Optional.of(remap), inputs);
  }

  private Set set(Set.SetOp op, Optional<Rel.Remap> remap, Rel... inputs) {
    return Set.builder().setOp(op).remap(remap).addAllInputs(Arrays.asList(inputs)).build();
  }

  public Sort sort(Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn, Rel input) {
    return sort(sortFieldFn, Optional.empty(), input);
  }

  public Sort sort(
      Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn,
      Rel.Remap remap,
      Rel input) {
    return sort(sortFieldFn, Optional.of(remap), input);
  }

  private Sort sort(
      Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    var condition = sortFieldFn.apply(input);
    return Sort.builder().input(input).sortFields(condition).remap(remap).build();
  }

  // Expressions

  public Expression.BoolLiteral bool(boolean v) {
    return Expression.BoolLiteral.builder().value(v).build();
  }

  public Expression.I32Literal i32(int v) {
    return Expression.I32Literal.builder().value(v).build();
  }

  public Expression.I64Literal i64(int v) {
    return Expression.I64Literal.builder().value(v).build();
  }

  public Expression.FP64Literal fp64(double v) {
    return Expression.FP64Literal.builder().value(v).build();
  }

  public Expression.StrLiteral str(String s) {
    return Expression.StrLiteral.builder().value(s).build();
  }

  public Expression.StructLiteral struct(Expression.Literal... fields) {
    return Expression.StructLiteral.builder().addFields(fields).build();
  }

  public Expression cast(Expression input, Type type) {
    return Cast.builder()
        .input(input)
        .type(type)
        .failureBehavior(FailureBehavior.UNSPECIFIED)
        .build();
  }

  public FieldReference fieldReference(Rel input, int index) {
    return ImmutableFieldReference.newInputRelReference(index, input);
  }

  public List<FieldReference> fieldReferences(Rel input, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(input, index))
        .collect(java.util.stream.Collectors.toList());
  }

  public FieldReference fieldReference(List<Rel> inputs, int index) {
    return ImmutableFieldReference.newInputRelReference(index, inputs);
  }

  public List<FieldReference> fieldReferences(List<Rel> inputs, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(inputs, index))
        .collect(java.util.stream.Collectors.toList());
  }

  public IfThen ifThen(Iterable<? extends IfClause> ifClauses, Expression elseClause) {
    return IfThen.builder().addAllIfClauses(ifClauses).elseClause(elseClause).build();
  }

  public IfClause ifClause(Expression condition, Expression then) {
    return IfClause.builder().condition(condition).then(then).build();
  }

  public Expression singleOrList(Expression condition, Expression... options) {
    return SingleOrList.builder().condition(condition).addOptions(options).build();
  }

  public Expression.InPredicate inPredicate(Rel haystack, Expression... needles) {
    return Expression.InPredicate.builder()
        .addAllNeedles(Arrays.asList(needles))
        .haystack(haystack)
        .build();
  }

  public List<Expression.SortField> sortFields(Rel input, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(
            index ->
                Expression.SortField.builder()
                    .expr(ImmutableFieldReference.newInputRelReference(index, input))
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build())
        .collect(java.util.stream.Collectors.toList());
  }

  public Expression.SortField sortField(
      Expression expression, Expression.SortDirection sortDirection) {
    return Expression.SortField.builder().expr(expression).direction(sortDirection).build();
  }

  public SwitchClause switchClause(Expression.Literal condition, Expression then) {
    return SwitchClause.builder().condition(condition).then(then).build();
  }

  public Switch switchExpression(
      Expression match, Iterable<? extends SwitchClause> clauses, Expression defaultClause) {
    return Switch.builder()
        .match(match)
        .addAllSwitchClauses(clauses)
        .defaultClause(defaultClause)
        .build();
  }

  // Aggregate Functions

  public AggregateFunctionInvocation aggregateFn(
      String namespace, String key, Type outputType, Expression... args) {
    var declaration =
        extensions.getAggregateFunction(SimpleExtension.FunctionAnchor.of(namespace, key));
    return AggregateFunctionInvocation.builder()
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .outputType(outputType)
        .declaration(declaration)
        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
        .invocation(Expression.AggregationInvocation.ALL)
        .build();
  }

  public Aggregate.Grouping grouping(Rel input, int... indexes) {
    var columns = fieldReferences(input, indexes);
    return Aggregate.Grouping.builder().addAllExpressions(columns).build();
  }

  public Aggregate.Grouping grouping(Expression... expressions) {
    return Aggregate.Grouping.builder().addExpressions(expressions).build();
  }

  public Aggregate.Measure count(Rel input, int field) {
    var declaration =
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

  public Aggregate.Measure min(Rel input, int field) {
    return min(fieldReference(input, field));
  }

  public Aggregate.Measure min(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "min",
        // min output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure max(Rel input, int field) {
    return max(fieldReference(input, field));
  }

  public Aggregate.Measure max(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "max",
        // max output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure avg(Rel input, int field) {
    return avg(fieldReference(input, field));
  }

  public Aggregate.Measure avg(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "avg",
        // avg output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure sum(Rel input, int field) {
    return sum(fieldReference(input, field));
  }

  public Aggregate.Measure sum(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "sum",
        // sum output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  public Aggregate.Measure sum0(Rel input, int field) {
    return sum(fieldReference(input, field));
  }

  public Aggregate.Measure sum0(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "sum0",
        // sum0 output is always NOT NULL I64
        R.I64);
  }

  private Aggregate.Measure singleArgumentArithmeticAggregate(
      Expression expr, String functionName, Type outputType) {
    String typeString = ToTypeString.apply(expr.getType());
    var declaration =
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

  public Expression.ScalarFunctionInvocation negate(Expression expr) {
    // output type of negate is the same as the input type
    var outputType = expr.getType();
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        String.format("negate:%s", ToTypeString.apply(outputType)),
        outputType,
        expr);
  }

  public Expression.ScalarFunctionInvocation add(Expression left, Expression right) {
    return arithmeticFunction("add", left, right);
  }

  public Expression.ScalarFunctionInvocation subtract(Expression left, Expression right) {
    return arithmeticFunction("substract", left, right);
  }

  public Expression.ScalarFunctionInvocation multiply(Expression left, Expression right) {
    return arithmeticFunction("multiply", left, right);
  }

  public Expression.ScalarFunctionInvocation divide(Expression left, Expression right) {
    return arithmeticFunction("divide", left, right);
  }

  private Expression.ScalarFunctionInvocation arithmeticFunction(
      String fname, Expression left, Expression right) {
    var leftTypeStr = ToTypeString.apply(left.getType());
    var rightTypeStr = ToTypeString.apply(right.getType());
    var key = String.format("%s:%s_%s", fname, leftTypeStr, rightTypeStr);

    var isOutputNullable = left.getType().nullable() || right.getType().nullable();
    var outputType = left.getType();
    outputType =
        isOutputNullable
            ? TypeCreator.asNullable(outputType)
            : TypeCreator.asNotNullable(outputType);

    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, key, outputType, left, right);
  }

  public Expression.ScalarFunctionInvocation equal(Expression left, Expression right) {
    return comparisonFunction("equal", left, right);
  }

  public Expression.ScalarFunctionInvocation lt(Expression left, Expression right) {
    return comparisonFunction("lt", left, right);
  }

  public Expression.ScalarFunctionInvocation gt(Expression left, Expression right) {
    return comparisonFunction("gt", left, right);
  }

  private Expression.ScalarFunctionInvocation comparisonFunction(
      String fname, Expression left, Expression right) {
    var key = String.format("%s:any_any", fname);

    var isOutputNullable = left.getType().nullable() || right.getType().nullable();
    var outputType = left.getType();
    outputType =
        isOutputNullable
            ? TypeCreator.asNullable(outputType)
            : TypeCreator.asNotNullable(outputType);

    return scalarFn(
        io.substrait.extension.DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
        key,
        outputType,
        left,
        right);
  }

  public Expression.ScalarFunctionInvocation or(Expression... args) {
    // If any arg is nullable, the output of or is potentially nullable
    // For example: false or null = null
    var isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
    var outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "or:bool", outputType, args);
  }

  public Expression.ScalarFunctionInvocation scalarFn(
      String namespace, String key, Type outputType, Expression... args) {
    var declaration =
        extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, key));
    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .build();
  }

  public Expression.WindowFunctionInvocation windowFn(
      String namespace,
      String key,
      Type outputType,
      Expression.AggregationPhase aggregationPhase,
      Expression.AggregationInvocation invocation,
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      Expression... args) {
    var declaration =
        extensions.getWindowFunction(SimpleExtension.FunctionAnchor.of(namespace, key));
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

  // MATCH_RECOGNIZE Functions

  public Expression.ScalarFunctionInvocation patternRef(
      Rel input, String patternIdentifier, int colRef) {
    var outputType = input.getRecordType().fields().get(colRef);
    return scalarFn(
        io.substrait.extension.DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "pattern_ref:str_i32",
        outputType,
        str(patternIdentifier),
        i32(colRef));
  }

  public Expression.ScalarFunctionInvocation classifier() {
    return scalarFn(
        io.substrait.extension.DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "classifier:",
        TypeCreator.of(false).STRING);
  }

  public Expression.ScalarFunctionInvocation matchNumber() {
    return scalarFn(
        io.substrait.extension.DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "match_number:",
        TypeCreator.of(false).I32);
  }

  public Expression.ScalarFunctionInvocation prev(Rel input, String patternIdentifier, int colRef) {
    var outputType = input.getRecordType().fields().get(colRef);
    return scalarFn(
        io.substrait.extension.DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "prev:str_i32",
        outputType,
        str(patternIdentifier),
        i32(colRef));
  }

  public Expression.ScalarFunctionInvocation next(Rel input, String patternIdentifier, int colRef) {
    var outputType = input.getRecordType().fields().get(colRef);
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "next:str_i32",
        outputType,
        str(patternIdentifier),
        i32(colRef));
  }

  public Expression.ScalarFunctionInvocation last(Rel input, int colRef) {
    var outputType = input.getRecordType().fields().get(colRef);
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE, "last:i32", outputType, i32(colRef));
  }

  public Expression.ScalarFunctionInvocation last(Rel input, String patternIdentifier, int colRef) {
    var outputType = input.getRecordType().fields().get(colRef);
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_MATCH_RECOGNIZE,
        "last:str_i32",
        outputType,
        str(patternIdentifier),
        i32(colRef));
  }

  // Types
  public Type.UserDefined userDefinedType(String namespace, String typeName) {
    return ImmutableType.UserDefined.builder()
        .uri(namespace)
        .name(typeName)
        .nullable(false)
        .build();
  }

  // Misc

  public Plan.Root root(Rel rel) {
    return ImmutableRoot.builder().input(rel).build();
  }

  public Plan.Root root(Rel rel, List<String> names) {
    return ImmutableRoot.builder().input(rel).build();
  }

  public Plan plan(Plan.Root root) {
    return ImmutablePlan.builder().addRoots(root).build();
  }

  public Rel.Remap remap(Integer... fields) {
    return Rel.Remap.of(Arrays.asList(fields));
  }
}
