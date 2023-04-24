package io.substrait.dsl;

import com.github.bsideup.jabel.Desugar;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.function.SimpleExtension;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.proto.AggregateFunction;
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
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
  public Aggregate aggregate(
      Function<Rel, Aggregate.Grouping> groupingFn,
      Function<Rel, List<AggregateFunctionInvocation>> measuresFn,
      Rel input) {
    Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.empty(), input);
  }

  public Aggregate aggregate(
      Function<Rel, Aggregate.Grouping> groupingFn,
      Function<Rel, List<AggregateFunctionInvocation>> measuresFn,
      Rel.Remap remap,
      Rel input) {
    Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.of(remap), input);
  }

  private Aggregate aggregate(
      Function<Rel, List<Aggregate.Grouping>> groupingsFn,
      Function<Rel, List<AggregateFunctionInvocation>> measuresFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    var groupings = groupingsFn.apply(input);
    var measures =
        measuresFn.apply(input).stream()
            .map(m -> Aggregate.Measure.builder().function(m).build())
            .collect(java.util.stream.Collectors.toList());
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
    return fetch(offset, count, Optional.empty(), input);
  }

  public Fetch fetch(long offset, long count, Rel.Remap remap, Rel input) {
    return fetch(offset, count, Optional.of(remap), input);
  }

  private Fetch fetch(long offset, long count, Optional<Rel.Remap> remap, Rel input) {
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

  public FieldReference fieldReference(Rel input, int index) {
    return ImmutableFieldReference.newInputRelReference(index, input);
  }

  public List<FieldReference> fieldReferences(Rel input, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(input, index))
        .collect(java.util.stream.Collectors.toList());
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
        .invocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)
        .build();
  }

  public Aggregate.Grouping grouping(Rel input, int... indexes) {
    var columns = fieldReferences(input, indexes);
    return Aggregate.Grouping.builder().addAllExpressions(columns).build();
  }

  public AggregateFunctionInvocation count(Rel input, int field) {
    var declaration =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of("/functions_aggregate_generic.yaml", "count:any"));
    return AggregateFunctionInvocation.builder()
        .arguments(fieldReferences(input, field))
        .outputType(R.I64)
        .declaration(declaration)
        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
        .invocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)
        .build();
  }

  // Scalar Functions

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

  // Misc

  public Plan.Root root(Rel rel) {
    return ImmutableRoot.builder().input(rel).build();
  }

  public Rel.Remap remap(Integer... fields) {
    return Rel.Remap.of(Arrays.asList(fields));
  }
}
