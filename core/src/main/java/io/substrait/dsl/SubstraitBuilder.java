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
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Aggregate.Measure;
import io.substrait.relation.Cross;
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
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A helper class for constructing Substrait query plans.
 *
 * <p>This class provides convenience methods for creating various Substrait relation operators
 * (e.g., aggregate, join, filter, project) and expressions (e.g., literals, function invocations,
 * field references). It simplifies the construction of complex query plans.
 *
 * <p>The builder requires an {@link SimpleExtension.ExtensionCollection} to resolve function
 * declarations and other extension metadata.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * SubstraitBuilder builder = new SubstraitBuilder(extensions);
 * Rel scan = builder.namedScan(List.of("table"), List.of("col1", "col2"), List.of(R.I32, R.STRING));
 * Rel filtered = builder.filter(rel -> builder.equal(builder.fieldReference(rel, 0), builder.i32(42)), scan);
 * }</pre>
 */
public class SubstraitBuilder {
  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  private final SimpleExtension.ExtensionCollection extensions;

  /**
   * Constructs a new SubstraitBuilder with the specified extension collection.
   *
   * @param extensions the extension collection used to resolve function declarations and other
   *     extension metadata
   */
  public SubstraitBuilder(SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
  }

  // Relations

  /**
   * Creates an aggregate measure from an aggregate function invocation.
   *
   * @param aggFn the aggregate function invocation
   * @return a new {@link Aggregate.Measure}
   */
  public Aggregate.Measure measure(AggregateFunctionInvocation aggFn) {
    return Aggregate.Measure.builder().function(aggFn).build();
  }

  /**
   * Creates an aggregate measure from an aggregate function invocation with a pre-measure filter.
   *
   * @param aggFn the aggregate function invocation
   * @param preMeasureFilter the filter expression to apply before aggregation
   * @return a new {@link Aggregate.Measure}
   */
  public Aggregate.Measure measure(AggregateFunctionInvocation aggFn, Expression preMeasureFilter) {
    return Aggregate.Measure.builder().function(aggFn).preMeasureFilter(preMeasureFilter).build();
  }

  /**
   * Creates an aggregate relation with a single grouping.
   *
   * @param groupingFn function to derive the grouping from the input relation
   * @param measuresFn function to derive the measures from the input relation
   * @param input the input relation to aggregate
   * @return a new {@link Aggregate} relation
   */
  public Aggregate aggregate(
      Function<Rel, Aggregate.Grouping> groupingFn,
      Function<Rel, List<Aggregate.Measure>> measuresFn,
      Rel input) {
    Function<Rel, List<Aggregate.Grouping>> groupingsFn =
        groupingFn.andThen(g -> Stream.of(g).collect(Collectors.toList()));
    return aggregate(groupingsFn, measuresFn, Optional.empty(), input);
  }

  /**
   * Creates an aggregate relation with a single grouping and output field remapping.
   *
   * @param groupingFn function to derive the grouping from the input relation
   * @param measuresFn function to derive the measures from the input relation
   * @param remap the output field remapping specification
   * @param input the input relation to aggregate
   * @return a new {@link Aggregate} relation
   */
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
    List<Aggregate.Grouping> groupings = groupingsFn.apply(input);
    List<Aggregate.Measure> measures = measuresFn.apply(input);
    return Aggregate.builder()
        .groupings(groupings)
        .measures(measures)
        .remap(remap)
        .input(input)
        .build();
  }

  /**
   * Creates a cross join (Cartesian product) between two relations.
   *
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link Cross} relation
   */
  public Cross cross(Rel left, Rel right) {
    return cross(left, right, Optional.empty());
  }

  /**
   * Creates a cross join (Cartesian product) between two relations with output field remapping.
   *
   * @param left the left input relation
   * @param right the right input relation
   * @param remap the output field remapping specification
   * @return a new {@link Cross} relation
   */
  public Cross cross(Rel left, Rel right, Rel.Remap remap) {
    return cross(left, right, Optional.of(remap));
  }

  private Cross cross(Rel left, Rel right, Optional<Rel.Remap> remap) {
    return Cross.builder().left(left).right(right).remap(remap).build();
  }

  /**
   * Creates a fetch relation that skips a number of rows and returns a limited number of rows.
   *
   * @param offset the number of rows to skip
   * @param count the maximum number of rows to return
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch fetch(long offset, long count, Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.empty(), input);
  }

  /**
   * Creates a fetch relation that skips a number of rows and returns a limited number of rows with
   * output field remapping.
   *
   * @param offset the number of rows to skip
   * @param count the maximum number of rows to return
   * @param remap the output field remapping specification
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch fetch(long offset, long count, Rel.Remap remap, Rel input) {
    return fetch(offset, OptionalLong.of(count), Optional.of(remap), input);
  }

  /**
   * Creates a fetch relation that limits the number of rows returned.
   *
   * @param limit the maximum number of rows to return
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch limit(long limit, Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.empty(), input);
  }

  /**
   * Creates a fetch relation that limits the number of rows returned with output field remapping.
   *
   * @param limit the maximum number of rows to return
   * @param remap the output field remapping specification
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch limit(long limit, Rel.Remap remap, Rel input) {
    return fetch(0, OptionalLong.of(limit), Optional.of(remap), input);
  }

  /**
   * Creates a fetch relation that skips a number of rows.
   *
   * @param offset the number of rows to skip
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch offset(long offset, Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.empty(), input);
  }

  /**
   * Creates a fetch relation that skips a number of rows with output field remapping.
   *
   * @param offset the number of rows to skip
   * @param remap the output field remapping specification
   * @param input the input relation
   * @return a new {@link Fetch} relation
   */
  public Fetch offset(long offset, Rel.Remap remap, Rel input) {
    return fetch(offset, OptionalLong.empty(), Optional.of(remap), input);
  }

  private Fetch fetch(long offset, OptionalLong count, Optional<Rel.Remap> remap, Rel input) {
    return Fetch.builder().offset(offset).count(count).input(input).remap(remap).build();
  }

  /**
   * Creates a filter relation that applies a condition to filter rows from the input.
   *
   * @param conditionFn function to derive the filter condition from the input relation
   * @param input the input relation to filter
   * @return a new {@link Filter} relation
   */
  public Filter filter(Function<Rel, Expression> conditionFn, Rel input) {
    return filter(conditionFn, Optional.empty(), input);
  }

  /**
   * Creates a filter relation that applies a condition to filter rows from the input with output
   * field remapping.
   *
   * @param conditionFn function to derive the filter condition from the input relation
   * @param remap the output field remapping specification
   * @param input the input relation to filter
   * @return a new {@link Filter} relation
   */
  public Filter filter(Function<Rel, Expression> conditionFn, Rel.Remap remap, Rel input) {
    return filter(conditionFn, Optional.of(remap), input);
  }

  private Filter filter(
      Function<Rel, Expression> conditionFn, Optional<Rel.Remap> remap, Rel input) {
    Expression condition = conditionFn.apply(input);
    return Filter.builder().input(input).condition(condition).remap(remap).build();
  }

  /**
   * A container class that holds both left and right relations for join operations.
   *
   * <p>This class is used to pass both join inputs to functions that need to reference fields from
   * either side of the join.
   */
  public static final class JoinInput {
    private final Rel left;
    private final Rel right;

    JoinInput(Rel left, Rel right) {
      this.left = left;
      this.right = right;
    }

    /**
     * Returns the left relation of the join.
     *
     * @return the left relation
     */
    public Rel left() {
      return left;
    }

    /**
     * Returns the right relation of the join.
     *
     * @return the right relation
     */
    public Rel right() {
      return right;
    }
  }

  /**
   * Creates an inner join between two relations.
   *
   * @param conditionFn function to derive the join condition from the join inputs
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link Join} relation with INNER join type
   */
  public Join innerJoin(Function<JoinInput, Expression> conditionFn, Rel left, Rel right) {
    return join(conditionFn, Join.JoinType.INNER, left, right);
  }

  /**
   * Creates an inner join between two relations with output field remapping.
   *
   * @param conditionFn function to derive the join condition from the join inputs
   * @param remap the output field remapping specification
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link Join} relation with INNER join type
   */
  public Join innerJoin(
      Function<JoinInput, Expression> conditionFn, Rel.Remap remap, Rel left, Rel right) {
    return join(conditionFn, Join.JoinType.INNER, remap, left, right);
  }

  /**
   * Creates a join between two relations with a specified join type.
   *
   * @param conditionFn function to derive the join condition from the join inputs
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link Join} relation
   */
  public Join join(
      Function<JoinInput, Expression> conditionFn, Join.JoinType joinType, Rel left, Rel right) {
    return join(conditionFn, joinType, Optional.empty(), left, right);
  }

  /**
   * Creates a join between two relations with a specified join type and output field remapping.
   *
   * @param conditionFn function to derive the join condition from the join inputs
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param remap the output field remapping specification
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link Join} relation
   */
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
    Expression condition = conditionFn.apply(new JoinInput(left, right));
    return Join.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  /**
   * Creates a hash join between two relations using specified key columns.
   *
   * @param leftKeys the list of field indexes from the left relation to use as join keys
   * @param rightKeys the list of field indexes from the right relation to use as join keys
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link HashJoin} relation
   */
  public HashJoin hashJoin(
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      HashJoin.JoinType joinType,
      Rel left,
      Rel right) {
    return hashJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  /**
   * Creates a hash join between two relations using specified key columns with optional output
   * field remapping.
   *
   * @param leftKeys the list of field indexes from the left relation to use as join keys
   * @param rightKeys the list of field indexes from the right relation to use as join keys
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param remap optional output field remapping specification
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link HashJoin} relation
   */
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

  /**
   * Creates a merge join between two relations using specified key columns.
   *
   * @param leftKeys the list of field indexes from the left relation to use as join keys
   * @param rightKeys the list of field indexes from the right relation to use as join keys
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link MergeJoin} relation
   */
  public MergeJoin mergeJoin(
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      MergeJoin.JoinType joinType,
      Rel left,
      Rel right) {
    return mergeJoin(leftKeys, rightKeys, joinType, Optional.empty(), left, right);
  }

  /**
   * Creates a merge join between two relations using specified key columns with optional output
   * field remapping.
   *
   * @param leftKeys the list of field indexes from the left relation to use as join keys
   * @param rightKeys the list of field indexes from the right relation to use as join keys
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param remap optional output field remapping specification
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link MergeJoin} relation
   */
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

  /**
   * Creates a nested loop join between two relations.
   *
   * @param conditionFn function to derive the join condition from the join inputs
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, etc.)
   * @param left the left input relation
   * @param right the right input relation
   * @return a new {@link NestedLoopJoin} relation
   */
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
    Expression condition = conditionFn.apply(new JoinInput(left, right));
    return NestedLoopJoin.builder()
        .left(left)
        .right(right)
        .condition(condition)
        .joinType(joinType)
        .remap(remap)
        .build();
  }

  /**
   * Creates a named scan relation that reads from a table.
   *
   * @param tableName the qualified name of the table to scan
   * @param columnNames the names of the columns in the table
   * @param types the types of the columns in the table
   * @return a new {@link NamedScan} relation
   */
  public NamedScan namedScan(
      Iterable<String> tableName, Iterable<String> columnNames, Iterable<Type> types) {
    return namedScan(tableName, columnNames, types, Optional.empty());
  }

  /**
   * Creates a named scan relation that reads from a table with output field remapping.
   *
   * @param tableName the qualified name of the table to scan
   * @param columnNames the names of the columns in the table
   * @param types the types of the columns in the table
   * @param remap the output field remapping specification
   * @return a new {@link NamedScan} relation
   */
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
    Type.Struct struct = Type.Struct.builder().addAllFields(types).nullable(false).build();
    NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
    return NamedScan.builder().names(tableName).initialSchema(namedStruct).remap(remap).build();
  }

  /**
   * Creates an empty virtual table scan with no columns.
   *
   * @return a new {@link VirtualTableScan} relation with an empty schema
   */
  public VirtualTableScan emptyVirtualTableScan() {
    return VirtualTableScan.builder()
        .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct()))
        .build();
  }

  /**
   * Creates a named write relation that writes data to a table.
   *
   * @param tableName the qualified name of the table to write to
   * @param columnNames the names of the columns in the table
   * @param op the write operation (INSERT, UPDATE, DELETE, etc.)
   * @param createMode the table creation mode
   * @param outputMode the output mode for the write operation
   * @param input the input relation containing data to write
   * @return a new {@link NamedWrite} relation
   */
  public NamedWrite namedWrite(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      AbstractWriteRel.WriteOp op,
      AbstractWriteRel.CreateMode createMode,
      AbstractWriteRel.OutputMode outputMode,
      Rel input) {
    return namedWrite(tableName, columnNames, op, createMode, outputMode, input, Optional.empty());
  }

  /**
   * Creates a named write relation that writes data to a table with output field remapping.
   *
   * @param tableName the qualified name of the table to write to
   * @param columnNames the names of the columns in the table
   * @param op the write operation (INSERT, UPDATE, DELETE, etc.)
   * @param createMode the table creation mode
   * @param outputMode the output mode for the write operation
   * @param input the input relation containing data to write
   * @param remap the output field remapping specification
   * @return a new {@link NamedWrite} relation
   */
  public NamedWrite namedWrite(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      AbstractWriteRel.WriteOp op,
      AbstractWriteRel.CreateMode createMode,
      AbstractWriteRel.OutputMode outputMode,
      Rel input,
      Rel.Remap remap) {
    return namedWrite(
        tableName, columnNames, op, createMode, outputMode, input, Optional.of(remap));
  }

  private NamedWrite namedWrite(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      AbstractWriteRel.WriteOp op,
      AbstractWriteRel.CreateMode createMode,
      AbstractWriteRel.OutputMode outputMode,
      Rel input,
      Optional<Rel.Remap> remap) {
    Type.Struct struct = input.getRecordType();
    NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
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

  /**
   * Creates a named update relation that updates rows in a table.
   *
   * @param tableName the qualified name of the table to update
   * @param columnNames the names of the columns in the table
   * @param transformations the list of transformation expressions to apply
   * @param condition the condition to determine which rows to update
   * @param nullable whether the output schema is nullable
   * @return a new {@link NamedUpdate} relation
   */
  public NamedUpdate namedUpdate(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      List<NamedUpdate.TransformExpression> transformations,
      Expression condition,
      boolean nullable) {
    return namedUpdate(
        tableName, columnNames, transformations, condition, nullable, Optional.empty());
  }

  /**
   * Creates a named update relation that updates rows in a table with output field remapping.
   *
   * @param tableName the qualified name of the table to update
   * @param columnNames the names of the columns in the table
   * @param transformations the list of transformation expressions to apply
   * @param condition the condition to determine which rows to update
   * @param nullable whether the output schema is nullable
   * @param remap the output field remapping specification
   * @return a new {@link NamedUpdate} relation
   */
  public NamedUpdate namedUpdate(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      List<NamedUpdate.TransformExpression> transformations,
      Expression condition,
      boolean nullable,
      Rel.Remap remap) {
    return namedUpdate(
        tableName, columnNames, transformations, condition, nullable, Optional.of(remap));
  }

  private NamedUpdate namedUpdate(
      Iterable<String> tableName,
      Iterable<String> columnNames,
      List<NamedUpdate.TransformExpression> transformations,
      Expression condition,
      boolean nullable,
      Optional<Rel.Remap> remap) {
    List<Type> types =
        transformations.stream()
            .map(t -> t.getTransformation().getType())
            .collect(Collectors.toList());
    Type.Struct struct = Type.Struct.builder().fields(types).nullable(nullable).build();
    NamedStruct namedStruct = NamedStruct.of(columnNames, struct);
    return NamedUpdate.builder()
        .names(tableName)
        .tableSchema(namedStruct)
        .transformations(transformations)
        .condition(condition)
        .remap(remap)
        .build();
  }

  /**
   * Creates a project relation that computes new expressions from the input.
   *
   * @param expressionsFn function to derive the projection expressions from the input relation
   * @param input the input relation to project
   * @return a new {@link Project} relation
   */
  public Project project(Function<Rel, Iterable<? extends Expression>> expressionsFn, Rel input) {
    return project(expressionsFn, Optional.empty(), input);
  }

  /**
   * Creates a project relation that computes new expressions from the input with output field
   * remapping.
   *
   * @param expressionsFn function to derive the projection expressions from the input relation
   * @param remap the output field remapping specification
   * @param input the input relation to project
   * @return a new {@link Project} relation
   */
  public Project project(
      Function<Rel, Iterable<? extends Expression>> expressionsFn, Rel.Remap remap, Rel input) {
    return project(expressionsFn, Optional.of(remap), input);
  }

  private Project project(
      Function<Rel, Iterable<? extends Expression>> expressionsFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    Iterable<? extends Expression> expressions = expressionsFn.apply(input);
    return Project.builder().input(input).expressions(expressions).remap(remap).build();
  }

  /**
   * Creates an expand relation that generates multiple output rows from each input row.
   *
   * @param fieldsFn function to derive the expand fields from the input relation
   * @param input the input relation to expand
   * @return a new {@link Expand} relation
   */
  public Expand expand(Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn, Rel input) {
    return expand(fieldsFn, Optional.empty(), input);
  }

  /**
   * Creates an expand relation that generates multiple output rows from each input row with output
   * field remapping.
   *
   * @param fieldsFn function to derive the expand fields from the input relation
   * @param remap the output field remapping specification
   * @param input the input relation to expand
   * @return a new {@link Expand} relation
   */
  public Expand expand(
      Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn, Rel.Remap remap, Rel input) {
    return expand(fieldsFn, Optional.of(remap), input);
  }

  private Expand expand(
      Function<Rel, Iterable<? extends Expand.ExpandField>> fieldsFn,
      Optional<Rel.Remap> remap,
      Rel input) {
    Iterable<? extends Expand.ExpandField> fields = fieldsFn.apply(input);
    return Expand.builder().input(input).fields(fields).remap(remap).build();
  }

  /**
   * Creates a set operation relation (UNION, INTERSECT, EXCEPT) over multiple input relations.
   *
   * @param op the set operation type
   * @param inputs the input relations to combine
   * @return a new {@link Set} relation
   */
  public Set set(Set.SetOp op, Rel... inputs) {
    return set(op, Optional.empty(), inputs);
  }

  /**
   * Creates a set operation relation (UNION, INTERSECT, EXCEPT) over multiple input relations with
   * output field remapping.
   *
   * @param op the set operation type
   * @param remap the output field remapping specification
   * @param inputs the input relations to combine
   * @return a new {@link Set} relation
   */
  public Set set(Set.SetOp op, Rel.Remap remap, Rel... inputs) {
    return set(op, Optional.of(remap), inputs);
  }

  private Set set(Set.SetOp op, Optional<Rel.Remap> remap, Rel... inputs) {
    return Set.builder().setOp(op).remap(remap).addAllInputs(Arrays.asList(inputs)).build();
  }

  /**
   * Creates a sort relation that orders rows based on sort fields.
   *
   * @param sortFieldFn function to derive the sort fields from the input relation
   * @param input the input relation to sort
   * @return a new {@link Sort} relation
   */
  public Sort sort(Function<Rel, Iterable<? extends Expression.SortField>> sortFieldFn, Rel input) {
    return sort(sortFieldFn, Optional.empty(), input);
  }

  /**
   * Creates a sort relation that orders rows based on sort fields with output field remapping.
   *
   * @param sortFieldFn function to derive the sort fields from the input relation
   * @param remap the output field remapping specification
   * @param input the input relation to sort
   * @return a new {@link Sort} relation
   */
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
    Iterable<? extends Expression.SortField> condition = sortFieldFn.apply(input);
    return Sort.builder().input(input).sortFields(condition).remap(remap).build();
  }

  // Expressions

  /**
   * Creates a boolean literal expression.
   *
   * @param v the boolean value
   * @return a new {@link Expression.BoolLiteral}
   */
  public Expression.BoolLiteral bool(boolean v) {
    return Expression.BoolLiteral.builder().value(v).build();
  }

  /**
   * Creates a 32-bit integer literal expression.
   *
   * @param v the integer value
   * @return a new {@link Expression.I32Literal}
   */
  public Expression.I32Literal i32(int v) {
    return Expression.I32Literal.builder().value(v).build();
  }

  /**
   * Creates a 64-bit floating point literal expression.
   *
   * @param v the double value
   * @return a new {@link Expression.FP64Literal}
   */
  public Expression.FP64Literal fp64(double v) {
    return Expression.FP64Literal.builder().value(v).build();
  }

  /**
   * Creates a string literal expression.
   *
   * @param s the string value
   * @return a new {@link Expression.StrLiteral}
   */
  public Expression.StrLiteral str(String s) {
    return Expression.StrLiteral.builder().value(s).build();
  }

  /**
   * Creates a cast expression that converts an expression to a different type.
   *
   * @param input the expression to cast
   * @param type the target type
   * @return a new {@link Cast} expression
   */
  public Expression cast(Expression input, Type type) {
    return Cast.builder()
        .input(input)
        .type(type)
        .failureBehavior(FailureBehavior.UNSPECIFIED)
        .build();
  }

  /**
   * Creates a field reference to a column in the input relation by index.
   *
   * @param input the input relation
   * @param index the zero-based index of the field to reference
   * @return a new {@link FieldReference}
   */
  public FieldReference fieldReference(Rel input, int index) {
    return FieldReference.newInputRelReference(index, input);
  }

  /**
   * Creates multiple field references to columns in the input relation by indexes.
   *
   * @param input the input relation
   * @param indexes the zero-based indexes of the fields to reference
   * @return a list of {@link FieldReference} objects
   */
  public List<FieldReference> fieldReferences(Rel input, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(input, index))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Creates a field reference for a join operation by index across both left and right relations.
   * The index spans across the combined schema of both join inputs, where fields from the left
   * relation come first, followed by fields from the right relation.
   *
   * @param inputs the join inputs containing both left and right relations
   * @param index the zero-based field index across the combined schema of both relations
   * @return a {@link FieldReference} pointing to the specified field in the join context
   */
  public FieldReference fieldReference(JoinInput inputs, int index) {
    final List<Rel> inputsList = new LinkedList<>();
    inputsList.add(inputs.left);
    inputsList.add(inputs.right);
    return FieldReference.newInputRelReference(index, inputsList);
  }

  /**
   * Creates a field reference to a column across multiple input relations by index.
   *
   * @param inputs the list of input relations
   * @param index the zero-based index of the field across all input relations
   * @return a new {@link FieldReference}
   */
  public FieldReference fieldReference(List<Rel> inputs, int index) {
    return FieldReference.newInputRelReference(index, inputs);
  }

  /**
   * Creates multiple field references for a join operation by indexes across both left and right
   * relations. Each index spans across the combined schema of both join inputs, where fields from
   * the left relation come first, followed by fields from the right relation.
   *
   * @param inputs the join inputs containing both left and right relations
   * @param indexes the zero-based field indexes across the combined schema of both relations
   * @return a list of {@link FieldReference} objects pointing to the specified fields in the join
   *     context
   */
  public List<FieldReference> fieldReferences(JoinInput inputs, int... indexes) {
    final List<Rel> inputsList = new LinkedList<>();
    inputsList.add(inputs.left);
    inputsList.add(inputs.right);
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(inputsList, index))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Creates multiple field references to columns across multiple input relations by indexes.
   *
   * @param inputs the list of input relations
   * @param indexes the zero-based indexes of the fields across all input relations
   * @return a list of {@link FieldReference} objects
   */
  public List<FieldReference> fieldReferences(List<Rel> inputs, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(index -> fieldReference(inputs, index))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Creates an if-then-else expression with multiple conditional branches.
   *
   * @param ifClauses the list of if-then clauses to evaluate
   * @param elseClause the expression to return if no if-clause matches
   * @return a new {@link IfThen} expression
   */
  public IfThen ifThen(Iterable<? extends IfClause> ifClauses, Expression elseClause) {
    return IfThen.builder().addAllIfClauses(ifClauses).elseClause(elseClause).build();
  }

  /**
   * Creates an if-clause that pairs a condition with a result expression.
   *
   * @param condition the boolean condition to evaluate
   * @param then the expression to return if the condition is true
   * @return a new {@link IfClause}
   */
  public IfClause ifClause(Expression condition, Expression then) {
    return IfClause.builder().condition(condition).then(then).build();
  }

  /**
   * Creates a single-or-list expression that checks if a condition matches any of the provided
   * options.
   *
   * @param condition the expression to match against
   * @param options the list of possible values to match
   * @return a new {@link SingleOrList} expression
   */
  public Expression singleOrList(Expression condition, Expression... options) {
    return SingleOrList.builder().condition(condition).addOptions(options).build();
  }

  /**
   * Creates an IN predicate expression that checks if any needle values exist in the haystack
   * relation.
   *
   * @param haystack the relation to search within
   * @param needles the values to search for
   * @return a new {@link Expression.InPredicate}
   */
  public Expression.InPredicate inPredicate(Rel haystack, Expression... needles) {
    return Expression.InPredicate.builder()
        .addAllNeedles(Arrays.asList(needles))
        .haystack(haystack)
        .build();
  }

  /**
   * Creates a list of sort fields from field indexes with ascending order and nulls last.
   *
   * @param input the input relation
   * @param indexes the zero-based indexes of the fields to sort by
   * @return a list of {@link Expression.SortField} objects
   */
  public List<Expression.SortField> sortFields(Rel input, int... indexes) {
    return Arrays.stream(indexes)
        .mapToObj(
            index ->
                Expression.SortField.builder()
                    .expr(FieldReference.newInputRelReference(index, input))
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build())
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Creates a sort field with a specified expression and sort direction.
   *
   * @param expression the expression to sort by
   * @param sortDirection the sort direction (ASC/DESC with nulls first/last)
   * @return a new {@link Expression.SortField}
   */
  public Expression.SortField sortField(
      Expression expression, Expression.SortDirection sortDirection) {
    return Expression.SortField.builder().expr(expression).direction(sortDirection).build();
  }

  /**
   * Creates a switch clause that pairs a literal condition with a result expression.
   *
   * @param condition the literal value to match against
   * @param then the expression to return if the condition matches
   * @return a new {@link SwitchClause}
   */
  public SwitchClause switchClause(Expression.Literal condition, Expression then) {
    return SwitchClause.builder().condition(condition).then(then).build();
  }

  /**
   * Creates a switch expression that matches a value against multiple cases.
   *
   * @param match the expression to match against
   * @param clauses the list of switch clauses to evaluate
   * @param defaultClause the expression to return if no clause matches
   * @return a new {@link Switch} expression
   */
  public Switch switchExpression(
      Expression match, Iterable<? extends SwitchClause> clauses, Expression defaultClause) {
    return Switch.builder()
        .match(match)
        .addAllSwitchClauses(clauses)
        .defaultClause(defaultClause)
        .build();
  }

  // Aggregate Functions

  /**
   * Creates an aggregate function invocation with specified arguments.
   *
   * @param urn the URN of the extension containing the function
   * @param key the function key (name and signature)
   * @param outputType the output type of the function
   * @param args the arguments to pass to the function
   * @return a new {@link AggregateFunctionInvocation}
   */
  public AggregateFunctionInvocation aggregateFn(
      String urn, String key, Type outputType, Expression... args) {
    SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(SimpleExtension.FunctionAnchor.of(urn, key));
    return AggregateFunctionInvocation.builder()
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .outputType(outputType)
        .declaration(declaration)
        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
        .invocation(Expression.AggregationInvocation.ALL)
        .build();
  }

  /**
   * Creates a grouping from field indexes in the input relation.
   *
   * @param input the input relation
   * @param indexes the zero-based indexes of the fields to group by
   * @return a new {@link Aggregate.Grouping}
   */
  public Aggregate.Grouping grouping(Rel input, int... indexes) {
    List<FieldReference> columns = fieldReferences(input, indexes);
    return Aggregate.Grouping.builder().addAllExpressions(columns).build();
  }

  /**
   * Creates a grouping from expressions.
   *
   * @param expressions the expressions to group by
   * @return a new {@link Aggregate.Grouping}
   */
  public Aggregate.Grouping grouping(Expression... expressions) {
    return Aggregate.Grouping.builder().addExpressions(expressions).build();
  }

  /**
   * Creates a COUNT aggregate measure for a specific field.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to count
   * @return a new {@link Aggregate.Measure} representing COUNT
   */
  public Aggregate.Measure count(Rel input, int field) {
    SimpleExtension.AggregateFunctionVariant declaration =
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

  /**
   * Creates a MIN aggregate measure for a specific field.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to find the minimum value
   * @return a new {@link Aggregate.Measure} representing MIN
   */
  public Aggregate.Measure min(Rel input, int field) {
    return min(fieldReference(input, field));
  }

  /**
   * Creates a MIN aggregate measure for an expression.
   *
   * @param expr the expression to find the minimum value
   * @return a new {@link Aggregate.Measure} representing MIN
   */
  public Aggregate.Measure min(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "min",
        // min output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  /**
   * Creates a MAX aggregate measure for a specific field.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to find the maximum value
   * @return a new {@link Aggregate.Measure} representing MAX
   */
  public Aggregate.Measure max(Rel input, int field) {
    return max(fieldReference(input, field));
  }

  /**
   * Creates a MAX aggregate measure for an expression.
   *
   * @param expr the expression to find the maximum value
   * @return a new {@link Aggregate.Measure} representing MAX
   */
  public Aggregate.Measure max(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "max",
        // max output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  /**
   * Creates an AVG aggregate measure for a specific field.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to calculate the average
   * @return a new {@link Aggregate.Measure} representing AVG
   */
  public Aggregate.Measure avg(Rel input, int field) {
    return avg(fieldReference(input, field));
  }

  /**
   * Creates an AVG aggregate measure for an expression.
   *
   * @param expr the expression to calculate the average
   * @return a new {@link Aggregate.Measure} representing AVG
   */
  public Aggregate.Measure avg(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "avg",
        // avg output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  /**
   * Creates a SUM aggregate measure for a specific field.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to sum
   * @return a new {@link Aggregate.Measure} representing SUM
   */
  public Aggregate.Measure sum(Rel input, int field) {
    return sum(fieldReference(input, field));
  }

  /**
   * Creates a SUM aggregate measure for an expression.
   *
   * @param expr the expression to sum
   * @return a new {@link Aggregate.Measure} representing SUM
   */
  public Aggregate.Measure sum(Expression expr) {
    return singleArgumentArithmeticAggregate(
        expr,
        "sum",
        // sum output is always nullable
        TypeCreator.asNullable(expr.getType()));
  }

  /**
   * Creates a SUM0 aggregate measure for a specific field that returns 0 for empty sets.
   *
   * @param input the input relation
   * @param field the zero-based index of the field to sum
   * @return a new {@link Aggregate.Measure} representing SUM0
   */
  public Aggregate.Measure sum0(Rel input, int field) {
    return sum(fieldReference(input, field));
  }

  /**
   * Creates a SUM0 aggregate measure for an expression that returns 0 for empty sets.
   *
   * @param expr the expression to sum
   * @return a new {@link Aggregate.Measure} representing SUM0
   */
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
    SimpleExtension.AggregateFunctionVariant declaration =
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

  /**
   * Creates a negate scalar function that returns the negation of the input expression.
   *
   * @param expr the expression to negate
   * @return a new {@link Expression.ScalarFunctionInvocation} representing negation
   */
  public Expression.ScalarFunctionInvocation negate(Expression expr) {
    // output type of negate is the same as the input type
    Type outputType = expr.getType();
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        String.format("negate:%s", ToTypeString.apply(outputType)),
        outputType,
        expr);
  }

  /**
   * Creates an addition scalar function that adds two expressions.
   *
   * @param left the left operand
   * @param right the right operand
   * @return a new {@link Expression.ScalarFunctionInvocation} representing addition
   */
  public Expression.ScalarFunctionInvocation add(Expression left, Expression right) {
    return arithmeticFunction("add", left, right);
  }

  /**
   * Creates a subtraction scalar function that subtracts the right expression from the left.
   *
   * @param left the left operand
   * @param right the right operand
   * @return a new {@link Expression.ScalarFunctionInvocation} representing subtraction
   */
  public Expression.ScalarFunctionInvocation subtract(Expression left, Expression right) {
    return arithmeticFunction("subtract", left, right);
  }

  /**
   * Creates a multiplication scalar function that multiplies two expressions.
   *
   * @param left the left operand
   * @param right the right operand
   * @return a new {@link Expression.ScalarFunctionInvocation} representing multiplication
   */
  public Expression.ScalarFunctionInvocation multiply(Expression left, Expression right) {
    return arithmeticFunction("multiply", left, right);
  }

  /**
   * Creates a division scalar function that divides the left expression by the right.
   *
   * @param left the left operand (dividend)
   * @param right the right operand (divisor)
   * @return a new {@link Expression.ScalarFunctionInvocation} representing division
   */
  public Expression.ScalarFunctionInvocation divide(Expression left, Expression right) {
    return arithmeticFunction("divide", left, right);
  }

  private Expression.ScalarFunctionInvocation arithmeticFunction(
      String fname, Expression left, Expression right) {
    String leftTypeStr = ToTypeString.apply(left.getType());
    String rightTypeStr = ToTypeString.apply(right.getType());
    String key = String.format("%s:%s_%s", fname, leftTypeStr, rightTypeStr);

    boolean isOutputNullable = left.getType().nullable() || right.getType().nullable();
    Type outputType = left.getType();
    outputType =
        isOutputNullable
            ? TypeCreator.asNullable(outputType)
            : TypeCreator.asNotNullable(outputType);

    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, key, outputType, left, right);
  }

  /**
   * Creates an equality comparison scalar function that checks if two expressions are equal.
   *
   * @param left the left operand
   * @param right the right operand
   * @return a new {@link Expression.ScalarFunctionInvocation} representing equality comparison
   */
  public Expression.ScalarFunctionInvocation equal(Expression left, Expression right) {
    return scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "equal:any_any", R.BOOLEAN, left, right);
  }

  /**
   * Creates a logical AND scalar function that returns true if all arguments are true.
   *
   * @param args the boolean expressions to AND together
   * @return a new {@link Expression.ScalarFunctionInvocation} representing logical AND
   */
  public Expression.ScalarFunctionInvocation and(Expression... args) {
    // If any arg is nullable, the output of and is potentially nullable
    // For example: false and null = null
    boolean isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
    Type outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "and:bool", outputType, args);
  }

  /**
   * Creates a logical OR scalar function that returns true if any argument is true.
   *
   * @param args the boolean expressions to OR together
   * @return a new {@link Expression.ScalarFunctionInvocation} representing logical OR
   */
  public Expression.ScalarFunctionInvocation or(Expression... args) {
    // If any arg is nullable, the output of or is potentially nullable
    // For example: false or null = null
    boolean isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
    Type outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
    return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "or:bool", outputType, args);
  }

  /**
   * Creates a scalar function invocation with specified arguments.
   *
   * @param urn the URN of the extension containing the function
   * @param key the function key (name and signature)
   * @param outputType the output type of the function
   * @param args the arguments to pass to the function
   * @return a new {@link Expression.ScalarFunctionInvocation}
   */
  public Expression.ScalarFunctionInvocation scalarFn(
      String urn, String key, Type outputType, FunctionArg... args) {
    SimpleExtension.ScalarFunctionVariant declaration =
        extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(urn, key));
    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
        .build();
  }

  /**
   * Creates a window function invocation with specified arguments and window bounds.
   *
   * @param urn the URN of the extension containing the function
   * @param key the function key (name and signature)
   * @param outputType the output type of the function
   * @param aggregationPhase the aggregation phase
   * @param invocation the aggregation invocation mode
   * @param boundsType the type of window bounds
   * @param lowerBound the lower bound of the window
   * @param upperBound the upper bound of the window
   * @param args the arguments to pass to the function
   * @return a new {@link Expression.WindowFunctionInvocation}
   */
  public Expression.WindowFunctionInvocation windowFn(
      String urn,
      String key,
      Type outputType,
      Expression.AggregationPhase aggregationPhase,
      Expression.AggregationInvocation invocation,
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      Expression... args) {
    SimpleExtension.WindowFunctionVariant declaration =
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

  /**
   * Creates a user-defined type with the specified URN and type name.
   *
   * @param urn the URN of the extension containing the type
   * @param typeName the name of the user-defined type
   * @return a new {@link Type.UserDefined}
   */
  public Type.UserDefined userDefinedType(String urn, String typeName) {
    return Type.UserDefined.builder().urn(urn).name(typeName).nullable(false).build();
  }

  // Misc

  /**
   * Creates a plan root from a relation.
   *
   * @param rel the relation to use as the root
   * @return a new {@link Plan.Root}
   */
  public Plan.Root root(Rel rel) {
    return Plan.Root.builder().input(rel).build();
  }

  /**
   * Creates a plan from a plan root with default execution behavior.
   *
   * <p>The plan is created with {@link VariableEvaluationMode#VARIABLE_EVALUATION_MODE_PER_PLAN} as
   * the default variable evaluation mode. To specify a custom execution behavior, use {@link
   * #plan(Plan.ExecutionBehavior, Plan.Root)} instead.
   *
   * @param root the plan root
   * @return a new {@link Plan}
   */
  public Plan plan(Plan.Root root) {
    return plan(
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
            .build(),
        root);
  }

  /**
   * Creates a plan from a plan root with custom execution behavior.
   *
   * @param executionBehavior the execution behavior for the plan
   * @param root the plan root
   * @return a new {@link Plan}
   */
  public Plan plan(Plan.ExecutionBehavior executionBehavior, Plan.Root root) {
    return Plan.builder().executionBehavior(executionBehavior).addRoots(root).build();
  }

  /**
   * Creates a plan from multiple plan roots with custom execution behavior.
   *
   * @param executionBehavior the execution behavior for the plan
   * @param roots the plan roots
   * @return a new {@link Plan}
   */
  public Plan plan(Plan.ExecutionBehavior executionBehavior, Plan.Root... roots) {
    return Plan.builder().executionBehavior(executionBehavior).roots(Arrays.asList(roots)).build();
  }

  /**
   * Creates a plan from multiple plan roots with custom execution behavior.
   *
   * @param executionBehavior the execution behavior for the plan
   * @param roots the plan roots as an iterable
   * @return a new {@link Plan}
   */
  public Plan plan(Plan.ExecutionBehavior executionBehavior, Iterable<Plan.Root> roots) {
    return Plan.builder().executionBehavior(executionBehavior).roots(roots).build();
  }

  /**
   * Creates a field remapping specification from field indexes.
   *
   * @param fields the field indexes to include in the output
   * @return a new {@link Rel.Remap}
   */
  public Rel.Remap remap(Integer... fields) {
    return Rel.Remap.of(Arrays.asList(fields));
  }

  /**
   * Creates a scalar subquery expression that returns a single value from a relation.
   *
   * @param input the input relation that must return exactly one row and one column
   * @param type the type of the scalar value
   * @return a new {@link Expression.ScalarSubquery}
   */
  public Expression scalarSubquery(Rel input, Type type) {
    return Expression.ScalarSubquery.builder().input(input).type(type).build();
  }

  /**
   * Creates an EXISTS predicate expression that checks if a relation returns any rows.
   *
   * @param rel the relation to check for existence of rows
   * @return a new {@link Expression.SetPredicate} representing EXISTS
   */
  public Expression exists(Rel rel) {
    return Expression.SetPredicate.builder()
        .tuples(rel)
        .predicateOp(PredicateOp.PREDICATE_OP_EXISTS)
        .build();
  }
}
