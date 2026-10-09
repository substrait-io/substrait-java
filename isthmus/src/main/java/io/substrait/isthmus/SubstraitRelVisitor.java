package io.substrait.isthmus;

import com.google.common.collect.Iterables;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.LiteralConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Aggregate.Grouping;
import io.substrait.relation.Aggregate.Measure;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.ImmutableFetch;
import io.substrait.relation.ImmutableMeasure.Builder;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

/**
 * SubstraitRelVisitor is used to convert Calcite {@link RelNode}s to Substrait {@link Rel}s.
 *
 * <p>Conversion behaviours can be customized by using a {@link ConverterProvider} and/or extending
 * this class
 */
@SuppressWarnings("UnstableApiUsage")
@Value.Enclosing
public class SubstraitRelVisitor extends RelNodeVisitor<Rel, RuntimeException> {

  private static final Expression.BoolLiteral TRUE = ExpressionCreator.bool(false, true);

  /** Converter for Calcite {@link RexNode} to Substrait {@link Expression}. */
  protected final RexExpressionConverter rexExpressionConverter;

  /** Converter for {@link AggregateCall} to Substrait aggregate invocation. */
  protected final AggregateFunctionConverter aggregateFunctionConverter;

  /** Converter for Calcite {@link RelDataType} to Substrait {@link Type}. */
  protected final TypeConverter typeConverter;

  private OuterReferenceResolver outerReferenceResolver;

  /** Rex builder for creating Rex expressions during conversion. */
  protected RexBuilder rexBuilder;

  /**
   * Creates a new SubstraitRelVisitor with the specified type factory and extensions.
   *
   * @param typeFactory the Calcite type factory
   * @param extensions the Substrait extension collection
   * @deprecated Use {@link SubstraitRelVisitor#SubstraitRelVisitor(ConverterProvider)}
   */
  @Deprecated
  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    this(new ConverterProvider(extensions, typeFactory));
  }

  /**
   * Creates a new SubstraitRelVisitor with the specified converter provider.
   *
   * @param converterProvider the converter provider containing configuration and converters
   */
  public SubstraitRelVisitor(ConverterProvider converterProvider) {
    this.typeConverter = converterProvider.getTypeConverter();
    this.aggregateFunctionConverter = converterProvider.getAggregateFunctionConverter();
    this.rexExpressionConverter = converterProvider.getRexExpressionConverter(this);
    this.rexBuilder = new RexBuilder(converterProvider.getTypeFactory());
  }

  /**
   * Converts a {@link RexNode} to a Substrait {@link Expression}.
   *
   * @param node Rex expression node
   * @return Substrait expression
   */
  protected Expression toExpression(RexNode node) {
    return node.accept(rexExpressionConverter);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.TableScan}.
   *
   * @param scan Calcite table scan
   * @return Substrait named scan
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.TableScan scan) {
    NamedStruct type = typeConverter.toNamedStruct(scan.getRowType());
    return NamedScan.builder()
        .initialSchema(type)
        .addAllNames(scan.getTable().getQualifiedName())
        .build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.TableFunctionScan}.
   *
   * @param scan Calcite table function scan
   * @return Converted relation or {@code super.visit(scan)}
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.TableFunctionScan scan) {
    return super.visit(scan);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Values}.
   *
   * @param values Calcite values relation
   * @return Substrait scan (empty or virtual table)
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Values values) {
    NamedStruct type = typeConverter.toNamedStruct(values.getRowType());
    List<RelDataTypeField> rowFields = values.getRowType().getFieldList();

    LiteralConverter literalConverter = new LiteralConverter(typeConverter);
    List<Expression.NestedStruct> structs =
        values.getTuples().stream()
            .map(
                list -> {
                  // Calcite may infer a narrower type for a tuple literal than for its row field.
                  // Virtual table rows must use the row field type.
                  List<Expression> fields =
                      IntStream.range(0, list.size())
                          .mapToObj(
                              i ->
                                  literalConverter.convert(list.get(i), rowFields.get(i).getType()))
                          .collect(Collectors.toUnmodifiableList());
                  return ExpressionCreator.nestedStruct(false, fields);
                })
            .collect(Collectors.toUnmodifiableList());
    return VirtualTableScan.builder().initialSchema(type).addAllRows(structs).build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Filter}.
   *
   * @param filter Calcite filter relation
   * @return Substrait filter
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Filter filter) {
    Expression condition = toExpression(filter.getCondition());
    return Filter.builder().condition(condition).input(apply(filter.getInput())).build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Calc}.
   *
   * @param calc Calcite calc relation
   * @return Converted relation
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Calc calc) {
    return super.visit(calc);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Project}.
   *
   * @param project Calcite project relation
   * @return Substrait project
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Project project) {
    // An identity projection (input refs in order, with matching types) passes every input field
    // through unchanged and only ever renames fields. Substrait carries output names on Plan.Root
    // and, for a single relation, in its hint -- neither of them a Project, and this conversion
    // writes no hints -- so emitting one here is redundant: the reverse conversion drops it, which
    // leaves the two sides structurally different and breaks round-trips. Skip it instead. Output
    // names are still preserved because convert(RelRoot, ...) takes them from validatedRowType.
    if (RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
      return apply(project.getInput());
    }

    List<Expression> expressions =
        project.getProjects().stream()
            .map(this::toExpression)
            .collect(java.util.stream.Collectors.toList());

    // if there are no input fields, no remap is necessary
    if (project.getInput().getRowType().getFieldCount() == 0) {
      return Project.builder().expressions(expressions).input(apply(project.getInput())).build();
    }

    // todo: eliminate the remaining excessive projects. Identity projects are dropped above; a
    // projection whose expressions are all input refs (a permutation or column pruning) could
    // likewise be expressed as a remap over the input rather than copied expressions.
    return Project.builder()
        .remap(
            Rel.Remap.offset(project.getInput().getRowType().getFieldCount(), expressions.size()))
        .expressions(expressions)
        .input(apply(project.getInput()))
        .build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Join}.
   *
   * @param join Calcite join relation
   * @return Substrait join or cross
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Join join) {
    Rel left = apply(join.getLeft());
    Rel right = apply(join.getRight());
    Expression condition = toExpression(join.getCondition());
    JoinType joinType = asJoinType(join);

    // An INNER JOIN with a join condition of TRUE can be encoded as a Substrait Cross relation
    if (joinType == Join.JoinType.INNER && TRUE.equals(condition)) {
      return Cross.builder().left(left).right(right).build();
    }
    return Join.builder().condition(condition).joinType(joinType).left(left).right(right).build();
  }

  private Join.JoinType asJoinType(org.apache.calcite.rel.core.Join join) {
    JoinRelType type = join.getJoinType();

    if (type == JoinRelType.INNER) {
      return Join.JoinType.INNER;
    } else if (type == JoinRelType.LEFT) {
      return Join.JoinType.LEFT;
    } else if (type == JoinRelType.RIGHT) {
      return Join.JoinType.RIGHT;
    } else if (type == JoinRelType.FULL) {
      return Join.JoinType.OUTER;
    } else if (type == JoinRelType.SEMI) {
      return Join.JoinType.LEFT_SEMI;
    } else if (type == JoinRelType.ANTI) {
      return Join.JoinType.LEFT_ANTI;
    }

    throw new UnsupportedOperationException("Unsupported join type: " + join.getJoinType());
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Correlate}.
   *
   * @param correlate Calcite correlate relation
   * @return Converted relation
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Correlate correlate) {
    // left input of correlated-join is similar to the left input of a logical join
    apply(correlate.getLeft());

    // right input of correlated-join is similar to a correlated sub-query
    apply(correlate.getRight());

    return super.visit(correlate);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Union}.
   *
   * @param union Calcite union relation
   * @return Substrait set-union
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Union union) {
    List<Rel> inputs = apply(union.getInputs());
    Set.SetOp setOp = union.all ? Set.SetOp.UNION_ALL : Set.SetOp.UNION_DISTINCT;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Intersect}.
   *
   * @param intersect Calcite intersect relation
   * @return Substrait set-intersection
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Intersect intersect) {
    List<Rel> inputs = apply(intersect.getInputs());
    Set.SetOp setOp =
        intersect.all ? Set.SetOp.INTERSECTION_MULTISET_ALL : Set.SetOp.INTERSECTION_MULTISET;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Minus}.
   *
   * @param minus Calcite minus relation
   * @return Substrait set-minus
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Minus minus) {
    List<Rel> inputs = apply(minus.getInputs());
    Set.SetOp setOp = minus.all ? Set.SetOp.MINUS_PRIMARY_ALL : Set.SetOp.MINUS_PRIMARY;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Aggregate}.
   *
   * @param aggregate Calcite aggregate relation
   * @return Substrait aggregate
   * @throws IllegalStateException if unexpected remap state is encountered.
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Aggregate aggregate) {
    // Substrait's std_dev/variance functions only define fp32/fp64 signatures. If a statistical
    // aggregate has a non-floating-point argument, rewrite the aggregate to cast that argument to
    // fp64 and cast the result back to the type Calcite inferred, then convert the rewritten plan
    // through the normal path. The rewrite is idempotent (fp32/fp64 arguments are left untouched),
    // so it terminates when the converted plan is re-converted.
    RelNode rewritten = castStatisticalAggregatesToFloatingPoint(aggregate);
    if (rewritten != aggregate) {
      return apply(rewritten);
    }

    Rel input = apply(aggregate.getInput());
    Stream<ImmutableBitSet> sets;
    if (aggregate.groupSets != null) {
      sets = aggregate.groupSets.stream();
    } else {
      sets = Stream.of(aggregate.getGroupSet());
    }

    List<Grouping> groupings =
        sets.filter(s -> s != null).map(s -> fromGroupSet(s, input)).collect(Collectors.toList());

    List<AggregateCall> calls = aggregate.getAggCallList();
    boolean hasGrouping =
        calls.stream().anyMatch(c -> c.getAggregation().getKind() == SqlKind.GROUPING);
    boolean hasSpecialCalls = calls.stream().anyMatch(SubstraitRelVisitor::isGroupingOrLiteralCall);
    boolean hasLiteral =
        calls.stream().anyMatch(c -> c.getAggregation().getKind() == SqlKind.LITERAL_AGG);
    if (hasLiteral && groupings.size() > 1) {
      throw new UnsupportedOperationException(
          "LITERAL_AGG combined with GROUPING SETS / CUBE / ROLLUP is not supported");
    }

    List<Measure> measures =
        calls.stream()
            .filter(c -> !isGroupingOrLiteralCall(c))
            .map(c -> fromAggCall(aggregate.getInput(), input.getRecordType(), c))
            .collect(Collectors.toList());
    ImmutableAggregate.Builder builder =
        Aggregate.builder().input(input).addAllGroupings(groupings).addAllMeasures(measures);
    List<Integer> mapping = new ArrayList<>(calciteGroupingOrder(groupings));
    // The distinct grouping expressions across the sets. Calcite emits one column per bit of
    // getGroupSet() instead, so a Calcite group set wider than the union of its grouping sets
    // shifts every measure. Substrait has no such aggregate: each grouping expression must occur
    // in at least one grouping set.
    int groupingFieldCount = mapping.size();
    int groupingSetIndex = groupingFieldCount + measures.size();
    for (int call = 0; call < measures.size(); call++) {
      mapping.add(groupingFieldCount + call);
    }
    if (groupings.size() > 1) {
      // Keep the implicit ordinal only while deriving GROUPING values. The project below restores
      // the original Calcite schema; ordinary aggregates emit just their keys and measures.
      if (hasGrouping) {
        mapping.add(groupingSetIndex);
      }
      builder.remap(Remap.of(mapping));
    }
    Rel aggRel = builder.build();
    if (!hasSpecialCalls) {
      return aggRel;
    }

    List<Expression> output = new ArrayList<>();
    for (int field = 0; field < groupingFieldCount; field++) {
      output.add(FieldReference.newInputRelReference(field, aggRel));
    }
    int measureIndex = groupingFieldCount;
    for (AggregateCall call : calls) {
      switch (call.getAggregation().getKind()) {
        case GROUPING:
          output.add(groupingValue(call, aggregate.getGroupSets(), aggRel, groupingSetIndex));
          break;
        case GROUP_ID:
          // RelBuilder expands repeated grouping sets into UNION ALL branches, and a Calcite
          // Aggregate only asserts that its sets are distinct -- so a directly built one can
          // still carry repetitions, which GROUP_ID is the value that distinguishes.
          if (aggregate.getGroupSets().stream().distinct().count()
              < aggregate.getGroupSets().size()) {
            throw new UnsupportedOperationException(
                "GROUP_ID over repeated grouping sets is not supported");
          }
          output.add(ExpressionCreator.i64(false, 0));
          break;
        case LITERAL_AGG:
          output.add(toExpression(Iterables.getOnlyElement(call.rexList)));
          break;
        default:
          output.add(FieldReference.newInputRelReference(measureIndex++, aggRel));
      }
    }
    return Project.builder()
        .input(aggRel)
        .expressions(output)
        .remap(Remap.offset(aggRel.getRecordType().fields().size(), output.size()))
        .build();
  }

  private static boolean isGroupingOrLiteralCall(AggregateCall call) {
    SqlKind kind = call.getAggregation().getKind();
    return kind == SqlKind.GROUPING || kind == SqlKind.GROUP_ID || kind == SqlKind.LITERAL_AGG;
  }

  /** Computes SQL's membership bit mask from Substrait's declared grouping-set ordinal. */
  private static Expression groupingValue(
      AggregateCall call, List<ImmutableBitSet> sets, Rel aggregate, int groupingSetIndex) {
    if (call.getArgList().size() >= Long.SIZE) {
      throw new UnsupportedOperationException(
          String.format(
              "%s over %d arguments: the mask does not fit the i64 it is returned in, which holds"
                  + " at most %d arguments",
              call.getAggregation().getName(), call.getArgList().size(), Long.SIZE - 1));
    }
    List<Expression.SwitchClause> clauses = new ArrayList<>();
    Expression value = ExpressionCreator.i64(false, 0);
    for (int index = 0; index < sets.size(); index++) {
      long mask = 0;
      for (int argument : call.getArgList()) {
        mask = (mask << 1) | (sets.get(index).get(argument) ? 0 : 1);
      }
      value = ExpressionCreator.i64(false, mask);
      if (index < sets.size() - 1) {
        clauses.add(ExpressionCreator.switchClause(ExpressionCreator.i32(false, index), value));
      }
    }
    return sets.size() <= 1
        ? value
        : ExpressionCreator.switchStatement(
            FieldReference.newInputRelReference(groupingSetIndex, aggregate), value, clauses);
  }

  /**
   * Returns, for each grouping column of the converted Calcite aggregate, the position that column
   * holds in the output the Substrait aggregate declares.
   *
   * <p>substrait-java takes the grouping columns to be the distinct grouping expressions in the
   * order they first appear across the grouping sets, reconstructing the shared list the spec
   * orders them by; Calcite takes them from a bit set and so emits them ordered by field index.
   * Reading the result as an emit mapping presents the aggregate's output in Calcite's order.
   *
   * @param groupings the grouping sets of the converted aggregate
   * @return the declared position of each grouping column, in the order Calcite emits them
   */
  private static List<Integer> calciteGroupingOrder(List<Grouping> groupings) {
    List<Expression> declared =
        groupings.stream()
            .flatMap(grouping -> grouping.getExpressions().stream())
            .distinct()
            .collect(Collectors.toList());
    return declared.stream()
        .sorted(Comparator.comparingInt(SubstraitRelVisitor::groupingFieldOffset))
        .map(declared::indexOf)
        .collect(Collectors.toList());
  }

  /**
   * Returns the field the given grouping expression references.
   *
   * @param expression a grouping expression, as built by {@link #fromGroupSet(ImmutableBitSet,
   *     Rel)}
   * @return the offset of the field it references
   */
  private static int groupingFieldOffset(Expression expression) {
    FieldReference reference = (FieldReference) expression;
    return ((FieldReference.StructField) reference.segments().get(0)).offset();
  }

  Aggregate.Grouping fromGroupSet(ImmutableBitSet bitSet, Rel input) {
    List<Expression> references =
        bitSet.asList().stream()
            .map(i -> FieldReference.newInputRelReference(i, input))
            .collect(Collectors.toList());
    return Aggregate.Grouping.builder().addAllExpressions(references).build();
  }

  /**
   * Converts a Calcite {@link AggregateCall} to a Substrait {@link Aggregate.Measure}.
   *
   * <p>Statistical aggregate arguments have already been rewritten by {@link
   * #castStatisticalAggregatesToFloatingPoint(org.apache.calcite.rel.core.Aggregate)} before this
   * method runs. That rewrite appends fp64 input columns and re-points the affected calls.
   *
   * <p>The method also processes optional filter arguments (FILTER clauses) by converting them to
   * Substrait's preMeasureFilter representation.
   *
   * @param input the input relational node providing data to the aggregate operation
   * @param inputType the Substrait struct type representing the schema of the input relation
   * @param call the Calcite aggregate call to convert, containing the aggregate function,
   *     arguments, and optional filter
   * @return a Substrait {@link Aggregate.Measure} representing the aggregate function invocation
   *     with its configuration
   * @throws UnsupportedOperationException if the aggregate function cannot be converted to a
   *     Substrait representation (no matching function binding found)
   */
  Aggregate.Measure fromAggCall(RelNode input, Type.Struct inputType, AggregateCall call) {
    Optional<AggregateFunctionInvocation> invocation =
        aggregateFunctionConverter.convert(
            input, inputType, call, t -> t.accept(rexExpressionConverter));
    if (invocation.isEmpty()) {
      throw new UnsupportedOperationException("Unable to find binding for call " + call);
    }
    Builder builder = Aggregate.Measure.builder().function(invocation.get());
    if (call.filterArg != -1) {
      builder.preMeasureFilter(
          FieldReference.StructField.of(call.filterArg).constructOnRoot(inputType));
    }
    return builder.build();
  }

  private static boolean isStatisticalDistributionAggregate(SqlKind kind) {
    return kind == SqlKind.STDDEV_POP
        || kind == SqlKind.STDDEV_SAMP
        || kind == SqlKind.VAR_POP
        || kind == SqlKind.VAR_SAMP;
  }

  private boolean isFloatingPoint(RelDataType type) {
    Type substraitType = typeConverter.toSubstrait(type);
    return TypeCreator.NULLABLE.FP32.equalsIgnoringNullability(substraitType)
        || TypeCreator.NULLABLE.FP64.equalsIgnoringNullability(substraitType);
  }

  /**
   * Rewrites a Calcite aggregate so that statistical aggregate functions (STDDEV_POP, STDDEV_SAMP,
   * VAR_POP, VAR_SAMP) with non-floating-point arguments operate on fp64, since Substrait's {@code
   * std_dev} / {@code variance} functions only define fp32 and fp64 signatures.
   *
   * <p>For each statistical aggregate whose single argument is neither fp32 nor fp64 (e.g. an
   * integer or decimal column), the rewrite:
   *
   * <ol>
   *   <li>appends a {@code cast(arg AS fp64)} column to the aggregate's input (leaving the original
   *       column in place, so other aggregates over the same column are unaffected),
   *   <li>re-points the statistical aggregate at the appended column (its return type is re-derived
   *       over fp64), and
   *   <li>casts the aggregate's results back to the types Calcite originally inferred, via a
   *       projection on top, so the aggregate's output row type is preserved.
   * </ol>
   *
   * <p>The rewrite is idempotent: fp32/fp64 arguments are left untouched, so converting the
   * rewritten plan (whose statistical arguments are already fp64) produces no further rewrite and
   * the recursion in {@link #visit(org.apache.calcite.rel.core.Aggregate)} terminates. If no
   * argument needs casting, the aggregate is returned unchanged.
   *
   * @param aggregate the Calcite aggregate to inspect
   * @return {@code aggregate} unchanged, or a {@link LogicalProject} wrapping a rewritten aggregate
   */
  protected RelNode castStatisticalAggregatesToFloatingPoint(
      org.apache.calcite.rel.core.Aggregate aggregate) {
    RelNode input = aggregate.getInput();
    List<AggregateCall> calls = aggregate.getAggCallList();
    int inputFieldCount = input.getRowType().getFieldCount();

    // fp64 cast expressions to append to the input, and the source field each one casts (for reuse)
    List<RexNode> appendedCasts = new ArrayList<>();
    List<Integer> appendedSourceFields = new ArrayList<>();
    // per call: the appended column its argument should be re-pointed at, or -1 if unchanged
    List<Integer> rewrittenArgColumns = new ArrayList<>(calls.size());

    for (AggregateCall call : calls) {
      int rewrittenArgColumn = -1;
      if (isStatisticalDistributionAggregate(call.getAggregation().getKind())
          && call.getArgList().size() == 1) {
        int argIndex = call.getArgList().get(0);
        RelDataType argType = input.getRowType().getFieldList().get(argIndex).getType();
        if (!isFloatingPoint(argType)) {
          int existing = appendedSourceFields.indexOf(argIndex);
          if (existing >= 0) {
            rewrittenArgColumn = inputFieldCount + existing;
          } else {
            RelDataType fp64 =
                typeConverter.toCalcite(
                    rexBuilder.getTypeFactory(), Type.withNullability(argType.isNullable()).FP64);
            appendedCasts.add(rexBuilder.makeCast(fp64, rexBuilder.makeInputRef(input, argIndex)));
            appendedSourceFields.add(argIndex);
            rewrittenArgColumn = inputFieldCount + appendedCasts.size() - 1;
          }
        }
      }
      rewrittenArgColumns.add(rewrittenArgColumn);
    }

    if (appendedCasts.isEmpty()) {
      return aggregate;
    }

    // Extended input: all original columns (passthrough) followed by the appended fp64 casts.
    List<RexNode> inputProjects = new ArrayList<>(inputFieldCount + appendedCasts.size());
    for (int i = 0; i < inputFieldCount; i++) {
      inputProjects.add(rexBuilder.makeInputRef(input, i));
    }
    inputProjects.addAll(appendedCasts);
    RelNode extendedInput =
        LogicalProject.create(input, Collections.emptyList(), inputProjects, (List<String>) null);

    // Re-point the statistical calls at the appended fp64 columns (return type re-derived); leave
    // all other calls unchanged.
    List<AggregateCall> rewrittenCalls = new ArrayList<>(calls.size());
    for (int i = 0; i < calls.size(); i++) {
      AggregateCall call = calls.get(i);
      int rewrittenArgColumn = rewrittenArgColumns.get(i);
      if (rewrittenArgColumn < 0) {
        rewrittenCalls.add(call);
      } else {
        rewrittenCalls.add(
            AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                Collections.singletonList(rewrittenArgColumn),
                call.filterArg,
                call.distinctKeys,
                call.getCollation(),
                aggregate.getGroupCount(),
                extendedInput,
                /* type, null to re-derive over fp64 */ null,
                call.getName()));
      }
    }

    org.apache.calcite.rel.core.Aggregate rewrittenAggregate =
        aggregate.copy(
            aggregate.getTraitSet(),
            extendedInput,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            rewrittenCalls);

    // Cast the (now fp64) statistical results back to the types Calcite originally inferred,
    // preserving the aggregate's original output row type. Group keys and unaffected measures pass
    // through unchanged.
    RelDataType originalRowType = aggregate.getRowType();
    List<RexNode> outputProjects = new ArrayList<>(originalRowType.getFieldCount());
    for (int i = 0; i < originalRowType.getFieldCount(); i++) {
      RelDataType targetType = originalRowType.getFieldList().get(i).getType();
      RexNode ref = rexBuilder.makeInputRef(rewrittenAggregate, i);
      outputProjects.add(
          ref.getType().equals(targetType) ? ref : rexBuilder.makeCast(targetType, ref));
    }
    return LogicalProject.create(
        rewrittenAggregate, Collections.emptyList(), outputProjects, originalRowType);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Match}.
   *
   * @param match Calcite match relation
   * @return Converted relation
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Match match) {
    return super.visit(match);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Sort}.
   *
   * @param sort Calcite sort relation
   * @return Substrait sort/fetch chain
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Sort sort) {
    Rel input = apply(sort.getInput());
    Rel output = input;

    // The Calcite Sort relation combines sorting along with offset and fetch/limit
    // Sorting is applied BEFORE the offset and limit is are applied
    // Substrait splits this functionality into two different relations: SortRel, FetchRel
    // Add the SortRel to the relation tree first to match Calcite's application order
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      List<Expression.SortField> fields =
          sort.getCollation().getFieldCollations().stream()
              .map(t -> toSortField(t, input.getRecordType()))
              .collect(java.util.stream.Collectors.toList());
      output = Sort.builder().addAllSortFields(fields).input(output).build();
    }

    if (sort.fetch != null || sort.offset != null) {
      // Offset/count are expressions; pass the Calcite RexNodes through so non-literal (e.g.
      // dynamic-parameter) offset/count are preserved.
      ImmutableFetch.Builder builder = Fetch.builder().input(output);
      if (sort.offset != null) {
        builder.offset(toExpression(sort.offset));
      }
      if (sort.fetch != null) {
        builder.count(toExpression(sort.fetch));
      }
      output = builder.build();
    }

    return output;
  }

  /**
   * Converts a Calcite sort collation to a Substrait {@link Expression.SortField}.
   *
   * @param collation Calcite field collation
   * @param inputType Input record type
   * @return Substrait sort field
   */
  public static Expression.SortField toSortField(
      RelFieldCollation collation, Type.Struct inputType) {
    Expression.SortDirection direction = asSortDirection(collation);

    return Expression.SortField.builder()
        .expr(FieldReference.StructField.of(collation.getFieldIndex()).constructOnRoot(inputType))
        .direction(direction)
        .build();
  }

  private static Expression.SortDirection asSortDirection(RelFieldCollation collation) {
    RelFieldCollation.Direction direction = collation.direction;

    if (direction == Direction.STRICTLY_ASCENDING || direction == Direction.ASCENDING) {
      return collation.nullDirection == RelFieldCollation.NullDirection.LAST
          ? Expression.SortDirection.ASC_NULLS_LAST
          : Expression.SortDirection.ASC_NULLS_FIRST;
    } else if (direction == Direction.STRICTLY_DESCENDING || direction == Direction.DESCENDING) {
      return collation.nullDirection == RelFieldCollation.NullDirection.LAST
          ? Expression.SortDirection.DESC_NULLS_LAST
          : Expression.SortDirection.DESC_NULLS_FIRST;
    } else if (direction == Direction.CLUSTERED) {
      return Expression.SortDirection.CLUSTERED;
    }

    throw new IllegalArgumentException("Unsupported collation direction: " + direction);
  }

  /**
   * Converts a Calcite {@link org.apache.calcite.rel.core.Exchange}.
   *
   * @param exchange Calcite exchange relation
   * @return Converted relation
   */
  @Override
  public Rel visit(org.apache.calcite.rel.core.Exchange exchange) {
    return super.visit(exchange);
  }

  /**
   * Converts a Calcite {@link TableModify} (INSERT/DELETE/UPDATE).
   *
   * @param modify Calcite table modify node
   * @return Substrait write/update relation
   * @throws IllegalStateException if an update column is not found in the table schema.
   * @throws UnsupportedOperationException if an UPDATE input is not a chain of projections and
   *     filters over a scan of the target table with the row type of that table, or if flattening
   *     the chain into one condition and the assignments could change the result. That includes a
   *     correlation bound above a projection, a window that would range over different rows, a
   *     subquery or nondeterministic expression that would be duplicated or moved across a filter,
   *     and expression growth over the complexity limit.
   */
  @Override
  public Rel visit(TableModify modify) {
    switch (modify.getOperation()) {
      case INSERT:
      case DELETE:
        {
          final Rel input = apply(modify.getInput());
          final AbstractWriteRel.WriteOp op =
              modify.getOperation() == TableModify.Operation.INSERT
                  ? AbstractWriteRel.WriteOp.INSERT
                  : AbstractWriteRel.WriteOp.DELETE;

          final RelOptTable table = requireTable(modify);
          return NamedWrite.builder()
              .input(input)
              .tableSchema(typeConverter.toNamedStruct(table.getRowType()))
              .operation(op)
              .createMode(AbstractWriteRel.CreateMode.UNSPECIFIED)
              .outputMode(AbstractWriteRel.OutputMode.MODIFIED_RECORDS)
              .names(table.getQualifiedName())
              .build();
        }

      case UPDATE:
        {
          final RelOptTable table = requireTable(modify);

          RelNode input = modify.getInput();
          List<RexNode> conditions = new ArrayList<>();
          List<RelNode> correlationBindings = new ArrayList<>();
          List<RexNode> sourceExpressions =
              Optional.ofNullable(modify.getSourceExpressionList()).orElse(Collections.emptyList());
          while (input instanceof org.apache.calcite.rel.core.Project
              || input instanceof org.apache.calcite.rel.core.Filter) {
            if (outerReferenceResolver != null && !input.getVariablesSet().isEmpty()) {
              // A Filter or Project that declares correlation variables binds them to its input.
              RelNode binding = input.getInput(0);
              if (!(binding instanceof org.apache.calcite.rel.core.TableScan)
                  && !(binding instanceof org.apache.calcite.rel.core.Filter
                      && ((org.apache.calcite.rel.core.Filter) binding).getInput()
                          instanceof org.apache.calcite.rel.core.TableScan)) {
                throw new UnsupportedOperationException(
                    "UPDATE cannot remove an input that binds a correlated subquery");
              }
              correlationBindings.add(binding);
            }
            if (input instanceof org.apache.calcite.rel.core.Project) {
              org.apache.calcite.rel.core.Project project =
                  (org.apache.calcite.rel.core.Project) input;
              List<RexNode> expressions = new ArrayList<>(sourceExpressions);
              expressions.addAll(conditions);
              int[] referenceCounts = new int[project.getProjects().size()];
              new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                  referenceCounts[inputRef.getIndex()]++;
                  return inputRef;
                }
              }.apply(expressions);
              for (int i = 0; i < referenceCounts.length; i++) {
                RexNode expression = project.getProjects().get(i);
                // A window can only become the one assignment that references it. This check
                // finds a filter above the window. The check after the walk finds one below it.
                if (referenceCounts[i] > 0
                    && RexOver.containsOver(expression)
                    && (referenceCounts[i] > 1
                        || !conditions.isEmpty()
                        || RexOver.containsOver(expressions, null))) {
                  throw new UnsupportedOperationException(
                      "UPDATE cannot flatten a window projection");
                }
                if (referenceCounts[i] > 1 && RexUtil.SubQueryFinder.find(expression) != null) {
                  throw new UnsupportedOperationException(
                      "UPDATE cannot duplicate a projected subquery");
                }
                if (referenceCounts[i] > 0
                    && (!conditions.isEmpty() || referenceCounts[i] > 1)
                    && !isDeterministic(expression)) {
                  throw new UnsupportedOperationException(
                      "UPDATE cannot flatten a nondeterministic projection");
                }
              }
              // The size limit of RelOptUtil.pushPastProjectUnlessBloat, which also refuses every
              // window expression above a Project that contains a window.
              List<RexNode> flattened = RelOptUtil.pushPastProject(expressions, project);
              if (RexUtil.nodeCount(flattened)
                  > RexUtil.nodeCount(expressions)
                      + RexUtil.nodeCount(project.getProjects())
                      + RelOptUtil.DEFAULT_BLOAT) {
                throw new UnsupportedOperationException(
                    "UPDATE projection flattening would exceed the expression complexity limit");
              }
              int assignmentCount = sourceExpressions.size();
              sourceExpressions = flattened.subList(0, assignmentCount);
              conditions = new ArrayList<>(flattened.subList(assignmentCount, flattened.size()));
              input = project.getInput();
            } else {
              org.apache.calcite.rel.core.Filter filter =
                  (org.apache.calcite.rel.core.Filter) input;
              if (conditions.stream().anyMatch(RexOver::containsOver)) {
                throw new UnsupportedOperationException(
                    "UPDATE cannot merge a window filter with a lower filter");
              }
              // A lower filter goes first, as in Calcite's FilterMergeRule, so that it still guards
              // the conditions above it for a consumer that evaluates a conjunction in order.
              conditions.add(0, filter.getCondition());
              input = filter.getInput();
            }
          }
          if (!(input instanceof org.apache.calcite.rel.core.TableScan)
              || !table.getQualifiedName().equals(input.getTable().getQualifiedName())) {
            throw new UnsupportedOperationException(
                "UPDATE requires a scan of its target table beneath projections and filters");
          }
          if (!input.getRowType().equals(table.getRowType())) {
            throw new UnsupportedOperationException(
                "UPDATE target scan schema must match the target table schema");
          }
          if (!conditions.isEmpty() && RexOver.containsOver(sourceExpressions, null)) {
            // The spec does not define the rows a window in an UpdateRel transformation ranges
            // over, so only an unfiltered update, where it can only be every row, is converted.
            throw new UnsupportedOperationException(
                "UPDATE cannot apply a window assignment to filtered rows");
          }
          if (conditions.size() > 1
              && conditions.stream().anyMatch(expr -> !isDeterministic(expr))) {
            throw new UnsupportedOperationException("UPDATE cannot merge nondeterministic filters");
          }
          for (RelNode binding : correlationBindings) {
            outerReferenceResolver.rebindTarget(binding, modify);
          }
          Expression condition =
              toExpression(
                  conditions.size() == 1
                      ? conditions.get(0)
                      : RexUtil.composeConjunction(rexBuilder, conditions));

          List<String> updateColumnNames = modify.getUpdateColumnList();
          List<String> allTableColumnNames = table.getRowType().getFieldNames();
          List<NamedUpdate.TransformExpression> transformations = new ArrayList<>();

          for (int i = 0; i < updateColumnNames.size(); i++) {
            String colName = updateColumnNames.get(i);
            RexNode rexExpr = sourceExpressions.get(i);

            int columnIndex = allTableColumnNames.indexOf(colName);
            if (columnIndex == -1) {
              throw new IllegalStateException(
                  "Updated column '" + colName + "' not found in table schema.");
            }

            Expression substraitExpr = toExpression(rexExpr);

            transformations.add(
                NamedUpdate.TransformExpression.builder()
                    .columnTarget(columnIndex)
                    .transformation(substraitExpr)
                    .build());
          }

          return NamedUpdate.builder()
              .tableSchema(typeConverter.toNamedStruct(table.getRowType()))
              .names(table.getQualifiedName())
              .condition(condition)
              .transformations(transformations)
              .build();
        }

      default:
        return super.visit(modify);
    }
  }

  private static RelOptTable requireTable(TableModify modify) {
    RelOptTable table = modify.getTable();
    if (table == null) {
      throw new IllegalArgumentException(
          String.format(
              "TableModify with operation %s has no target table", modify.getOperation()));
    }
    return table;
  }

  private static boolean isDeterministic(RexNode expression) {
    DeterminismChecker checker = new DeterminismChecker();
    expression.accept(checker);
    return checker.deterministic;
  }

  /** Includes subquery relations, which RexUtil.isDeterministic does not inspect. */
  private static final class DeterminismChecker extends RexShuttle {
    private boolean deterministic = true;

    @Override
    public RexNode visitCall(RexCall call) {
      deterministic &= call.getOperator().isDeterministic();
      return deterministic ? super.visitCall(call) : call;
    }

    @Override
    public SqlAggFunction visitOverAggFunction(SqlAggFunction function) {
      deterministic &= function.isDeterministic();
      return function;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      new RelVisitor() {
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
          if (!deterministic) {
            return;
          }
          node.accept(DeterminismChecker.this);
          if (node instanceof org.apache.calcite.rel.core.Sort) {
            org.apache.calcite.rel.core.Sort sort = (org.apache.calcite.rel.core.Sort) node;
            if ((sort.fetch != null || sort.offset != null)
                && !Boolean.TRUE.equals(
                    sort.getCluster()
                        .getMetadataQuery()
                        .areColumnsUnique(
                            sort.getInput(), ImmutableBitSet.of(sort.getCollation().getKeys())))) {
              deterministic = false;
            }
          }
          if (node instanceof org.apache.calcite.rel.core.Aggregate) {
            // Aggregate operators and their direct arguments are not visited by accept(RexShuttle).
            for (AggregateCall call :
                ((org.apache.calcite.rel.core.Aggregate) node).getAggCallList()) {
              deterministic &= call.getAggregation().isDeterministic();
              apply(call.rexList);
            }
          }
          super.visit(node, ordinal, parent);
        }
      }.go(subQuery.rel);
      return deterministic ? super.visitSubQuery(subQuery) : subQuery;
    }
  }

  /**
   * Handles Calcite {@link CreateTable} as Substrait CTAS. (Create Table As Select)
   *
   * @param createTable Calcite create-table node
   * @return Substrait CTAS write relation
   */
  public Rel handleCreateTable(CreateTable createTable) {
    RelNode input = createTable.getInput();
    Rel inputRel = apply(input);
    NamedStruct schema = typeConverter.toNamedStruct(createTable.getTableSchema());
    return NamedWrite.builder()
        .input(inputRel)
        .tableSchema(schema)
        .operation(AbstractWriteRel.WriteOp.CTAS)
        .createMode(createTable.getCreateMode())
        .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
        .names(createTable.getTableName())
        .build();
  }

  /**
   * Handles Calcite {@link CreateView} as Substrait view DDL.
   *
   * @param createView Calcite create-view node
   * @return Substrait view DDL relation
   */
  public Rel handleCreateView(CreateView createView) {
    RelNode input = createView.getInput();
    Rel inputRel = apply(input);

    final Expression.StructLiteral defaults = ExpressionCreator.struct(false);

    return NamedDdl.builder()
        .viewDefinition(inputRel)
        .tableSchema(typeConverter.toNamedStruct(createView.getViewSchema()))
        .tableDefaults(defaults)
        .operation(AbstractDdlRel.DdlOp.CREATE)
        .object(AbstractDdlRel.DdlObject.VIEW)
        .names(createView.getViewName())
        .build();
  }

  /**
   * Converts the isthmus {@link VirtualTable}, which is what a virtual table whose rows are not all
   * literals converts to.
   *
   * @param virtualTable Calcite virtual table
   * @return Substrait virtual table scan
   */
  @Override
  public Rel visit(VirtualTable virtualTable) {
    // At the row type's field types rather than the values' own, as visit(Values) does: a literal
    // narrower than its column -- Calcite infers one for a tuple value -- would otherwise disagree
    // with the schema built from the same row type, and VirtualTableScan rejects the relation on
    // that.
    List<RelDataTypeField> rowFields = virtualTable.getRowType().getFieldList();
    LiteralConverter literalConverter = new LiteralConverter(typeConverter);
    List<Expression.NestedStruct> rows = new ArrayList<>(virtualTable.getRows().size());
    for (List<RexNode> row : virtualTable.getRows()) {
      List<Expression> fields = new ArrayList<>(row.size());
      for (int column = 0; column < row.size(); column++) {
        RexNode value = row.get(column);
        RelDataType declaredType = rowFields.get(column).getType();
        Expression converted =
            value instanceof RexLiteral
                ? literalConverter.convert((RexLiteral) value, declaredType)
                : toExpression(value);
        // A value that is not a literal is converted from the expressions it is built of and
        // takes its type from them, which the declared type cannot be put back on: casting at it
        // would put an expression in the output the input did not have. Refused here rather than
        // left to VirtualTableScan, whose check compares the two types without promoting either.
        Type declared = typeConverter.toSubstrait(declaredType);
        if (!converted.getType().equals(declared)) {
          throw new UnsupportedOperationException(
              String.format(
                  "A virtual table's value %s converts to %s where its column is declared %s: "
                      + "isthmus cannot convert a value that does not carry its column's type",
                  value, converted.getType(), declared));
        }
        fields.add(converted);
      }
      rows.add(ExpressionCreator.nestedStruct(false, fields));
    }
    return VirtualTableScan.builder()
        .initialSchema(typeConverter.toNamedStruct(virtualTable.getRowType()))
        .addAllRows(rows)
        .build();
  }

  /**
   * Visits other Calcite nodes (e.g., DDL wrappers).
   *
   * @param other Calcite node
   * @return Converted relation
   * @throws UnsupportedOperationException if the node type is unsupported.
   */
  @Override
  public Rel visitOther(RelNode other) {
    if (other instanceof CreateTable) {
      return handleCreateTable((CreateTable) other);

    } else if (other instanceof CreateView) {
      return handleCreateView((CreateView) other);
    }
    throw new UnsupportedOperationException("Unable to handle node: " + other);
  }

  /**
   * Assigns id-based outer-reference anchors for correlated expressions in the given tree.
   *
   * @param root Root Calcite node to analyze
   */
  protected void resolveOuterReferences(RelNode root) {
    outerReferenceResolver = new OuterReferenceResolver();
    outerReferenceResolver.resolve(root);
  }

  /**
   * Returns the outer-reference anchor a correlated field reference bound to the given correlation
   * id must emit.
   *
   * @param correlationId the Calcite correlation id
   * @return the anchor, or {@code null} if unknown
   */
  public Integer getOuterReferenceAnchor(CorrelationId correlationId) {
    return outerReferenceResolver == null
        ? null
        : outerReferenceResolver.anchorForCorrelationId(correlationId);
  }

  /**
   * Applies the visitor to a Calcite {@link RelNode}, stamping the produced relation with its
   * outer-reference {@code rel_anchor} when it is the binding point of a correlated reference.
   *
   * @param r Calcite node
   * @return Converted Substrait relation
   */
  public Rel apply(RelNode r) {
    Rel rel = reverseAccept(r);
    if (outerReferenceResolver != null) {
      Integer anchor = outerReferenceResolver.anchorForTarget(r);
      if (anchor != null) {
        try {
          return rel.withRelAnchor(anchor);
        } catch (UnsupportedOperationException e) {
          // rel is the binding point of a correlated outer reference but cannot carry a rel_anchor
          // (a custom, non-Immutables Rel that does not override withRelAnchor). Fail with context
          // rather than propagating the bare "does not support setting a relation anchor" default.
          throw new UnsupportedOperationException(
              "Relation "
                  + rel.getClass()
                  + " is the binding point of an id-based outer reference but does not support "
                  + "setting a rel_anchor; override Rel#withRelAnchor to convert correlated "
                  + "subqueries into this relation type",
              e);
        }
      }
    }
    return rel;
  }

  /**
   * Applies the visitor to a list of Calcite {@link RelNode}s.
   *
   * @param inputs Calcite input relations
   * @return Converted Substrait relations
   */
  public List<Rel> apply(List<RelNode> inputs) {
    return inputs.stream()
        .map(inputRel -> apply(inputRel))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Deprecated, use {@link #convert(RelRoot, ConverterProvider)} directly
   *
   * @param relRoot The Calcite RelRoot to convert
   * @param extensions The extension collection to use for the conversion
   * @return The resulting Substrait Plan.Root
   */
  @Deprecated
  public static Plan.Root convert(RelRoot relRoot, SimpleExtension.ExtensionCollection extensions) {
    return convert(relRoot, new ConverterProvider(extensions));
  }

  /**
   * Converts a Calcite {@link RelRoot} to a Substrait {@link Plan.Root}
   *
   * <p>Converts the output of {@link RelRoot#project()} to a Substrait {@link Rel} and wraps it in
   * a {@link Plan.Root}. Handles the extraction of final output field names, paying special
   * attention to nested types (structs, maps) via the visitor's type converter, rather than using
   * the names from {@link RelRoot#validatedRowType} directly.
   *
   * @param relRoot The Calcite RelRoot to convert. This is expected to be a complete plan.
   * @param converterProvider The {@link ConverterProvider} controlling conversion behaviours.
   * @return The resulting Substrait {@link Plan.Root}, containing the converted relational tree and
   *     the output names.
   */
  public static Plan.Root convert(RelRoot relRoot, ConverterProvider converterProvider) {
    SubstraitRelVisitor visitor = converterProvider.getSubstraitRelVisitor();
    // Assign id-based outer-reference anchors; apply() then emits rel_reference for correlated
    // references and stamps rel_anchor on their binding relations.
    visitor.resolveOuterReferences(relRoot.rel);
    Rel rel = visitor.apply(relRoot.project());

    // Avoid using the names from relRoot.validatedRowType because if there are
    // nested types (i.e ROW, MAP, etc) the typeConverter will pad names correctly
    List<String> names = visitor.typeConverter.toNamedStruct(relRoot.validatedRowType).names();
    return Plan.Root.builder().input(rel).names(names).build();
  }

  /**
   * Deprecated, use {@link #convert(RelNode, ConverterProvider)} directly
   *
   * <p>This method is suitable for converting a relational sub-tree, but it does not produce a
   * {@link Plan.Root}. For a complete plan conversion, use {@link #convert(RelRoot,
   * SimpleExtension.ExtensionCollection)}.
   *
   * @param relNode The Calcite RelNode (and its subtree) to convert.
   * @param extensions The extension collection to use for the conversion.
   * @return The resulting Substrait Rel.
   */
  @Deprecated
  public static Rel convert(RelNode relNode, SimpleExtension.ExtensionCollection extensions) {
    return convert(relNode, new ConverterProvider(extensions));
  }

  /**
   * Converts a Calcite {@link RelNode} to a Substrait {@link Rel}
   *
   * @param relNode The Calcite RelNode to convert.
   * @param converterProvider The {@link ConverterProvider} controlling conversion behaviours.
   * @return The resulting Substrait Rel.
   */
  public static Rel convert(RelNode relNode, ConverterProvider converterProvider) {
    SubstraitRelVisitor visitor = converterProvider.getSubstraitRelVisitor();
    visitor.resolveOuterReferences(relNode);
    return visitor.apply(relNode);
  }
}
