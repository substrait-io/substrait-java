package io.substrait.isthmus;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortDirection;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.FunctionBindingResolver;
import io.substrait.extension.ResolvedAggregateBinding;
import io.substrait.extension.ResolvedArgument;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.AbstractUpdate;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.OuterReferenceConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter
    extends AbstractRelVisitor<RelNode, SubstraitRelNodeConverter.Context, RuntimeException> {

  /** Calcite type factory used to construct row and field types. */
  protected final RelDataTypeFactory typeFactory;

  /** Converter for Substrait scalar functions to Calcite operators. */
  protected final ScalarFunctionConverter scalarFunctionConverter;

  /** Converter for Substrait aggregate functions to Calcite operators. */
  protected final AggregateFunctionConverter aggregateFunctionConverter;

  /** Converts Substrait {@code Expression}s into Calcite {@code RexNode}s. */
  protected final ExpressionRexConverter expressionRexConverter;

  /** Calcite {@link RelBuilder} used to construct relational expressions during conversion. */
  protected final RelBuilder relBuilder;

  /** Calcite {@link RexBuilder} used to build Rex nodes (e.g., input refs, literals). */
  protected final RexBuilder rexBuilder;

  /** Type converter to translate between Calcite and Substrait type systems. */
  private final TypeConverter typeConverter;

  /** Controls how aggregate output types are chosen and validated. */
  private final AggregateConversion aggregateConversion;

  /**
   * Creates a new SubstraitRelNodeConverter with the specified extensions, type factory, and
   * relation builder.
   *
   * @param extensions the Substrait extension collection
   * @param typeFactory the Calcite type factory
   * @param relBuilder the Calcite relation builder
   * @deprecated Use {@link #SubstraitRelNodeConverter(RelBuilder, ConverterProvider)} instead
   */
  @Deprecated
  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelDataTypeFactory typeFactory,
      RelBuilder relBuilder) {
    this(relBuilder, new ConverterProvider(extensions, typeFactory));
  }

  /**
   * Creates a new SubstraitRelNodeConverter with the specified relation builder and converter
   * provider. Aggregate conversion follows the provider's {@link
   * ConverterProvider#getAggregateConversion()} — the provider is the single configuration channel,
   * so a subclass overriding that method always takes effect.
   *
   * @param relBuilder the Calcite relation builder
   * @param converterProvider the converter provider containing configuration and converters
   */
  public SubstraitRelNodeConverter(RelBuilder relBuilder, ConverterProvider converterProvider) {
    this.typeFactory = converterProvider.getTypeFactory();
    this.typeConverter = converterProvider.getTypeConverter();
    this.relBuilder = relBuilder;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = converterProvider.getScalarFunctionConverter();
    this.aggregateFunctionConverter = converterProvider.getAggregateFunctionConverter();
    this.expressionRexConverter = converterProvider.getExpressionRexConverter(this);
    this.aggregateConversion = converterProvider.getAggregateConversion();
  }

  /**
   * Converts a Substrait {@link Rel} plan to a Calcite {@link RelNode}.
   *
   * @param relRoot the root Substrait relation to convert
   * @param catalogReader the Calcite catalog reader for schema resolution
   * @param converterProvider the converter provider containing configuration and converters
   * @return the converted Calcite {@link RelNode}
   */
  public static RelNode convert(
      Rel relRoot, Prepare.CatalogReader catalogReader, ConverterProvider converterProvider) {
    RelBuilder relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(converterProvider.getSqlParserConfig())
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .typeSystem(converterProvider.getTypeSystem())
                .programs()
                .build());
    Utils.useReflectiveMetadataProvider(relBuilder.getCluster());
    // Normalize any offset-based outer references (steps_out) to the id-based form (rel_anchor /
    // rel_reference) so the conversion below resolves correlations purely by anchor. Plans that are
    // already id-based are left unchanged.
    return OuterReferenceConverter.toIdBased(relRoot)
        .accept(converterProvider.getSubstraitRelNodeConverter(relBuilder), Context.newContext());
  }

  @Override
  public RelNode visit(Filter filter, Context context) throws RuntimeException {
    RelNode input = filter.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(filter.getInput().getRelAnchor(), input.getRowType()));
    RexNode filterCondition = filter.getCondition().accept(expressionRexConverter, context);
    RelNode node = relBuilder.push(input).filter(context.exitScope(), filterCondition).build();
    return applyRelCommon(node, filter, input);
  }

  @Override
  public RelNode visit(NamedScan namedScan, Context context) throws RuntimeException {
    RelNode node = relBuilder.scan(namedScan.getNames()).build();
    return applyRelCommon(node, namedScan);
  }

  @Override
  public RelNode visit(LocalFiles localFiles, Context context) throws RuntimeException {
    return visitFallback(localFiles, context);
  }

  @Override
  public RelNode visit(Project project, Context context) throws RuntimeException {
    RelNode child = project.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(project.getInput().getRelAnchor(), child.getRowType()));

    Stream<RexNode> directOutputs =
        IntStream.range(0, child.getRowType().getFieldCount())
            .mapToObj(fieldIndex -> rexBuilder.makeInputRef(child, fieldIndex));

    Stream<RexNode> exprs =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter, context));

    List<RexNode> rexExprs =
        Stream.concat(directOutputs, exprs).collect(java.util.stream.Collectors.toList());

    RelNode node =
        relBuilder.push(child).project(rexExprs, List.of(), false, context.exitScope()).build();
    return applyRelCommon(node, project, child);
  }

  @Override
  public RelNode visit(Cross cross, Context context) throws RuntimeException {
    RelNode left = cross.getLeft().accept(this, context);
    RelNode right = cross.getRight().accept(this, context);
    // Calcite represents CROSS JOIN as the equivalent INNER JOIN with true condition
    RelNode node =
        relBuilder.push(left).push(right).join(JoinRelType.INNER, relBuilder.literal(true)).build();
    return applyRelCommon(node, cross, left, right);
  }

  @Override
  public RelNode visit(Join join, Context context) throws RuntimeException {
    RelNode left = join.getLeft().accept(this, context);
    RelNode right = join.getRight().accept(this, context);
    context.enterScope(
        AnchoredInput.of(join.getLeft().getRelAnchor(), left.getRowType()),
        AnchoredInput.of(join.getRight().getRelAnchor(), right.getRowType()));
    RexNode condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter, context))
            .orElse(relBuilder.literal(true));
    JoinRelType joinType = asJoinRelType(join);
    RelNode node =
        relBuilder.push(left).push(right).join(joinType, condition, context.exitScope()).build();
    return applyRelCommon(node, join, left, right);
  }

  private JoinRelType asJoinRelType(Join join) {
    Join.JoinType type = join.getJoinType();

    if (type == JoinType.INNER) {
      return JoinRelType.INNER;
    }
    if (type == JoinType.LEFT) {
      return JoinRelType.LEFT;
    }
    if (type == JoinType.RIGHT) {
      return JoinRelType.RIGHT;
    }
    if (type == JoinType.OUTER) {
      return JoinRelType.FULL;
    }
    if (type == JoinType.LEFT_SEMI) {
      return JoinRelType.SEMI;
    }
    if (type == JoinType.LEFT_ANTI) {
      return JoinRelType.ANTI;
    }
    if (type == JoinType.UNKNOWN) {
      throw new UnsupportedOperationException("Unknown join type is not supported");
    }

    throw new UnsupportedOperationException("Unsupported join type: " + join.getJoinType().name());
  }

  @Override
  public RelNode visit(Set set, Context context) throws RuntimeException {
    List<RelNode> inputs = new ArrayList<>(set.getInputs().size());
    for (Rel input : set.getInputs()) {
      RelNode converted = input.accept(this, context);
      inputs.add(converted);
      relBuilder.push(converted);
    }
    RelBuilder builder = getRelBuilder(set);
    RelNode node = builder.build();
    return applyRelCommon(node, set, inputs.toArray(new RelNode[0]));
  }

  private RelBuilder getRelBuilder(Set set) {
    int numInputs = set.getInputs().size();

    switch (set.getSetOp()) {
      case MINUS_PRIMARY:
        return relBuilder.minus(false, numInputs);
      case MINUS_PRIMARY_ALL:
        return relBuilder.minus(true, numInputs);
      case INTERSECTION_MULTISET:
        return relBuilder.intersect(false, numInputs);
      case INTERSECTION_MULTISET_ALL:
        return relBuilder.intersect(true, numInputs);
      case UNION_DISTINCT:
        return relBuilder.union(false, numInputs);
      case UNION_ALL:
        return relBuilder.union(true, numInputs);
      case UNKNOWN:
        throw new UnsupportedOperationException("Unknown set operation is not supported");
      default:
        throw new UnsupportedOperationException("Unsupported set operation: " + set.getSetOp());
    }
  }

  @Override
  public RelNode visit(Aggregate aggregate, Context context) throws RuntimeException {
    if (!PreCalciteAggregateValidator.isValidCalciteAggregate(aggregate)) {
      aggregate =
          PreCalciteAggregateValidator.PreCalciteAggregateTransformer
              .transformToValidCalciteAggregate(aggregate);
    }

    RelNode child = aggregate.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(aggregate.getInput().getRelAnchor(), child.getRowType()));
    List<List<RexNode>> groupExprLists =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter, context))
                        .collect(java.util.stream.Collectors.toList()))
            .collect(java.util.stream.Collectors.toList());
    List<RexNode> groupExprs =
        groupExprLists.stream().flatMap(Collection::stream).collect(Collectors.toList());
    // An aggregate with no groupings at all is a global aggregation, i.e. a single empty grouping
    // set. Handing RelBuilder an empty list of grouping sets instead would leave the Calcite
    // aggregate with no grouping set at all, and RelBuilder would then re-infer every measure as if
    // there were no empty group — which is the opposite of what a global aggregation means.
    boolean globalAggregation = groupExprLists.isEmpty();
    RelBuilder.GroupKey groupKey =
        globalAggregation
            ? relBuilder.groupKey(groupExprs)
            : relBuilder.groupKey(groupExprs, groupExprLists);
    // Mirrors how RelBuilder derives an aggregate call's hasEmptyGroup from the grouping sets it
    // builds out of this group key; the two must agree or the inferred type below is not the one
    // the call ends up with.
    boolean hasEmptyGroup = globalAggregation || groupExprLists.stream().anyMatch(List::isEmpty);

    List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream()
            .map(measure -> fromMeasure(measure, context, child, hasEmptyGroup))
            .collect(java.util.stream.Collectors.toList());

    final Optional<Remap> remap = aggregate.getRemap();
    // A field grouped on by several sets is one column of the relation, so the grouping-set index
    // sits after the distinct grouping expressions, not after every mention of them.
    final int groupColumnCount = new LinkedHashSet<>(groupExprs).size();
    final int groupingSetIndex = groupColumnCount + aggregateCalls.size();

    // The index is a column of the converted aggregate only where the relation emits it: an
    // aggregate that maps its output away does not need the call at all.
    final boolean emitDirect = remap.isEmpty();
    final boolean groupingSetIndexGetsRemapped =
        remap.map(r -> r.indices().contains(groupingSetIndex)).orElse(false);
    if (aggregate.getGroupings().size() > 1 && (emitDirect || groupingSetIndexGetsRemapped)) {
      aggregateCalls.add(
          AggregateCall.create(
              SqlStdOperatorTable.GROUP_ID,
              false,
              false,
              false,
              Collections.emptyList(),
              Collections.emptyList(),
              -1,
              null,
              RelCollations.EMPTY,
              typeConverter.toCalcite(typeFactory, TypeCreator.REQUIRED.I64),
              null));
    }

    exitUncorrelatedScope(context, Aggregate.class);

    // RelBuilder deduplicates equal aggregate calls, and AggregateCall equality ignores the stored
    // type: two measures of the same function that differ only by their declared output type would
    // collapse into one column. Opt out of deduplication for exactly those aggregates — narrowly,
    // because transform() yields a plain RelBuilder carrying the same cluster and schema but not a
    // custom builder subclass.
    RelBuilder aggregateBuilder =
        hasTypeDistinctDuplicates(aggregateCalls)
            ? relBuilder.transform(config -> config.withDedupAggregateCalls(false))
            : relBuilder;

    RelNode node = aggregateBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
    // Not applyRelCommon: the mapping applied here is the one rewritten above, not the one the
    // relation carries.
    return applyOutputNames(
        applyRemap(node, inConvertedGroupingOrder(remap, groupExprs, aggregateCalls.size())),
        aggregate,
        child);
  }

  /**
   * Returns the emit mapping of a converted aggregate with its indices translated from the order
   * the relation declares its output in to the order the converted aggregate emits it.
   *
   * <p>substrait-java takes the grouping columns of an aggregate to be the distinct grouping
   * expressions in the order they first appear across its grouping sets. The spec orders them by
   * the relation's shared grouping-expression list, which each set's expression references index
   * into; the POJO models a per-set expression list and cannot hold that list's order, so
   * first-appearance is the reconstruction this library reads and writes. Calcite takes them from a
   * bit set, so it emits them ordered by field index. A relation whose grouping sets first mention
   * field 1 and then field 0 declares them in that order, and its emit mapping indexes that order,
   * while the aggregate underneath emits field 0 first.
   *
   * <p>An aggregate that emits directly and declares an order Calcite does not produce gets a
   * mapping it did not carry, which is what puts the columns back in the declared order.
   *
   * @param remap the emit mapping the relation carries, indexing its declared output
   * @param groupExprs the converted grouping expressions, in declared order, with duplicates
   * @param callCount the number of aggregate calls, including any grouping-set index
   * @return the mapping to apply to the converted aggregate
   */
  private static Optional<Remap> inConvertedGroupingOrder(
      Optional<Remap> remap, List<RexNode> groupExprs, int callCount) {
    List<RexNode> declared = new ArrayList<>(new LinkedHashSet<>(groupExprs));
    // Calcite emits the grouping columns in the order they sit in the aggregate's input: a field
    // reference where its field sits, and anything else -- an outer reference, which the transform
    // above leaves alone -- in the projection Calcite adds after them, in the order it was
    // declared. Sorting is stable, so giving the second kind one key keeps that order among them.
    List<RexNode> converted =
        declared.stream()
            .sorted(
                Comparator.comparingInt(
                    expr ->
                        expr instanceof RexInputRef
                            ? ((RexInputRef) expr).getIndex()
                            : Integer.MAX_VALUE))
            .collect(Collectors.toList());
    if (converted.equals(declared)) {
      return remap;
    }
    List<Integer> declaredToConverted = new ArrayList<>();
    for (RexNode expression : declared) {
      declaredToConverted.add(converted.indexOf(expression));
    }
    for (int call = 0; call < callCount; call++) {
      declaredToConverted.add(declared.size() + call);
    }
    return Optional.of(
        Remap.of(
            remap
                .map(
                    mapping ->
                        mapping.indices().stream()
                            .map(declaredToConverted::get)
                            .collect(Collectors.toList()))
                .orElse(declaredToConverted)));
  }

  /**
   * Returns whether the binding carries semantics no stock Calcite aggregate operator can express,
   * so that two invocations differing only in those would be indistinguishable once converted: the
   * function's options, any phase other than a full initial-to-result aggregation, and type
   * arguments — only value arguments become Calcite operands, so a type argument cannot be rebuilt
   * by re-matching. Enum arguments are an operator-dependent case decided by the caller: they are
   * recoverable only when the operator's kind encodes them (e.g. {@code STDDEV_POP} vs {@code
   * STDDEV_SAMP}), which {@link AggregateFunctionConverter#encodesEnumArguments} answers.
   */
  private static boolean carriesOpaqueSemantics(ResolvedAggregateBinding binding) {
    return !binding.function().options().isEmpty()
        || binding.phase() != Expression.AggregationPhase.INITIAL_TO_RESULT
        || hasTypeArgument(binding);
  }

  private static boolean hasTypeArgument(ResolvedAggregateBinding binding) {
    return hasArgumentOfKind(binding, ResolvedArgument.Kind.TYPE);
  }

  private static boolean hasEnumArgument(ResolvedAggregateBinding binding) {
    return hasArgumentOfKind(binding, ResolvedArgument.Kind.ENUM);
  }

  private static boolean hasArgumentOfKind(
      ResolvedAggregateBinding binding, ResolvedArgument.Kind kind) {
    for (ResolvedArgument argument : binding.function().arguments()) {
      if (argument.kind() == kind) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether two aggregate calls are equal to Calcite yet carry different types, i.e.
   * whether deduplicating them would silently give one of them the other's type.
   */
  private static boolean hasTypeDistinctDuplicates(List<AggregateCall> aggregateCalls) {
    Map<AggregateCall, RelDataType> typesByCall = new HashMap<>();
    for (AggregateCall call : aggregateCalls) {
      RelDataType existing = typesByCall.putIfAbsent(call, call.getType());
      if (existing != null && !existing.equals(call.getType())) {
        return true;
      }
    }
    return false;
  }

  private AggregateCall fromMeasure(
      Aggregate.Measure measure, Context context, RelNode input, boolean hasEmptyGroup) {
    List<FunctionArg> eArgs = measure.getFunction().arguments();
    // Only value (Expression) arguments map to Calcite aggregate operands. Enum arguments such as
    // the std_dev/variance "distribution" are used to disambiguate the operator, not as operands.
    List<RexNode> arguments =
        IntStream.range(0, eArgs.size())
            .filter(i -> eArgs.get(i) instanceof Expression)
            .mapToObj(
                i ->
                    eArgs
                        .get(i)
                        .accept(
                            measure.getFunction().declaration(),
                            i,
                            expressionRexConverter,
                            context))
            .collect(java.util.stream.Collectors.toList());
    Optional<SqlOperator> operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(),
            measure.getFunction().outputType(),
            measure.getFunction().arguments());
    if (!operator.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    List<Integer> argIndex = new ArrayList<>();
    for (RexNode arg : arguments) {
      // arguments are guaranteed to be RexInputRef because of the prior call to
      // transformToValidCalciteAggregate
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    boolean distinct =
        measure.getFunction().invocation().equals(Expression.AggregationInvocation.DISTINCT);

    SqlAggFunction aggFunction;
    // Resolve the Substrait binding (semantic identity). Validating the signature, options and
    // declared output type against the extension declaration is a separate, opt-in concern
    // (EXTENSION_DECLARATION).
    ResolvedAggregateBinding binding = ResolvedAggregateBinding.resolve(measure.getFunction());
    if (aggregateConversion.bindingValidation()
        == AggregateConversion.FunctionBindingValidation.EXTENSION_DECLARATION) {
      FunctionBindingResolver.validate(binding, measure.getFunction().getType());
    }

    if (operator.get() instanceof SqlAggFunction) {
      aggFunction = (SqlAggFunction) operator.get();
    } else {
      String msg =
          String.format(
              "Unable to convert non-aggregate operator: %s for substrait aggregate function %s",
              operator.get(), measure.getFunction().declaration().name());
      throw new IllegalArgumentException(msg);
    }

    int filterArg = -1;
    if (measure.getPreMeasureFilter().isPresent()) {
      RexNode filter = measure.getPreMeasureFilter().get().accept(expressionRexConverter, context);
      filterArg = ((RexInputRef) filter).getIndex();
    }

    RelCollation relCollation = RelCollations.EMPTY;
    if (!measure.getFunction().sort().isEmpty()) {
      relCollation =
          RelCollations.of(
              measure.getFunction().sort().stream()
                  .map(sortField -> toRelFieldCollation(sortField, context))
                  .collect(Collectors.toList()));
    }

    boolean preservePlanType =
        aggregateConversion.outputTypeSource() == AggregateConversion.OutputTypeSource.PLAN_OUTPUT;
    boolean opaqueSemantics =
        carriesOpaqueSemantics(binding)
            || (hasEnumArgument(binding)
                && !AggregateFunctionConverter.encodesEnumArguments(aggFunction, binding));

    // Calcite's return-type inference runs inside AggregateCall.create when no type is passed in.
    // Under PLAN_OUTPUT its result only decides whether the plan's type must travel on a wrapper,
    // so it is skipped when opaque semantics force the wrapper anyway, and a failure — an operand
    // shape the operator's inference rule rejects, say an intermediate-state struct fed to a
    // caller-supplied operator — counts as "diverges" instead of failing a conversion that never
    // needed the inferred type. Under CALCITE_INFERENCE the inferred type is the output type, so
    // there is nothing to fall back to and the failure propagates.
    RelDataType inferredType = null;
    if (!preservePlanType || !opaqueSemantics) {
      try {
        inferredType =
            AggregateCall.create(
                    aggFunction,
                    distinct,
                    false,
                    false,
                    Collections.emptyList(),
                    argIndex,
                    filterArg,
                    null,
                    relCollation,
                    hasEmptyGroup,
                    input,
                    null,
                    null)
                .getType();
      } catch (RuntimeException e) {
        if (!preservePlanType) {
          throw e;
        }
      }
    }

    // Convert the declared type only where it is used: CALCITE_INFERENCE promises to ignore it, and
    // a type this converter cannot represent must not fail a conversion that never needed it.
    RelDataType returnType =
        preservePlanType
            ? typeConverter.toCalcite(typeFactory, measure.getFunction().getType())
            : inferredType;
    boolean typeDiverges = preservePlanType && !returnType.equals(inferredType);
    // The binding must also travel when dropping it would leave the declaration ambiguous: the
    // reverse direction re-matches by signature key and by type-driven fallbacks, picking by
    // registration order, so a declaration another variant can shadow for these argument types
    // and output type — or one with no reverse mapping at all — is only plan-determined via the
    // carried binding. The output type consulted is the one the converted call will carry.
    boolean mustCarryBinding =
        opaqueSemantics
            || typeDiverges
            || !aggregateFunctionConverter.reconstructsUniquely(
                aggFunction,
                measure.getFunction().declaration(),
                binding.function().arguments(),
                preservePlanType
                    ? measure.getFunction().getType()
                    : typeConverter.toSubstrait(returnType));
    if (mustCarryBinding) {
      // Calcite would either re-infer a different type, lose semantics its operator cannot
      // express, or re-match an ambiguous declaration by load order; carry the binding and the
      // chosen type on a transport wrapper so all of it survives Calcite's re-inference
      // (RelBuilder / planner rules) and its call deduplication.
      aggFunction = AggregateFunctions.bind(aggFunction, binding, returnType);
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        Collections.emptyList(),
        argIndex,
        filterArg,
        null,
        relCollation,
        returnType,
        null);
  }

  @Override
  public RelNode visit(Sort sort, Context context) throws RuntimeException {
    RelNode child = sort.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(sort.getInput().getRelAnchor(), child.getRowType()));
    List<RexNode> sortExpressions =
        sort.getSortFields().stream()
            .map(sortField -> directedRexNode(sortField, context))
            .collect(Collectors.toList());
    exitUncorrelatedScope(context, Sort.class);
    RelNode node = relBuilder.push(child).sort(sortExpressions).build();
    return applyRelCommon(node, sort, child);
  }

  private RexNode directedRexNode(Expression.SortField sortField, Context context) {
    Expression expression = sortField.expr();
    RexNode rexNode = expression.accept(expressionRexConverter, context);
    SortDirection sortDirection = sortField.direction();

    if (sortDirection == Expression.SortDirection.ASC_NULLS_FIRST) {
      return relBuilder.nullsFirst(rexNode);
    }
    if (sortDirection == Expression.SortDirection.ASC_NULLS_LAST) {
      return relBuilder.nullsLast(rexNode);
    }
    if (sortDirection == Expression.SortDirection.DESC_NULLS_FIRST) {
      return relBuilder.nullsFirst(relBuilder.desc(rexNode));
    }
    if (sortDirection == Expression.SortDirection.DESC_NULLS_LAST) {
      return relBuilder.nullsLast(relBuilder.desc(rexNode));
    }
    if (sortDirection == Expression.SortDirection.CLUSTERED) {
      throw new UnsupportedOperationException(
          String.format("Unexpected Expression.SortDirection: Clustered!"));
    }

    throw new IllegalArgumentException("Unsupported sort direction: " + sortDirection);
  }

  @Override
  public RelNode visit(Fetch fetch, Context context) throws RuntimeException {
    RelNode child = fetch.getInput().accept(this, context);
    context.enterScope(AnchoredInput.of(fetch.getInput().getRelAnchor(), child.getRowType()));
    // Offset/count are expressions; pass them through to Calcite as RexNodes so non-literal (e.g.
    // dynamic-parameter) offset/count are preserved. An unset offset means 0 and an unset count
    // means LIMIT ALL.
    RexNode offset =
        fetch.getOffset().map(e -> e.accept(expressionRexConverter, context)).orElse(null);
    RexNode count =
        fetch.getCount().map(e -> e.accept(expressionRexConverter, context)).orElse(null);
    exitUncorrelatedScope(context, Fetch.class);
    RelNode node = relBuilder.push(child).sortLimit(offset, count, ImmutableList.of()).build();
    return applyRelCommon(node, fetch, child);
  }

  private RelFieldCollation toRelFieldCollation(Expression.SortField sortField, Context context) {
    Expression expression = sortField.expr();
    RexNode rex = expression.accept(expressionRexConverter, context);
    SortDirection sortDirection = sortField.direction();
    RexSlot rexSlot = (RexSlot) rex;
    int fieldIndex = rexSlot.getIndex();

    final RelFieldCollation.Direction fieldDirection;
    final RelFieldCollation.NullDirection nullDirection;

    if (sortDirection == SortDirection.ASC_NULLS_FIRST) {
      fieldDirection = RelFieldCollation.Direction.ASCENDING;
      nullDirection = RelFieldCollation.NullDirection.FIRST;
    } else if (sortDirection == SortDirection.ASC_NULLS_LAST) {
      fieldDirection = RelFieldCollation.Direction.ASCENDING;
      nullDirection = RelFieldCollation.NullDirection.LAST;
    } else if (sortDirection == SortDirection.DESC_NULLS_FIRST) {
      nullDirection = RelFieldCollation.NullDirection.FIRST;
      fieldDirection = RelFieldCollation.Direction.DESCENDING;
    } else if (sortDirection == SortDirection.DESC_NULLS_LAST) {
      nullDirection = RelFieldCollation.NullDirection.LAST;
      fieldDirection = RelFieldCollation.Direction.DESCENDING;
    } else if (sortDirection == SortDirection.CLUSTERED) {
      fieldDirection = RelFieldCollation.Direction.CLUSTERED;
      nullDirection = RelFieldCollation.NullDirection.UNSPECIFIED;
    } else {
      throw new UnsupportedOperationException(
          String.format("Unexpected Expression.SortDirection enum: %s !", sortDirection));
    }

    return new RelFieldCollation(fieldIndex, fieldDirection, nullDirection);
  }

  @Override
  public RelNode visit(NamedUpdate update, Context context) {
    if (update.getRemap().isPresent()) {
      throw new UnsupportedOperationException(
          "Emit mapping on a NamedUpdate is not supported: TableModify's row type is a single "
              + "ROWCOUNT column, not the updated table's columns");
    }
    relBuilder.scan(update.getNames());
    context.enterScope(AnchoredInput.of(update.getRelAnchor(), relBuilder.peek().getRowType()));
    RexNode condition = update.getCondition().accept(expressionRexConverter, context);

    NamedStruct tableSchema = update.getTableSchema();
    List<String> fieldNames = tableSchema.names();

    List<String> updateColumnList = new ArrayList<>();
    List<RexNode> sourceExpressionList = new ArrayList<>();

    for (AbstractUpdate.TransformExpression transform : update.getTransformations()) {

      updateColumnList.add(fieldNames.get(transform.getColumnTarget()));
      sourceExpressionList.add(
          transform.getTransformation().accept(expressionRexConverter, context));
    }

    relBuilder.filter(context.exitScope(), condition);
    RelNode inputForModify = relBuilder.build();

    final RelOptTable table = requireRelOptSchema().getTableForMember(update.getNames());

    if (table == null) {
      throw new IllegalStateException("Table not found in Calcite catalog: " + update.getNames());
    }
    final Prepare.CatalogReader catalogReader =
        requireCatalogReader(table.getRelOptSchema(), update.getNames());

    return LogicalTableModify.create(
        table,
        catalogReader,
        inputForModify,
        TableModify.Operation.UPDATE,
        updateColumnList,
        sourceExpressionList,
        false);
  }

  @Override
  public RelNode visit(ScatterExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(SingleBucketExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(MultiBucketExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(RoundRobinExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(BroadcastExchange exchange, Context context) throws RuntimeException {
    return visitFallback(exchange, context);
  }

  @Override
  public RelNode visit(NamedDdl namedDdl, Context context) {
    if (namedDdl.getOperation() != AbstractDdlRel.DdlOp.CREATE
        || namedDdl.getObject() != AbstractDdlRel.DdlObject.VIEW) {
      throw new UnsupportedOperationException(
          String.format(
              "Can only handle NamedDdl with (%s, %s), given (%s, %s)",
              AbstractDdlRel.DdlOp.CREATE,
              AbstractDdlRel.DdlObject.VIEW,
              namedDdl.getOperation(),
              namedDdl.getObject()));
    }

    if (namedDdl.getViewDefinition().isEmpty()) {
      throw new IllegalArgumentException("NamedDdl view definition must be set");
    }

    if (namedDdl.getRemap().isPresent()) {
      throw new UnsupportedOperationException(
          "Emit mapping on a NamedDdl is not supported: isthmus has no place for a projection "
              + "between a CreateView's definition and the view it creates");
    }

    if (!namedDdl.getTableDefaults().fields().isEmpty()) {
      throw new UnsupportedOperationException(
          "Default values on a NamedDdl are not supported: a Calcite CreateView has nowhere to put "
              + "them, and the spec has table_defaults report a full list of them");
    }

    Rel viewDefinition = namedDdl.getViewDefinition().get();
    RelNode relNode = viewDefinition.accept(this, context);
    return new CreateView(namedDdl.getNames(), toRowType(namedDdl.getTableSchema()), relNode);
  }

  @Override
  public RelNode visit(VirtualTableScan virtualTableScan, Context context) {
    if (virtualTableScan.getProjection().isPresent()) {
      throw new UnsupportedOperationException(
          "Projection on a VirtualTableScan is not supported: its columns would have to be "
              + "masked before an emit mapping selects from them");
    }
    // A schema's names are one per field at every level of the struct, in depth-first order, so
    // they have to be handed to the conversion rather than paired with the row type afterwards:
    // with a nested struct anywhere in the schema the two lists do not even have the same length.
    final NamedStruct schema = virtualTableScan.getInitialSchema();
    // A relation's row type says what its columns are, not whether a value is there, and Calcite
    // builds one NOT NULL wherever it derives one -- so a nullable schema struct contributes its
    // fields and not its nullability.
    final RelDataType rowType =
        typeFactory.createTypeWithNullability(
            typeConverter.toCalcite(typeFactory, schema.struct(), schema.names()), false);

    List<List<RexNode>> convertedRows = new ArrayList<>();
    // The same values with the cast a nullable literal converts as taken off, which is the form a
    // LogicalValues tuple would hold them in -- where they are literals at all.
    List<List<RexNode>> tupleValues = new ArrayList<>();
    for (final Expression.NestedStruct rowExpr : virtualTableScan.getRows()) {
      List<RexNode> convertedRow = new ArrayList<>();
      List<RexNode> tupleRow = new ArrayList<>();
      for (int column = 0; column < rowExpr.fields().size(); column++) {
        Expression field = rowExpr.fields().get(column);
        RelDataType declaredType = rowType.getFieldList().get(column).getType();
        RexNode value =
            valueAsDeclared(field.accept(expressionRexConverter, context), declaredType);
        convertedRow.add(value);
        // Only a converted literal has its nullability cast taken off. A cast the plan carries
        // itself is doing work -- it has a failure behavior, and it is part of what round-trips --
        // so a row holding one is computed rather than tabulated.
        tupleRow.add(field instanceof Expression.Literal ? unwrapNullabilityCast(value) : value);
      }
      convertedRows.add(convertedRow);
      tupleValues.add(tupleRow);
    }

    // A LogicalValues tuple holds nothing but literals, and whether a value is one is a property of
    // what it converts to rather than of the Substrait expression it came from: a struct converts
    // to a ROW call and a list to an array constructor, literal or not. Neither belongs in a tuple
    // anyway -- Calcite orders tuples by casting each value to Comparable, and the value of a row
    // literal is a list of RexLiterals, which are not.
    boolean encodableAsTuples =
        tupleValues.stream().flatMap(List::stream).allMatch(value -> value instanceof RexLiteral);
    if (encodableAsTuples) {
      ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = ImmutableList.builder();
      for (final List<RexNode> tupleRow : tupleValues) {
        ImmutableList.Builder<RexLiteral> tupleBuilder = ImmutableList.builder();
        for (RexNode value : tupleRow) {
          tupleBuilder.add((RexLiteral) value);
        }
        tuplesBuilder.add(tupleBuilder.build());
      }
      return applyRelCommon(
          LogicalValues.create(relBuilder.getCluster(), rowType, tuplesBuilder.build()),
          virtualTableScan);
    } else {
      // A row that does not fit a LogicalValues tuple is computed instead: we create a
      // LogicalProject for each row to compute its values, and combine them together using a
      // LogicalUnion. For example the following:
      //
      //   VirtualTable
      //     (e1, e2)
      //     (e3, e4)
      //
      //  Becomes:
      //
      //   LogicalUnion(all=[true])
      //     LogicalProject(exprs=[e1, e2])
      //       <Empty Row>
      //     LogicalProject(exprs=[e3, e4])
      //       <Empty Row>
      //

      RelDataType emptyRowType = typeFactory.createStructType(List.of(), List.of());
      ImmutableList<ImmutableList<RexLiteral>> emptyRowValue = ImmutableList.of(ImmutableList.of());

      List<RelNode> projects = new ArrayList<>();
      for (final List<RexNode> rexRow : convertedRows) {
        RelNode values = LogicalValues.create(relBuilder.getCluster(), emptyRowType, emptyRowValue);
        RelNode project =
            LogicalProject.create(
                values, Collections.emptyList(), rexRow, rowType, Collections.emptySet());
        projects.add(project);
      }
      RelNode union = LogicalUnion.create(projects, true);

      // Apply a final LogicalProject on top to capture the field names from the VirtualTable
      List<RexNode> topProjectExprs = new ArrayList<>();
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        topProjectExprs.add(rexBuilder.makeInputRef(union, i));
      }
      RelNode topProject =
          LogicalProject.create(
              union, Collections.emptyList(), topProjectExprs, rowType, Collections.emptySet());
      return applyRelCommon(topProject, virtualTableScan, topProject);
    }
  }

  /**
   * Gives a converted row value the type its column is declared at.
   *
   * <p>A Substrait struct type has no field names of its own -- a schema names every field once, at
   * the relation level -- so a struct-valued expression converts carrying the placeholder names
   * Calcite derives for it, while the row type built from the schema carries the schema's own.
   * Calcite compares field names when it checks a value against the type it is declared at, so a
   * struct is rejected on that difference alone, wherever in the column's type it sits.
   *
   * <p>Names are what a row, list or map value is rebuilt for: a row's field types are checked
   * against the schema when the {@link VirtualTableScan} is built, so a value that is one of those
   * agrees with its declared type on everything a Substrait type says. It can still be stamped
   * nullable where it was not -- Calcite makes a struct's fields nullable along with the struct,
   * and a value that cannot be null stands in a column that can. A value that cannot take the
   * declared type at all -- a call returning a struct, say, whose names are not the declared ones
   * -- is reported here: {@link LogicalProject} and {@link LogicalValues} check the row type they
   * are handed with an {@code assert}, which says nothing at all unless assertions are on.
   *
   * @param value the converted row value
   * @param declaredType the type its column is declared at
   * @return the value, at {@code declaredType}
   * @throws IllegalArgumentException if the value cannot be given that type
   */
  private RexNode valueAsDeclared(RexNode value, RelDataType declaredType) {
    RexNode declared = renamedAsDeclared(value, declaredType);
    if (!fitsDeclared(declared.getType(), declaredType)) {
      throw new IllegalArgumentException(
          String.format(
              "A virtual table's value %s converts to the type %s, which is not the %s its column is declared at",
              value, declared.getType().getFullTypeString(), declaredType.getFullTypeString()));
    }
    return declared;
  }

  /**
   * Whether a converted value can stand at the type it is declared at.
   *
   * <p>A value that cannot be null stands in a column that can: Calcite makes a struct's fields
   * nullable along with the struct, so the fields of a nullable struct are declared that way
   * whatever the values in them are. The other direction is a mismatch, and so is any difference
   * beyond nullability -- a field name included, which is the one this rename is about.
   */
  private boolean fitsDeclared(RelDataType valueType, RelDataType declaredType) {
    return SqlTypeUtil.equalSansNullability(typeFactory, valueType, declaredType)
        && (declaredType.isNullable() || !valueType.isNullable());
  }

  private RexNode renamedAsDeclared(RexNode value, RelDataType declaredType) {
    if (value.getType().equals(declaredType)) {
      return value;
    }
    if (value instanceof RexLiteral && ((RexLiteral) value).isNull()) {
      // A null value has no fields to rename, only a type to take -- and a list or a map converts
      // to a literal when it is null and to a constructor call otherwise, so this is where those
      // two are named as much as a struct is.
      return rexBuilder.makeNullLiteral(declaredType);
    }
    switch (declaredType.getSqlTypeName()) {
      case ROW:
        return nameRowFieldsAsDeclared(value, declaredType);
      case ARRAY:
        return nameArrayItemsAsDeclared(value, declaredType);
      case MAP:
        return nameMapEntriesAsDeclared(value, declaredType);
      default:
        return value;
    }
  }

  private RexNode nameRowFieldsAsDeclared(RexNode value, RelDataType declaredType) {
    if (!value.isA(SqlKind.ROW)) {
      return value;
    }
    List<RexNode> fields = ((RexCall) value).getOperands();
    if (fields.size() != declaredType.getFieldCount()) {
      return value;
    }
    List<RexNode> renamed = new ArrayList<>();
    for (int field = 0; field < fields.size(); field++) {
      renamed.add(
          valueAsDeclared(fields.get(field), declaredType.getFieldList().get(field).getType()));
    }
    return rexBuilder.makeCall(declaredType, SqlStdOperatorTable.ROW, renamed);
  }

  private RexNode nameArrayItemsAsDeclared(RexNode value, RelDataType declaredType) {
    if (!value.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
      return value;
    }
    RelDataType itemType = Objects.requireNonNull(declaredType.getComponentType());
    List<RexNode> renamed = new ArrayList<>();
    for (RexNode item : ((RexCall) value).getOperands()) {
      renamed.add(valueAsDeclared(item, itemType));
    }
    return rexBuilder.makeCall(declaredType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, renamed);
  }

  private RexNode nameMapEntriesAsDeclared(RexNode value, RelDataType declaredType) {
    if (!value.isA(SqlKind.MAP_VALUE_CONSTRUCTOR)) {
      return value;
    }
    RelDataType keyType = Objects.requireNonNull(declaredType.getKeyType());
    RelDataType entryType = Objects.requireNonNull(declaredType.getValueType());
    List<RexNode> operands = ((RexCall) value).getOperands();
    List<RexNode> renamed = new ArrayList<>();
    for (int operand = 0; operand < operands.size(); operand++) {
      // the constructor takes keys and values alternating
      renamed.add(valueAsDeclared(operands.get(operand), operand % 2 == 0 ? keyType : entryType));
    }
    return rexBuilder.makeCall(declaredType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, renamed);
  }

  /**
   * Unwraps the cast a nullable literal converts as, returning any other node unchanged.
   *
   * <p>A nullable literal converts as {@code CAST(literal AS nullable type)}. LogicalValues tuples
   * hold bare literals, and the field's nullability is already declared in the row type, so that
   * cast has to come off before the literal can go into a tuple.
   *
   * @param rexNode the converted node
   * @return the literal the cast wraps, or {@code rexNode} itself
   */
  private RexNode unwrapNullabilityCast(RexNode rexNode) {
    return RexUtil.removeNullabilityCast(typeFactory, rexNode);
  }

  /**
   * Converts the schema a DDL relation declares into the row type that describes it.
   *
   * @param schema the declared schema, whose names are one per field at every level of the struct
   * @return the row type of the object the statement creates
   */
  private RelDataType toRowType(NamedStruct schema) {
    return typeConverter.toCalcite(typeFactory, schema.struct(), schema.names());
  }

  private RelNode handleCreateTableAs(NamedWrite namedWrite, Context context) {
    if (namedWrite.getCreateMode() != AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS
        || namedWrite.getOutputMode() != AbstractWriteRel.OutputMode.NO_OUTPUT) {
      throw new UnsupportedOperationException(
          String.format(
              "Can only handle CTAS NamedWrite with (%s, %s), given (%s, %s)",
              AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS,
              AbstractWriteRel.OutputMode.NO_OUTPUT,
              namedWrite.getCreateMode(),
              namedWrite.getOutputMode()));
    }

    Rel input = namedWrite.getInput();
    RelNode relNode = input.accept(this, context);
    return new CreateTable(namedWrite.getNames(), toRowType(namedWrite.getTableSchema()), relNode);
  }

  @Override
  public RelNode visit(NamedWrite write, Context context) {
    if (write.getRemap().isPresent()) {
      throw new UnsupportedOperationException(
          "Emit mapping on a NamedWrite is not supported: isthmus converts a write to a "
              + "TableModify whose row type is a single ROWCOUNT column, dropping the write's "
              + "output_mode, so the mapping has no columns to select from");
    }
    RelNode input = write.getInput().accept(this, context);
    final RelOptSchema relOptSchema = requireRelOptSchema();
    final RelOptTable targetTable = relOptSchema.getTableForMember(write.getNames());

    TableModify.Operation operation;
    switch (write.getOperation()) {
      case INSERT:
        operation = TableModify.Operation.INSERT;
        break;
      case DELETE:
        operation = TableModify.Operation.DELETE;
        break;
      case CTAS:
        return handleCreateTableAs(write, context);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "NamedWrite with WriteOp %s cannot be converted to a Calcite RelNode. Consider using a more specific Rel (e.g NamedUpdate)",
                write.getOperation()));
    }

    if (targetTable == null) {
      throw new IllegalStateException("Table not found in Calcite catalog: " + write.getNames());
    }

    return LogicalTableModify.create(
        targetTable,
        requireCatalogReader(relOptSchema, write.getNames()),
        input,
        operation,
        null,
        null,
        false);
  }

  /**
   * Returns the {@link RelOptSchema} backing the {@link org.apache.calcite.tools.RelBuilder} used
   * by this converter.
   *
   * @return the Calcite catalog to resolve table names against
   * @throws IllegalStateException if the RelBuilder was created without a RelOptSchema
   */
  protected RelOptSchema requireRelOptSchema() {
    RelOptSchema relOptSchema = relBuilder.getRelOptSchema();
    if (relOptSchema == null) {
      throw new IllegalStateException(
          "The RelBuilder of this converter has no RelOptSchema; a catalog-backed RelBuilder is required to resolve table names");
    }
    return relOptSchema;
  }

  /**
   * Narrows a {@link RelOptSchema} to the {@link org.apache.calcite.prepare.Prepare.CatalogReader}
   * that Calcite's table-modification relations require.
   *
   * <p>{@link RelOptTable#getRelOptSchema()} is nullable and a {@link RelOptSchema} is not
   * necessarily a catalog reader, while {@link TableModify} stores the reader without checking it
   * and only dereferences it later (e.g. from {@code getExpectedInputRowType()}). Validating here
   * reports the misconfigured catalog instead of failing as a {@link NullPointerException} or
   * {@link ClassCastException} further downstream.
   *
   * @param relOptSchema the schema to narrow, possibly null
   * @param names the table name being resolved, for the error message
   * @return the schema as a catalog reader
   * @throws IllegalStateException if the schema is null or is not a catalog reader
   */
  protected static Prepare.CatalogReader requireCatalogReader(
      RelOptSchema relOptSchema, List<String> names) {
    if (!(relOptSchema instanceof Prepare.CatalogReader)) {
      throw new IllegalStateException(
          String.format(
              "Cannot read table metadata for %s: expected a %s, but got %s",
              names,
              Prepare.CatalogReader.class.getName(),
              relOptSchema == null ? "null" : relOptSchema.getClass().getName()));
    }
    return (Prepare.CatalogReader) relOptSchema;
  }

  @Override
  public RelNode visitFallback(Rel rel, Context context) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel %s of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  /**
   * Applies the parts of a relation's {@code RelCommon} that Calcite can hold: its emit mapping
   * first, and then the alternative output field names of its hint.
   *
   * <p>The rest is dropped. A Calcite {@code RelNode} has no place for a relation's alias, its
   * statistics, the computations it saves or loads, or its common advanced extension, and inventing
   * one would mean defining a convention on every consumer's behalf. Its anchor is consumed by the
   * correlation machinery instead.
   *
   * @param relNode the node the relation was converted into
   * @param rel the relation being converted
   * @param inputs the nodes this relation's inputs were converted into
   * @return the node, remapped and renamed
   */
  protected RelNode applyRelCommon(RelNode relNode, Rel rel, RelNode... inputs) {
    return applyOutputNames(applyRemap(relNode, rel.getRemap()), rel, inputs);
  }

  /**
   * Applies the alternative output field names a relation carries in its hint to the node it was
   * converted into.
   *
   * <p>The names are applied to the projection this relation's own conversion produced: the one a
   * {@link Project} becomes, or the one {@link #applyRemap(RelNode, Optional)} adds for a relation
   * with an emit mapping. Anywhere else they are dropped, rather than renaming a node that stands
   * for another relation or adding a projection the plan never asked for. A relation converted into
   * a bare Calcite operator therefore keeps the names Calcite derives, and so does one Calcite
   * builds no operator for at all -- a filter that cannot filter, a sort with no sort fields, an
   * identity emit mapping -- where the node handed back is the input's, which is what the given
   * inputs are compared against.
   *
   * <p>They are dropped as well where the columns of that projection are not the columns of the
   * relation's record type, type by type. An aggregate over several grouping sets types its
   * grouping-set index i32, where the GROUP_ID call standing for it is i64, so the two disagree on
   * what the last column is and binding the names by position would name columns the plan does not
   * name.
   *
   * <p>Only the names of the top-level fields are applied. The names of the fields nested inside
   * them belong to the type of the expression that produces the field, which a projection cannot
   * restate: Calcite compares the two, nested names included, and rejects a projection whose row
   * type says something the expressions do not.
   *
   * <p>A name list that does not fit the relation is dropped as well, rather than failing the
   * conversion: a hint holds no meaning of its own and should not be able to break an otherwise
   * valid plan.
   *
   * <p>The names live in the row type of the projection, which Calcite treats as non-semantic: a
   * planner may discard them, and two projections that differ only in their names share one digest.
   * They describe the tree as it is returned here, and do not survive planning.
   *
   * @param relNode the node to rename, with any emit mapping already applied
   * @param rel the relation being converted
   * @param inputs the nodes this relation's inputs were converted into
   * @return the renamed node, or the node unchanged where the names do not apply
   */
  protected RelNode applyOutputNames(RelNode relNode, Rel rel, RelNode... inputs) {
    List<String> names = rel.getHint().map(Hint::getOutputNames).orElse(List.of());
    if (names.isEmpty() || !(relNode instanceof org.apache.calcite.rel.core.Project)) {
      return relNode;
    }
    for (RelNode input : inputs) {
      if (relNode == input) {
        return relNode;
      }
    }
    org.apache.calcite.rel.core.Project project = (org.apache.calcite.rel.core.Project) relNode;
    RelDataType rowType = project.getRowType();
    List<Type> fields = rel.getRecordType().fields();
    if (names.size() != NamedFieldCountingTypeVisitor.countNames(rel.getRecordType())
        || fields.size() != rowType.getFieldCount()
        || !describesColumnsOf(fields, rowType)) {
      return relNode;
    }
    List<String> fieldNames = new ArrayList<>(fields.size());
    for (int field = 0, nameIndex = 0; field < fields.size(); field++) {
      fieldNames.add(names.get(nameIndex));
      nameIndex += 1 + NamedFieldCountingTypeVisitor.countNames(fields.get(field));
    }
    if (fieldNames.stream().anyMatch(String::isEmpty)
        || new HashSet<>(fieldNames).size() != fieldNames.size()) {
      // Calcite requires the field names of a projection to be distinct and non-empty -- it reads
      // an empty one as the star identifier -- and uniquifying the names here would hand back names
      // the producer did not ask for.
      return relNode;
    }
    return project.copy(
        project.getTraitSet(),
        project.getInput(),
        project.getProjects(),
        typeFactory.createStructType(
            rowType.getStructKind(), RelOptUtil.getFieldTypeList(rowType), fieldNames));
  }

  /**
   * Returns whether the given row type holds the given fields, one for one and in the same order,
   * ignoring nullability: Calcite derives that from the expression producing the column, and a
   * relation's record type says what its columns are, not whether a value is there.
   *
   * @param fields the field types of a relation's record type
   * @param rowType the row type of the node it was converted into
   * @return whether the two describe the same columns in the same order
   */
  private boolean describesColumnsOf(List<Type> fields, RelDataType rowType) {
    for (int field = 0; field < fields.size(); field++) {
      RelDataType declared = typeConverter.toCalcite(typeFactory, fields.get(field));
      if (!SqlTypeUtil.equalSansNullability(
          declared, rowType.getFieldList().get(field).getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Applies an optional field remap to the given node.
   *
   * <p>If {@code remap} is present, the node is projected according to the provided indices;
   * otherwise the original node is returned unchanged.
   *
   * @param relNode the node to remap
   * @param remap optional field index remap
   * @return remapped node or original node if no remap is present
   */
  protected RelNode applyRemap(RelNode relNode, Optional<Rel.Remap> remap) {
    if (remap.isPresent()) {
      return applyRemap(relNode, remap.get());
    }
    return relNode;
  }

  /**
   * Exits a scope entered only to resolve input field types, on an operator that cannot carry
   * correlation variables. Calcite's Aggregate, Sort and Fetch have no {@code variablesSet}, so a
   * correlation resolved against such an operator's input has nowhere to be declared and is
   * rejected rather than silently dropped.
   *
   * @param context the conversion context whose innermost scope is exited
   * @param relType the relation type, used in the failure message
   */
  private static void exitUncorrelatedScope(Context context, Class<? extends Rel> relType) {
    java.util.Set<CorrelationId> correlationIds = context.exitScope();
    if (!correlationIds.isEmpty()) {
      String relName = relType.getSimpleName().toLowerCase(Locale.ROOT);
      throw new UnsupportedOperationException(
          "Outer references bound to the " + relName + " input are not supported");
    }
  }

  private RelNode applyRemap(RelNode relNode, Rel.Remap remap) {
    RelDataType rowType = relNode.getRowType();
    // By index rather than by name: a virtual table's row type comes straight from its schema,
    // which Calcite never uniquifies, so a name can stand for more than one field.
    List<RexNode> rexList =
        remap.indices().stream()
            .map(index -> new RexInputRef(index, rowType.getFieldList().get(index).getType()))
            .collect(java.util.stream.Collectors.toList());
    return relBuilder.push(relNode).project(rexList).build();
  }

  /**
   * A relational input paired with the {@code rel_anchor} it may carry, used by {@link Context}.
   */
  public static final class AnchoredInput {
    final Optional<Integer> anchor;
    final RelDataType rowType;

    private AnchoredInput(Optional<Integer> anchor, RelDataType rowType) {
      this.anchor = anchor;
      this.rowType = rowType;
    }

    /**
     * Creates an anchored input.
     *
     * @param anchor the input's {@code rel_anchor}, if any
     * @param rowType the input's Calcite row type
     * @return the anchored input
     */
    public static AnchoredInput of(Optional<Integer> anchor, RelDataType rowType) {
      return new AnchoredInput(anchor, rowType);
    }
  }

  /**
   * A shared context for the Substrait to RelNode conversion.
   *
   * <p>Correlated (outer) references are resolved by id: a relation that binds an outer reference
   * carries a {@code rel_anchor}, and each reference carries the matching {@code rel_reference}.
   * When a relational operator is converted, it enters a scope recording the anchors of its inputs;
   * a reference then resolves against the enclosing scope owning its anchor, and the {@link
   * CorrelationId} minted for that anchor is attached to the operator that owns it.
   */
  public static class Context implements VisitationContext {

    /** Stack of correlation scopes, innermost on top. */
    private final Deque<Scope> scopes = new ArrayDeque<>();

    /** Maps an in-scope {@code rel_anchor} to the scope that owns it. */
    private final Map<Integer, Scope> scopeByAnchor = new HashMap<>();

    /** Maps a {@code rel_anchor} to the single {@link CorrelationId} minted for it. */
    private final Map<Integer, CorrelationId> correlationIdByAnchor = new HashMap<>();

    /** Lambda parameter types by nesting level, innermost on top. */
    private final Deque<List<RelDataType>> lambdaParameterTypes = new ArrayDeque<>();

    /**
     * Every {@code rel_anchor} that has entered a scope. Resolution keys {@link #scopeByAnchor} and
     * {@link #correlationIdByAnchor} purely by anchor value, which is only sound if anchors are
     * unique plan-wide (as required by {@link io.substrait.relation.Rel#getRelAnchor()}). This set
     * lets {@link #enterScope} reject a plan that reuses an anchor for two distinct relations
     * rather than silently mis-resolving references.
     */
    private final java.util.Set<Integer> seenAnchors = new HashSet<>();

    /** One correlation scope per enclosing relational operator. */
    private static final class Scope {
      final List<RelDataType> inputRowTypes = new ArrayList<>();
      final Map<Integer, RelDataType> rowTypeByAnchor = new HashMap<>();
      final java.util.Set<CorrelationId> correlationIds = new HashSet<>();
    }

    /**
     * Creates a new {@link Context} instance.
     *
     * @return the new {@link Context} instance
     */
    public static Context newContext() {
      return new Context();
    }

    /**
     * Enters a correlation scope for a relational operator, recording the {@code rel_anchor} (if
     * any) and row type carried by each of its inputs.
     *
     * @param inputs the operator's inputs paired with their anchors
     */
    public void enterScope(final AnchoredInput... inputs) {
      final Scope scope = new Scope();
      for (final AnchoredInput input : inputs) {
        scope.inputRowTypes.add(input.rowType);
        if (input.anchor.isPresent()) {
          final int anchor = input.anchor.get();
          if (!seenAnchors.add(anchor)) {
            throw new UnsupportedOperationException(
                "Duplicate rel_anchor="
                    + anchor
                    + "; rel_anchors must be unique plan-wide for id-based outer references to "
                    + "resolve unambiguously");
          }
          scope.rowTypeByAnchor.put(anchor, input.rowType);
          scopeByAnchor.put(anchor, scope);
        }
      }
      scopes.push(scope);
    }

    /**
     * Returns the type of a field in the current operator's flattened input row.
     *
     * @param fieldIndex zero-based field index across all current inputs
     * @return the Calcite field type from the input relation
     * @throws IllegalStateException if expression conversion has no current relational input
     * @throws IndexOutOfBoundsException if the field index is outside the flattened input row
     */
    public RelDataType getInputFieldType(final int fieldIndex) {
      if (scopes.isEmpty()) {
        throw new IllegalStateException("No input row type is available for field reference");
      }

      int remainingIndex = fieldIndex;
      int fieldCount = 0;
      for (final RelDataType inputRowType : scopes.peek().inputRowTypes) {
        final int inputFieldCount = inputRowType.getFieldCount();
        fieldCount += inputFieldCount;
        if (remainingIndex < inputFieldCount) {
          return inputRowType.getFieldList().get(remainingIndex).getType();
        }
        remainingIndex -= inputFieldCount;
      }
      throw new IndexOutOfBoundsException(
          "Field index " + fieldIndex + " is outside input row with " + fieldCount + " fields");
    }

    /**
     * Enters a lambda scope with its Calcite parameter types.
     *
     * @param parameterTypes parameter types in declaration order
     */
    public void enterLambdaScope(final List<RelDataType> parameterTypes) {
      lambdaParameterTypes.push(List.copyOf(parameterTypes));
    }

    /** Exits the innermost lambda scope. */
    public void exitLambdaScope() {
      lambdaParameterTypes.pop();
    }

    /**
     * Returns a parameter type from the innermost lambda scope.
     *
     * @param parameterIndex zero-based parameter index
     * @return the Calcite parameter type
     * @throws IllegalStateException if expression conversion has no current lambda
     * @throws IndexOutOfBoundsException if the index is outside the lambda's parameter list
     */
    public RelDataType getLambdaParameterType(final int parameterIndex) {
      if (lambdaParameterTypes.isEmpty()) {
        throw new IllegalStateException(
            "No lambda parameter type is available for field reference");
      }
      return lambdaParameterTypes.peek().get(parameterIndex);
    }

    /**
     * Exits the innermost correlation scope, returning the correlation ids to attach to the
     * operator that owns it.
     *
     * @return the correlation ids resolved against this operator's inputs
     */
    public java.util.Set<CorrelationId> exitScope() {
      final Scope scope = scopes.pop();
      for (final Integer anchor : scope.rowTypeByAnchor.keySet()) {
        scopeByAnchor.remove(anchor);
      }
      return scope.correlationIds;
    }

    /**
     * Returns the Calcite row type of the in-scope relation bearing the given {@code rel_anchor}.
     *
     * @param anchor the referenced {@code rel_anchor}
     * @return the row type of the binding relation
     */
    public RelDataType getAnchorRowType(final int anchor) {
      final Scope scope = requireScope(anchor);
      return scope.rowTypeByAnchor.get(anchor);
    }

    /**
     * Returns the {@link CorrelationId} for the given {@code rel_anchor}, creating it on first use
     * and attaching it to the scope that owns the anchor so the binding operator declares it.
     *
     * @param anchor the referenced {@code rel_anchor}
     * @param factory supplies a fresh correlation id when one has not yet been minted
     * @return the correlation id for this anchor
     */
    public CorrelationId correlationIdForAnchor(
        final int anchor, final java.util.function.Supplier<CorrelationId> factory) {
      final Scope scope = requireScope(anchor);
      final CorrelationId correlationId =
          correlationIdByAnchor.computeIfAbsent(anchor, k -> factory.get());
      scope.correlationIds.add(correlationId);
      return correlationId;
    }

    private Scope requireScope(final int anchor) {
      final Scope scope = scopeByAnchor.get(anchor);
      if (scope == null) {
        // The anchor is not on the active scope stack: the referenced relation is not an enclosing
        // single-input host. This includes forward references and shared subtrees reached via a
        // ReferenceRel. Signalled as unsupported, consistent with OuterReferenceConverter.
        throw new UnsupportedOperationException(
            "Outer reference rel_reference="
                + anchor
                + " has no enclosing relation with that anchor");
      }
      return scope;
    }
  }

  /**
   * Returns the {@link RelBuilder} of this converter.
   *
   * @return the {@link RelBuilder}
   */
  public RelBuilder getRelBuilder() {
    return relBuilder;
  }
}
