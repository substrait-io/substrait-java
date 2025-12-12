package io.substrait.isthmus;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.calcite.rel.CreateView;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.LiteralConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Aggregate.Grouping;
import io.substrait.relation.Aggregate.Measure;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

@SuppressWarnings("UnstableApiUsage")
@Value.Enclosing
public class SubstraitRelVisitor extends RelNodeVisitor<Rel, RuntimeException> {

  private static final FeatureBoard FEATURES_DEFAULT = ImmutableFeatureBoard.builder().build();
  private static final Expression.BoolLiteral TRUE = ExpressionCreator.bool(false, true);

  protected final RexExpressionConverter rexExpressionConverter;
  protected final AggregateFunctionConverter aggregateFunctionConverter;
  protected final TypeConverter typeConverter;
  protected final FeatureBoard featureBoard;
  private Map<RexFieldAccess, Integer> fieldAccessDepthMap;

  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    this(typeFactory, extensions, FEATURES_DEFAULT);
  }

  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensions,
      FeatureBoard features) {

    this.typeConverter = TypeConverter.DEFAULT;
    ArrayList<CallConverter> converters = new ArrayList<>();
    converters.addAll(CallConverters.defaults(typeConverter));

    if (features.allowDynamicUdfs()) {
      SimpleExtension.ExtensionCollection dynamicExtensionCollection =
          ExtensionUtils.getDynamicExtensions(extensions);
      List<SqlOperator> dynamicOperators =
          SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, typeFactory);

      List<FunctionMappings.Sig> additionalSignatures =
          dynamicOperators.stream()
              .map(op -> FunctionMappings.s(op, op.getName()))
              .collect(Collectors.toList());
      converters.add(
          new ScalarFunctionConverter(
              extensions.scalarFunctions(),
              additionalSignatures,
              typeFactory,
              TypeConverter.DEFAULT));
    } else {
      converters.add(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory));
    }

    converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
    this.aggregateFunctionConverter =
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory);
    WindowFunctionConverter windowFunctionConverter =
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);
    this.rexExpressionConverter =
        new RexExpressionConverter(this, converters, windowFunctionConverter, typeConverter);
    this.featureBoard = features;
  }

  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory,
      ScalarFunctionConverter scalarFunctionConverter,
      AggregateFunctionConverter aggregateFunctionConverter,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter,
      FeatureBoard features) {
    ArrayList<CallConverter> converters = new ArrayList<CallConverter>();
    converters.addAll(CallConverters.defaults(typeConverter));
    converters.add(scalarFunctionConverter);
    converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
    this.aggregateFunctionConverter = aggregateFunctionConverter;
    this.rexExpressionConverter =
        new RexExpressionConverter(this, converters, windowFunctionConverter, typeConverter);
    this.typeConverter = typeConverter;
    this.featureBoard = features;
  }

  protected Expression toExpression(RexNode node) {
    return node.accept(rexExpressionConverter);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.TableScan scan) {
    NamedStruct type = typeConverter.toNamedStruct(scan.getRowType());
    return NamedScan.builder()
        .initialSchema(type)
        .addAllNames(scan.getTable().getQualifiedName())
        .build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.TableFunctionScan scan) {
    return super.visit(scan);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Values values) {
    NamedStruct type = typeConverter.toNamedStruct(values.getRowType());
    if (values.getTuples().isEmpty()) {
      return EmptyScan.builder().initialSchema(type).build();
    }
    LiteralConverter literalConverter = new LiteralConverter(typeConverter);
    List<Expression.NestedStruct> structs =
        values.getTuples().stream()
            .map(
                list -> {
                  List<Expression> fields =
                      list.stream()
                          .map(l -> literalConverter.convert(l))
                          .collect(Collectors.toUnmodifiableList());
                  return ExpressionCreator.nestedStruct(false, fields);
                })
            .collect(Collectors.toUnmodifiableList());
    return VirtualTableScan.builder().initialSchema(type).addAllRows(structs).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Filter filter) {
    Expression condition = toExpression(filter.getCondition());
    return Filter.builder().condition(condition).input(apply(filter.getInput())).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Calc calc) {
    return super.visit(calc);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Project project) {
    List<Expression> expressions =
        project.getProjects().stream()
            .map(this::toExpression)
            .collect(java.util.stream.Collectors.toList());

    // if there are no input fields, no remap is necessary
    if (project.getInput().getRowType().getFieldCount() == 0) {
      return Project.builder().expressions(expressions).input(apply(project.getInput())).build();
    }

    // todo: eliminate excessive projects. This should be done by converting rexinputrefs to remaps.
    return Project.builder()
        .remap(
            Rel.Remap.offset(project.getInput().getRowType().getFieldCount(), expressions.size()))
        .expressions(expressions)
        .input(apply(project.getInput()))
        .build();
  }

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

  @Override
  public Rel visit(org.apache.calcite.rel.core.Correlate correlate) {
    // left input of correlated-join is similar to the left input of a logical join
    apply(correlate.getLeft());

    // right input of correlated-join is similar to a correlated sub-query
    apply(correlate.getRight());

    return super.visit(correlate);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Union union) {
    List<Rel> inputs = apply(union.getInputs());
    Set.SetOp setOp = union.all ? Set.SetOp.UNION_ALL : Set.SetOp.UNION_DISTINCT;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Intersect intersect) {
    List<Rel> inputs = apply(intersect.getInputs());
    Set.SetOp setOp =
        intersect.all ? Set.SetOp.INTERSECTION_MULTISET_ALL : Set.SetOp.INTERSECTION_MULTISET;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Minus minus) {
    List<Rel> inputs = apply(minus.getInputs());
    Set.SetOp setOp = minus.all ? Set.SetOp.MINUS_PRIMARY_ALL : Set.SetOp.MINUS_PRIMARY;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Aggregate aggregate) {
    Rel input = apply(aggregate.getInput());
    Stream<ImmutableBitSet> sets;
    if (aggregate.groupSets != null) {
      sets = aggregate.groupSets.stream();
    } else {
      sets = Stream.of(aggregate.getGroupSet());
    }

    List<Grouping> groupings =
        sets.filter(s -> s != null).map(s -> fromGroupSet(s, input)).collect(Collectors.toList());

    // get GROUP_ID() function calls
    List<AggregateCall> groupIdCalls =
        aggregate.getAggCallList().stream()
            .filter(c -> c.getAggregation().equals(SqlStdOperatorTable.GROUP_ID))
            .collect(Collectors.toList());

    List<AggregateCall> filteredAggCalls =
        aggregate.getAggCallList().stream()
            // remove GROUP_ID() function calls
            .filter(c -> !groupIdCalls.contains(c))
            .collect(Collectors.toList());

    List<Measure> aggCalls =
        filteredAggCalls.stream()
            .map(c -> fromAggCall(aggregate.getInput(), input.getRecordType(), c))
            .collect(Collectors.toList());

    ImmutableAggregate.Builder builder =
        Aggregate.builder().input(input).addAllGroupings(groupings).addAllMeasures(aggCalls);

    if (groupings.size() > 1) {
      // remove the grouping set index if there was no explicit GROUP_ID() function call
      if (groupIdCalls.isEmpty()) {
        int groupingExprSize =
            Math.toIntExact(
                groupings.stream().flatMap(g -> g.getExpressions().stream()).distinct().count());
        builder.remap(Remap.offset(0, groupingExprSize + aggCalls.size()));
      } else {
        // remap grouping set index at the field positions where the GROUP_ID() function calls were
        final int groupingFieldCount =
            Math.toIntExact(groupings.stream().flatMap(g -> g.getExpressions().stream()).count());
        final int filterAggCallCount = aggCalls.size();
        final Integer groupingSetIndex = groupingFieldCount + filterAggCallCount;

        final List<Integer> remap =
            IntStream.range(0, groupingFieldCount)
                .mapToObj(i -> i)
                .collect(Collectors.toCollection(ArrayList::new));

        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
          AggregateCall aggCall = aggregate.getAggCallList().get(i);
          if (filteredAggCalls.contains(aggCall)) {
            remap.add(
                i + groupingFieldCount, filteredAggCalls.indexOf(aggCall) + groupingFieldCount);
          } else if (groupIdCalls.contains(aggCall)) {
            remap.add(i + groupingFieldCount, groupingSetIndex);
          } else {
            // this should never get triggered
            throw new IllegalStateException(
                "encountered AggregateCall that is neither in filteredAggCalls nor in groupIdCalls"
                    + aggCall);
          }
        }

        builder.remap(Remap.of(remap));
      }
    }

    return builder.build();
  }

  Aggregate.Grouping fromGroupSet(ImmutableBitSet bitSet, Rel input) {
    List<Expression> references =
        bitSet.asList().stream()
            .map(i -> FieldReference.newInputRelReference(i, input))
            .collect(Collectors.toList());
    return Aggregate.Grouping.builder().addAllExpressions(references).build();
  }

  Aggregate.Measure fromAggCall(RelNode input, Type.Struct inputType, AggregateCall call) {
    Optional<AggregateFunctionInvocation> invocation =
        aggregateFunctionConverter.convert(
            input, inputType, call, t -> t.accept(rexExpressionConverter));
    if (invocation.isEmpty()) {
      throw new UnsupportedOperationException("Unable to find binding for call " + call);
    }
    Builder builder = Aggregate.Measure.builder().function(invocation.get());
    if (call.filterArg != -1) {
      builder.preMeasureFilter(FieldReference.newRootStructReference(call.filterArg, inputType));
    }
    // TODO: handle the collation on the AggregateCall
    //   https://github.com/substrait-io/substrait-java/issues/215
    return builder.build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Match match) {
    return super.visit(match);
  }

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
      Long offset = Optional.ofNullable(sort.offset).map(this::asLong).orElse(0L);
      OptionalLong count =
          Optional.ofNullable(sort.fetch)
              .map(r -> OptionalLong.of(asLong(r)))
              .orElse(OptionalLong.empty());

      ImmutableFetch.Builder builder = Fetch.builder().input(output).offset(offset).count(count);
      output = builder.build();
    }

    return output;
  }

  private long asLong(RexNode rex) {
    Expression expr = toExpression(rex);
    if (expr instanceof Expression.I64Literal) {
      return ((Expression.I64Literal) expr).value();
    } else if (expr instanceof Expression.I32Literal) {
      return ((Expression.I32Literal) expr).value();
    }
    throw new UnsupportedOperationException("Unknown type: " + rex);
  }

  public static Expression.SortField toSortField(
      RelFieldCollation collation, Type.Struct inputType) {
    Expression.SortDirection direction = asSortDirection(collation);

    return Expression.SortField.builder()
        .expr(FieldReference.newRootStructReference(collation.getFieldIndex(), inputType))
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

  @Override
  public Rel visit(org.apache.calcite.rel.core.Exchange exchange) {
    return super.visit(exchange);
  }

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

          assert modify.getTable() != null;
          return NamedWrite.builder()
              .input(input)
              .tableSchema(typeConverter.toNamedStruct(modify.getTable().getRowType()))
              .operation(op)
              .createMode(AbstractWriteRel.CreateMode.UNSPECIFIED)
              .outputMode(AbstractWriteRel.OutputMode.MODIFIED_RECORDS)
              .names(modify.getTable().getQualifiedName())
              .build();
        }

      case UPDATE:
        {
          assert modify.getTable() != null;

          RelNode input = modify.getInput();
          final Expression condition;
          if (input instanceof org.apache.calcite.rel.core.Filter) {
            org.apache.calcite.rel.core.Filter filter = (org.apache.calcite.rel.core.Filter) input;
            condition = toExpression(filter.getCondition());
          } else {
            condition = Expression.BoolLiteral.builder().nullable(false).value(true).build();
          }

          List<String> updateColumnNames = modify.getUpdateColumnList();
          List<RexNode> sourceExpressions = getSourceExpressions(modify);
          List<String> allTableColumnNames = modify.getTable().getRowType().getFieldNames();
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
              .tableSchema(typeConverter.toNamedStruct(modify.getTable().getRowType()))
              .names(modify.getTable().getQualifiedName())
              .condition(condition)
              .transformations(transformations)
              .build();
        }

      default:
        return super.visit(modify);
    }
  }

  private List<RexNode> getSourceExpressions(TableModify modify) {
    List<RexNode> results = modify.getSourceExpressionList();
    if (results == null) {
      return Collections.emptyList();
    }

    RelNode input = modify.getInput();
    if (input instanceof org.apache.calcite.rel.core.Project) {
      return resolveProjectedRefs(results, (org.apache.calcite.rel.core.Project) input);
    }

    return results;
  }

  private List<RexNode> resolveProjectedRefs(
      List<RexNode> expressions, org.apache.calcite.rel.core.Project project) {
    List<RexNode> projects = project.getProjects();
    return expressions.stream()
        .map(
            expression -> {
              if (expression instanceof RexInputRef) {
                int refIndex = ((RexInputRef) expression).getIndex();
                return projects.get(refIndex);
              }

              return expression;
            })
        .collect(Collectors.toList());
  }

  private NamedStruct getSchema(final RelNode queryRelRoot) {
    final RelDataType rowType = queryRelRoot.getRowType();
    return typeConverter.toNamedStruct(rowType);
  }

  public Rel handleCreateTable(CreateTable createTable) {
    RelNode input = createTable.getInput();
    Rel inputRel = apply(input);
    NamedStruct schema = getSchema(input);
    return NamedWrite.builder()
        .input(inputRel)
        .tableSchema(schema)
        .operation(AbstractWriteRel.WriteOp.CTAS)
        .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
        .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
        .names(createTable.getTableName())
        .build();
  }

  public Rel handleCreateView(CreateView createView) {
    RelNode input = createView.getInput();
    Rel inputRel = apply(input);

    final Expression.StructLiteral defaults = ExpressionCreator.struct(false);

    return NamedDdl.builder()
        .viewDefinition(inputRel)
        .tableSchema(getSchema(input))
        .tableDefaults(defaults)
        .operation(AbstractDdlRel.DdlOp.CREATE)
        .object(AbstractDdlRel.DdlObject.VIEW)
        .names(createView.getViewName())
        .build();
  }

  @Override
  public Rel visitOther(RelNode other) {
    if (other instanceof CreateTable) {
      return handleCreateTable((CreateTable) other);

    } else if (other instanceof CreateView) {
      return handleCreateView((CreateView) other);
    }
    throw new UnsupportedOperationException("Unable to handle node: " + other);
  }

  protected void popFieldAccessDepthMap(RelNode root) {
    final OuterReferenceResolver resolver = new OuterReferenceResolver();
    resolver.apply(root);
    fieldAccessDepthMap = resolver.getFieldAccessDepthMap();
  }

  public Integer getFieldAccessDepth(RexFieldAccess fieldAccess) {
    return fieldAccessDepthMap.get(fieldAccess);
  }

  public Rel apply(RelNode r) {
    return reverseAccept(r);
  }

  public List<Rel> apply(List<RelNode> inputs) {
    return inputs.stream()
        .map(inputRel -> apply(inputRel))
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Converts a Calcite {@link RelRoot} to a Substrait {@link Plan.Root} using default features.
   *
   * <p>This is a convenience method that delegates to {@link #convert(RelRoot,
   * SimpleExtension.ExtensionCollection, FeatureBoard)} using {@link #FEATURES_DEFAULT}.
   *
   * @param relRoot The Calcite RelRoot to convert.
   * @param extensions The extension collection to use for the conversion.
   * @return The resulting Substrait Plan.Root.
   */
  public static Plan.Root convert(RelRoot relRoot, SimpleExtension.ExtensionCollection extensions) {
    return convert(relRoot, extensions, FEATURES_DEFAULT);
  }

  /**
   * Converts a Calcite {@link RelRoot} to a Substrait {@link Plan.Root} using a custom visitor.
   *
   * <p>This is the main conversion entry point for a complete plan. It applies the provided {@link
   * SubstraitRelVisitor} to the final projected {@link RelNode} from the {@code relRoot}, and wraps
   * the resulting {@link Rel} in a {@link Plan.Root}.
   *
   * <p>This method also correctly extracts the final output field names, paying special attention
   * to nested types (structs, maps) via the visitor's type converter, rather than using the names
   * from {@code relRoot.validatedRowType} directly.
   *
   * @param relRoot The Calcite RelRoot to convert. This is expected to be a complete, optimized
   *     plan.
   * @param visitor {@link SubstraitRelVisitor} or its subclass. This allows for custom visitor
   *     behavior.
   * @return The resulting Substrait Plan.Root, containing the converted relational tree and the
   *     output names.
   */
  public static Plan.Root convert(RelRoot relRoot, SubstraitRelVisitor visitor) {
    visitor.popFieldAccessDepthMap(relRoot.rel);
    Rel rel = visitor.apply(relRoot.project());

    // Avoid using the names from relRoot.validatedRowType because if there are
    // nested types (i.e ROW, MAP, etc) the typeConverter will pad names correctly
    List<String> names = visitor.typeConverter.toNamedStruct(relRoot.validatedRowType).names();
    return Plan.Root.builder().input(rel).names(names).build();
  }

  /**
   * Converts a Calcite {@link RelRoot} to a Substrait {@link Plan.Root} using the specified
   * features.
   *
   * <p>This is a convenience method that delegates to {@link #convert(RelRoot,
   * SubstraitRelVisitor)} using an instance of the {@link SubstraitRelVisitor} as the visitor.
   *
   * @param relRoot The Calcite RelRoot to convert.
   * @param extensions The extension collection to use for the conversion.
   * @param features The feature board specifying enabled Substrait features.
   * @return The resulting Substrait Plan.Root.
   */
  public static Plan.Root convert(
      RelRoot relRoot, SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    return convert(
        relRoot,
        new SubstraitRelVisitor(relRoot.rel.getCluster().getTypeFactory(), extensions, features));
  }

  /**
   * Converts a Calcite {@link RelNode} to a Substrait {@link Rel} using default features.
   *
   * <p>This method is suitable for converting a relational sub-tree, but it does not produce a
   * {@link Plan.Root}. For a complete plan conversion, use {@link #convert(RelRoot,
   * SimpleExtension.ExtensionCollection)}.
   *
   * <p>This is a convenience method that delegates to {@link #convert(RelNode,
   * SimpleExtension.ExtensionCollection, FeatureBoard)} using {@link #FEATURES_DEFAULT}.
   *
   * @param relNode The Calcite RelNode (and its subtree) to convert.
   * @param extensions The extension collection to use for the conversion.
   * @return The resulting Substrait Rel.
   */
  public static Rel convert(RelNode relNode, SimpleExtension.ExtensionCollection extensions) {
    return convert(relNode, extensions, FEATURES_DEFAULT);
  }

  /**
   * Converts a Calcite {@link RelNode} to a Substrait {@link Rel} using a custom visitor.
   *
   * <p>This is the main conversion entry point for a partial plan or a single node (and its
   * children). It applies the provided {@link SubstraitRelVisitor} to the given {@code relNode}.
   *
   * <p>This method does not wrap the result in a {@link Plan.Root} or extract output names. For
   * that, use {@link #convert(RelRoot, SubstraitRelVisitor)}.
   *
   * @param relNode The Calcite RelNode (and its subtree) to convert.
   * @param visitor {@link SubstraitRelVisitor} or its subclass. This allows for custom visitor
   *     behavior.
   * @return The resulting Substrait Rel.
   */
  public static Rel convert(RelNode relNode, SubstraitRelVisitor visitor) {
    visitor.popFieldAccessDepthMap(relNode);
    return visitor.apply(relNode);
  }

  /**
   * Converts a Calcite {@link RelNode} to a Substrait {@link Rel} using the specified features.
   *
   * <p>This method is suitable for converting a relational sub-tree, but it does not produce a
   * {@link Plan.Root}. For a complete plan conversion, use {@link #convert(RelRoot,
   * SimpleExtension.ExtensionCollection, FeatureBoard)}.
   *
   * <p>This is a convenience method that delegates to {@link #convert(RelNode,
   * SubstraitRelVisitor)} using an instance of the {@link SubstraitRelVisitor} as the visitor.
   *
   * @param relNode The Calcite RelNode (and its subtree) to convert.
   * @param extensions The extension collection to use for the conversion.
   * @param features The feature board specifying enabled Substrait features.
   * @return The resulting Substrait Rel.
   */
  public static Rel convert(
      RelNode relNode, SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    return convert(
        relNode,
        new SubstraitRelVisitor(relNode.getCluster().getTypeFactory(), extensions, features));
  }
}
