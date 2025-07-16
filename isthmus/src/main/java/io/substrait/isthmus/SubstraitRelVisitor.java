package io.substrait.isthmus;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.LiteralConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

@SuppressWarnings("UnstableApiUsage")
@Value.Enclosing
public class SubstraitRelVisitor extends RelNodeVisitor<Rel, RuntimeException> {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SubstraitRelVisitor.class);
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
    var converters = new ArrayList<CallConverter>();
    converters.addAll(CallConverters.defaults(typeConverter));
    converters.add(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory));
    converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
    this.aggregateFunctionConverter =
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory);
    var windowFunctionConverter =
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
    var converters = new ArrayList<CallConverter>();
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
    var type = typeConverter.toNamedStruct(scan.getRowType());
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
    var type = typeConverter.toNamedStruct(values.getRowType());
    if (values.getTuples().isEmpty()) {
      return EmptyScan.builder().initialSchema(type).build();
    }

    LiteralConverter literalConverter = new LiteralConverter(typeConverter);
    List<Expression.StructLiteral> structs =
        values.getTuples().stream()
            .map(
                list -> {
                  var fields =
                      list.stream()
                          .map(l -> literalConverter.convert(l))
                          .collect(Collectors.toList());
                  return ExpressionCreator.struct(false, fields);
                })
            .collect(Collectors.toList());
    return VirtualTableScan.builder().initialSchema(type).addAllRows(structs).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Filter filter) {
    var condition = toExpression(filter.getCondition());
    return Filter.builder().condition(condition).input(apply(filter.getInput())).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Calc calc) {
    return super.visit(calc);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Project project) {
    var expressions =
        project.getProjects().stream()
            .map(this::toExpression)
            .collect(java.util.stream.Collectors.toList());

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
    var left = apply(join.getLeft());
    var right = apply(join.getRight());
    var condition = toExpression(join.getCondition());
    var joinType =
        switch (join.getJoinType()) {
          case INNER -> Join.JoinType.INNER;
          case LEFT -> Join.JoinType.LEFT;
          case RIGHT -> Join.JoinType.RIGHT;
          case FULL -> Join.JoinType.OUTER;
          case SEMI -> Join.JoinType.LEFT_SEMI;
          case ANTI -> Join.JoinType.LEFT_ANTI;
          default -> throw new UnsupportedOperationException(
              "Unsupported join type: " + join.getJoinType());
        };

    // An INNER JOIN with a join condition of TRUE can be encoded as a Substrait Cross relation
    if (joinType == Join.JoinType.INNER && TRUE.equals(condition)) {
      return Cross.builder().left(left).right(right).build();
    }
    return Join.builder().condition(condition).joinType(joinType).left(left).right(right).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Correlate correlate) {
    // left input of correlated-join is similar to the left input of a logical join
    apply(correlate.getLeft());

    // right input of correlated-join is similar to a correlated sub-query
    apply(correlate.getRight());

    var joinType =
        switch (correlate.getJoinType()) {
          case INNER -> Join.JoinType.INNER; // corresponds to CROSS APPLY join
          case LEFT -> Join.JoinType.LEFT; // corresponds to OUTER APPLY join
          default -> throw new IllegalArgumentException(
              "Invalid correlated join type: " + correlate.getJoinType());
        };
    return super.visit(correlate);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Union union) {
    var inputs = apply(union.getInputs());
    var setOp = union.all ? Set.SetOp.UNION_ALL : Set.SetOp.UNION_DISTINCT;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Intersect intersect) {
    var inputs = apply(intersect.getInputs());
    var setOp =
        intersect.all ? Set.SetOp.INTERSECTION_MULTISET_ALL : Set.SetOp.INTERSECTION_MULTISET;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Minus minus) {
    var inputs = apply(minus.getInputs());
    var setOp = minus.all ? Set.SetOp.MINUS_PRIMARY_ALL : Set.SetOp.MINUS_PRIMARY;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Aggregate aggregate) {
    var input = apply(aggregate.getInput());
    Stream<ImmutableBitSet> sets;
    if (aggregate.groupSets != null) {
      sets = aggregate.groupSets.stream();
    } else {
      sets = Stream.of(aggregate.getGroupSet());
    }

    var groupings =
        sets.filter(s -> s != null).map(s -> fromGroupSet(s, input)).collect(Collectors.toList());

    var aggCalls =
        aggregate.getAggCallList().stream()
            .map(c -> fromAggCall(aggregate.getInput(), input.getRecordType(), c))
            .collect(Collectors.toList());

    return Aggregate.builder()
        .input(input)
        .addAllGroupings(groupings)
        .addAllMeasures(aggCalls)
        .build();
  }

  Aggregate.Grouping fromGroupSet(ImmutableBitSet bitSet, Rel input) {
    List<Expression> references =
        bitSet.asList().stream()
            .map(i -> FieldReference.newInputRelReference(i, input))
            .collect(Collectors.toList());
    return Aggregate.Grouping.builder().addAllExpressions(references).build();
  }

  Aggregate.Measure fromAggCall(RelNode input, Type.Struct inputType, AggregateCall call) {
    var invocation =
        aggregateFunctionConverter.convert(
            input, inputType, call, t -> t.accept(rexExpressionConverter));
    if (!invocation.isPresent()) {
      throw new UnsupportedOperationException("Unable to find binding for call " + call);
    }
    var builder = Aggregate.Measure.builder().function(invocation.get());
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

      var builder = Fetch.builder().input(output).offset(offset).count(count);
      output = builder.build();
    }

    return output;
  }

  private long asLong(RexNode rex) {
    var expr = toExpression(rex);
    if (expr instanceof Expression.I64Literal i) {
      return i.value();
    } else if (expr instanceof Expression.I32Literal i) {
      return i.value();
    }
    throw new UnsupportedOperationException("Unknown type: " + rex);
  }

  public static Expression.SortField toSortField(
      RelFieldCollation collation, Type.Struct inputType) {
    Expression.SortDirection direction =
        switch (collation.direction) {
          case STRICTLY_ASCENDING, ASCENDING -> collation.nullDirection
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.ASC_NULLS_LAST
              : Expression.SortDirection.ASC_NULLS_FIRST;
          case STRICTLY_DESCENDING, DESCENDING -> collation.nullDirection
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.DESC_NULLS_LAST
              : Expression.SortDirection.DESC_NULLS_FIRST;
          case CLUSTERED -> Expression.SortDirection.CLUSTERED;
        };

    return Expression.SortField.builder()
        .expr(FieldReference.newRootStructReference(collation.getFieldIndex(), inputType))
        .direction(direction)
        .build();
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.Exchange exchange) {
    return super.visit(exchange);
  }

  @Override
  public Rel visit(org.apache.calcite.rel.core.TableModify modify) {
    return super.visit(modify);
  }

  @Override
  public Rel visitOther(RelNode other) {
    throw new UnsupportedOperationException("Unable to handle node: " + other);
  }

  private void popFieldAccessDepthMap(RelNode root) {
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

  public static Plan.Root convert(RelRoot relRoot, SimpleExtension.ExtensionCollection extensions) {
    return convert(relRoot, extensions, FEATURES_DEFAULT);
  }

  public static Plan.Root convert(
      RelRoot relRoot, SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    SubstraitRelVisitor visitor =
        new SubstraitRelVisitor(relRoot.rel.getCluster().getTypeFactory(), extensions, features);
    visitor.popFieldAccessDepthMap(relRoot.rel);
    Rel rel = visitor.apply(relRoot.project());

    // Avoid using the names from relRoot.validatedRowType because if there are
    // nested types (i.e ROW, MAP, etc) the typeConverter will pad names correctly
    List<String> names = visitor.typeConverter.toNamedStruct(relRoot.validatedRowType).names();
    return Plan.Root.builder().input(rel).names(names).build();
  }

  public static Rel convert(RelNode relNode, SimpleExtension.ExtensionCollection extensions) {
    return convert(relNode, extensions, FEATURES_DEFAULT);
  }

  public static Rel convert(
      RelNode relNode, SimpleExtension.ExtensionCollection extensions, FeatureBoard features) {
    SubstraitRelVisitor visitor =
        new SubstraitRelVisitor(relNode.getCluster().getTypeFactory(), extensions, features);
    visitor.popFieldAccessDepthMap(relNode);
    return visitor.apply(relNode);
  }
}
