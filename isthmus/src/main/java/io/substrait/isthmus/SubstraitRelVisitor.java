package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy.KEEP_AS_CROSS_JOIN;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.expression.*;
import io.substrait.relation.*;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
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
  public static final Options OPTIONS = new Options();
  private static final Expression.BoolLiteral TRUE = ExpressionCreator.bool(false, true);

  private final SimpleExtension.ExtensionCollection extensions;
  private final RexExpressionConverter converter;
  private final AggregateFunctionConverter aggregateFunctionConverter;
  private Map<RexFieldAccess, Integer> fieldAccessDepthMap;

  private final Options options;

  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    this(typeFactory, extensions, OPTIONS);
  }

  public SubstraitRelVisitor(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensions,
      Options options) {
    this.extensions = extensions;
    var converters = new ArrayList<CallConverter>();
    converters.addAll(CallConverters.DEFAULTS);
    converters.add(new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory));
    converters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
    this.aggregateFunctionConverter =
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory);
    var windowFunctionConverter =
        new WindowFunctionConverter(
            extensions.windowFunctions(), typeFactory, aggregateFunctionConverter);
    this.converter = new RexExpressionConverter(this, converters, windowFunctionConverter);
    this.options = options;
  }

  private Expression toExpression(RexNode node) {
    return node.accept(converter);
  }

  @Override
  public Rel visit(TableScan scan) {
    var type = TypeConverter.toNamedStruct(scan.getRowType());
    return NamedScan.builder()
        .initialSchema(type)
        .addAllNames(scan.getTable().getQualifiedName())
        .build();
  }

  @Override
  public Rel visit(TableFunctionScan scan) {
    return super.visit(scan);
  }

  @Override
  public Rel visit(LogicalValues values) {
    var type = TypeConverter.toNamedStruct(values.getRowType());
    if (values.getTuples().isEmpty()) {
      return EmptyScan.builder().initialSchema(type).build();
    }

    List<Expression.StructLiteral> structs =
        values.getTuples().stream()
            .map(
                list -> {
                  var fields =
                      list.stream()
                          .map(l -> LiteralConverter.convert(l))
                          .collect(Collectors.toUnmodifiableList());
                  return ExpressionCreator.struct(false, fields);
                })
            .collect(Collectors.toUnmodifiableList());
    return VirtualTableScan.builder().addAllDfsNames(type.names()).addAllRows(structs).build();
  }

  @Override
  public Rel visit(LogicalFilter filter) {
    var condition = toExpression(filter.getCondition());
    return Filter.builder().condition(condition).input(apply(filter.getInput())).build();
  }

  @Override
  public Rel visit(LogicalCalc calc) {
    return super.visit(calc);
  }

  @Override
  public Rel visit(LogicalProject project) {
    var input = apply(project.getInput());
    this.converter.setInputRel(project.getInput());
    this.converter.setInputType(input.getRecordType());
    var expressions =
        project.getProjects().stream()
            .map(this::toExpression)
            .collect(java.util.stream.Collectors.toList());
    this.converter.setInputRel(null);
    this.converter.setInputType(null);

    // todo: eliminate excessive projects. This should be done by converting rexinputrefs to remaps.
    return Project.builder()
        .remap(
            Rel.Remap.offset(project.getInput().getRowType().getFieldCount(), expressions.size()))
        .expressions(expressions)
        .input(apply(project.getInput()))
        .build();
  }

  @Override
  public Rel visit(LogicalJoin join) {
    var left = apply(join.getLeft());
    var right = apply(join.getRight());
    var condition = toExpression(join.getCondition());
    var joinType =
        switch (join.getJoinType()) {
          case INNER -> Join.JoinType.INNER;
          case LEFT -> Join.JoinType.LEFT;
          case RIGHT -> Join.JoinType.RIGHT;
          case FULL -> Join.JoinType.OUTER;
          case SEMI -> Join.JoinType.SEMI;
          case ANTI -> Join.JoinType.ANTI;
        };

    if (joinType == Join.JoinType.INNER
        && TRUE.equals(condition)
        && options.getCrossJoinPolicy() == KEEP_AS_CROSS_JOIN) {
      return Cross.builder()
          .left(left)
          .right(right)
          .deriveRecordType(
              Type.Struct.builder().from(left.getRecordType()).from(right.getRecordType()).build())
          .build();
    }
    return Join.builder().condition(condition).joinType(joinType).left(left).right(right).build();
  }

  @Override
  public Rel visit(LogicalCorrelate correlate) {
    return super.visit(correlate);
  }

  @Override
  public Rel visit(LogicalUnion union) {
    var inputs = apply(union.getInputs());
    var setOp = union.all ? Set.SetOp.UNION_ALL : Set.SetOp.UNION_DISTINCT;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(LogicalIntersect intersect) {
    var inputs = apply(intersect.getInputs());
    var setOp = intersect.all ? Set.SetOp.INTERSECTION_MULTISET : Set.SetOp.INTERSECTION_PRIMARY;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(LogicalMinus minus) {
    var inputs = apply(minus.getInputs());
    var setOp = minus.all ? Set.SetOp.MINUS_MULTISET : Set.SetOp.MINUS_PRIMARY;
    return Set.builder().inputs(inputs).setOp(setOp).build();
  }

  @Override
  public Rel visit(LogicalAggregate aggregate) {
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
        aggregateFunctionConverter.convert(input, inputType, call, t -> t.accept(converter));
    if (invocation.isEmpty()) {
      throw new UnsupportedOperationException("Unable to find binding for call " + call);
    }
    var builder = Aggregate.Measure.builder().function(invocation.get());
    if (call.filterArg != -1) {
      builder.preMeasureFilter(FieldReference.newRootStructReference(call.filterArg, inputType));
    }
    return builder.build();
  }

  @Override
  public Rel visit(LogicalMatch match) {
    return super.visit(match);
  }

  @Override
  public Rel visit(LogicalSort sort) {
    var input = apply(sort.getInput());
    var fields =
        sort.getCollation().getFieldCollations().stream()
            .map(t -> toSortField(t, input.getRecordType()))
            .collect(java.util.stream.Collectors.toList());
    var convertedSort = Sort.builder().addAllSortFields(fields).input(input).build();
    if (sort.fetch == null && sort.offset == null) {
      return convertedSort;
    }
    var offset = Optional.ofNullable(sort.offset).map(r -> asLong(r)).orElse(0L);
    var builder = Fetch.builder().input(convertedSort).offset(offset);
    if (sort.fetch == null) {
      return builder.build();
    }

    return builder.count(asLong(sort.fetch)).build();
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
  public Rel visit(LogicalExchange exchange) {
    return super.visit(exchange);
  }

  @Override
  public Rel visit(LogicalTableModify modify) {
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

  public static Rel convert(RelRoot root, SimpleExtension.ExtensionCollection extensions) {
    return convert(root.rel, extensions, OPTIONS);
  }

  public static Rel convert(
      RelRoot root, SimpleExtension.ExtensionCollection extensions, Options options) {
    return convert(root.rel, extensions, options);
  }

  private static Rel convert(
      RelNode rel, SimpleExtension.ExtensionCollection extensions, Options options) {
    SubstraitRelVisitor visitor =
        new SubstraitRelVisitor(rel.getCluster().getTypeFactory(), extensions, options);
    visitor.popFieldAccessDepthMap(rel);
    return visitor.apply(rel);
  }

  public enum CrossJoinPolicy {
    KEEP_AS_CROSS_JOIN,
    CONVERT_TO_INNER_JOIN
  };

  public static class Options {
    private final CrossJoinPolicy crossJoinPolicy;

    public Options() {
      this(CrossJoinPolicy.CONVERT_TO_INNER_JOIN);
    }

    public Options(CrossJoinPolicy crossJoinPolicy) {
      this.crossJoinPolicy = crossJoinPolicy;
    }

    /**
     * Returns the {@code crossJoinAsInnerJoin} option. Controls whether to cross joins are
     * represented as inner joins with a true condition.
     */
    public CrossJoinPolicy getCrossJoinPolicy() {
      return crossJoinPolicy;
    }
  }
}
