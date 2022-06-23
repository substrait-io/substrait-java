package io.substrait.relation;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionLookup;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.CrossRel;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortRel;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.FromProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/** Converts from proto to pojo rel representation TODO: AdvancedExtension */
public class ProtoRelConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoRelConverter.class);

  private final FunctionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;

  public ProtoRelConverter(FunctionLookup lookup) throws IOException {
    this(lookup, SimpleExtension.loadDefaults());
  }

  public ProtoRelConverter(FunctionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    this.lookup = lookup;
    this.extensions = extensions;
  }

  public Rel from(io.substrait.proto.Rel rel) {
    var relType = rel.getRelTypeCase();
    switch (relType) {
      case READ -> {
        return newRead(rel.getRead());
      }
      case FILTER -> {
        return newFilter(rel.getFilter());
      }
      case FETCH -> {
        return newFetch(rel.getFetch());
      }
      case AGGREGATE -> {
        return newAggregate(rel.getAggregate());
      }
      case SORT -> {
        return newSort(rel.getSort());
      }
      case JOIN -> {
        return newJoin(rel.getJoin());
      }
      case SET -> {
        return newSet(rel.getSet());
      }
      case PROJECT -> {
        return newProject(rel.getProject());
      }
      case CROSS -> {
        return newCross(rel.getCross());
      }
      default -> {
        // TODO: add support for EXTENSION_SINGLE, EXTENSION_MULTI, EXTENSION_LEAF
        throw new UnsupportedOperationException("Unsupported RelTypeCase of " + relType);
      }
    }
  }

  private Rel newRead(ReadRel rel) {
    if (rel.hasVirtualTable()) {
      return newVirtualTable(rel);
    } else if (rel.hasNamedTable()) {
      return newNamedScan(rel);
    } else {
      return newEmptyScan(rel);
    }
  }

  private Filter newFilter(FilterRel rel) {
    var input = from(rel.getInput());
    return Filter.builder()
        .input(input)
        .condition(
            new ProtoExpressionConverter(lookup, extensions, input.getRecordType())
                .from(rel.getCondition()))
        .remap(optionalRelmap(rel.getCommon()))
        .build();
  }

  private NamedStruct newNamedStruct(ReadRel rel) {
    var namedStruct = rel.getBaseSchema();
    var struct = namedStruct.getStruct();
    return ImmutableNamedStruct.builder()
        .names(namedStruct.getNamesList())
        .struct(
            Type.Struct.builder()
                .fields(struct.getTypesList().stream().map(FromProto::from).toList())
                .nullable(FromProto.isNullable(struct.getNullability()))
                .build())
        .build();
  }

  private EmptyScan newEmptyScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    return EmptyScan.builder()
        .initialSchema(namedStruct)
        .remap(optionalRelmap(rel.getCommon()))
        .filter(
            Optional.ofNullable(
                rel.hasFilter()
                    ? new ProtoExpressionConverter(lookup, extensions, namedStruct.struct())
                        .from(rel.getFilter())
                    : null))
        .build();
  }

  private NamedScan newNamedScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    return NamedScan.builder()
        .initialSchema(namedStruct)
        .names(rel.getNamedTable().getNamesList())
        .remap(optionalRelmap(rel.getCommon()))
        .filter(
            Optional.ofNullable(
                rel.hasFilter()
                    ? new ProtoExpressionConverter(lookup, extensions, namedStruct.struct())
                        .from(rel.getFilter())
                    : null))
        .build();
  }

  private VirtualTableScan newVirtualTable(ReadRel rel) {
    var virtualTable = rel.getVirtualTable();
    List<Expression.StructLiteral> structLiterals = new ArrayList<>(virtualTable.getValuesCount());
    for (var struct : virtualTable.getValuesList()) {
      structLiterals.add(
          ImmutableExpression.StructLiteral.builder()
              .fields(struct.getFieldsList().stream().map(ProtoExpressionConverter::from).toList())
              .build());
    }
    var converter = new ProtoExpressionConverter(lookup, extensions, EMPTY_TYPE);
    return VirtualTableScan.builder()
        .filter(Optional.ofNullable(rel.hasFilter() ? converter.from(rel.getFilter()) : null))
        .remap(optionalRelmap(rel.getCommon()))
        .rows(structLiterals)
        .build();
  }

  private Fetch newFetch(FetchRel rel) {
    var input = from(rel.getInput());
    return Fetch.builder()
        .input(input)
        .remap(optionalRelmap(rel.getCommon()))
        .count(rel.getCount())
        .offset(rel.getOffset())
        .build();
  }

  private Project newProject(ProjectRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    return Project.builder()
        .input(input)
        .remap(optionalRelmap(rel.getCommon()))
        .expressions(rel.getExpressionsList().stream().map(converter::from).toList())
        .build();
  }

  private Aggregate newAggregate(AggregateRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    List<Aggregate.Grouping> groupings = new ArrayList<>(rel.getGroupingsCount());
    for (var grouping : rel.getGroupingsList()) {
      groupings.add(
          Aggregate.Grouping.builder()
              .expressions(
                  grouping.getGroupingExpressionsList().stream().map(converter::from).toList())
              .build());
    }
    List<Aggregate.Measure> measures = new ArrayList<>(rel.getMeasuresCount());
    var pF = new FunctionArg.ProtoFrom(converter);
    for (var measure : rel.getMeasuresList()) {
      var func = measure.getMeasure();
      var funcDecl = lookup.getAggregateFunction(func.getFunctionReference(), extensions);
      var args =
          IntStream.range(0, measure.getMeasure().getArgumentsCount())
              .mapToObj(i -> pF.convert(funcDecl, i, measure.getMeasure().getArguments(i)))
              .toList();
      measures.add(
          Aggregate.Measure.builder()
              .function(
                  AggregateFunctionInvocation.builder()
                      .arguments(args)
                      .declaration(funcDecl)
                      .outputType(FromProto.from(func.getOutputType()))
                      .aggregationPhase(Expression.AggregationPhase.fromProto(func.getPhase()))
                      .invocation(func.getInvocation())
                      .build())
              .preMeasureFilter(
                  Optional.ofNullable(
                      measure.hasFilter() ? converter.from(measure.getFilter()) : null))
              .build());
    }
    return Aggregate.builder()
        .input(input)
        .groupings(groupings)
        .measures(measures)
        .remap(optionalRelmap(rel.getCommon()))
        .build();
  }

  private Sort newSort(SortRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    return Sort.builder()
        .input(input)
        .remap(optionalRelmap(rel.getCommon()))
        .sortFields(
            rel.getSortsList().stream()
                .map(
                    field ->
                        Expression.SortField.builder()
                            .direction(Expression.SortDirection.fromProto(field.getDirection()))
                            .expr(converter.from(field.getExpr()))
                            .build())
                .toList())
        .build();
  }

  private Join newJoin(JoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var converter = new ProtoExpressionConverter(lookup, extensions, unionedStruct);
    return Join.builder()
        .condition(converter.from(rel.getExpression()))
        .joinType(Join.JoinType.fromProto(rel.getType()))
        .left(left)
        .right(right)
        .remap(optionalRelmap(rel.getCommon()))
        .postJoinFilter(
            Optional.ofNullable(
                rel.hasPostJoinFilter() ? converter.from(rel.getPostJoinFilter()) : null))
        .build();
  }

  private Rel newCross(CrossRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    return Cross.builder()
        .left(left)
        .right(right)
        .deriveRecordType(unionedStruct)
        .remap(optionalRelmap(rel.getCommon()))
        .build();
  }

  private Set newSet(SetRel rel) {
    List<Rel> inputs = rel.getInputsList().stream().map(inputRel -> from(inputRel)).toList();
    return Set.builder()
        .inputs(inputs)
        .setOp(Set.SetOp.fromProto(rel.getOp()))
        .remap(optionalRelmap(rel.getCommon()))
        .build();
  }

  private static Optional<Rel.Remap> optionalRelmap(io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasEmit() ? Rel.Remap.of(relCommon.getEmit().getOutputMappingList()) : null);
  }
}
