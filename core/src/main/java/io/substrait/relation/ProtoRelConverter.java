package io.substrait.relation;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.io.substrait.extension.AdvancedExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.CrossRel;
import io.substrait.proto.ExtensionLeafRel;
import io.substrait.proto.ExtensionMultiRel;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortRel;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.relation.extensions.EmptyOptimization;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.files.ImmutableFileFormat;
import io.substrait.relation.files.ImmutableFileOrFiles;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Converts from {@link io.substrait.proto.Rel} to {@link io.substrait.relation.Rel} */
public class ProtoRelConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoRelConverter.class);

  protected final ExtensionLookup lookup;
  protected final SimpleExtension.ExtensionCollection extensions;
  private final ProtoTypeConverter protoTypeConverter;

  public ProtoRelConverter(ExtensionLookup lookup) throws IOException {
    this(lookup, SimpleExtension.loadDefaults());
  }

  public ProtoRelConverter(ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
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
      case EXTENSION_LEAF -> {
        return newExtensionLeaf(rel.getExtensionLeaf());
      }
      case EXTENSION_SINGLE -> {
        return newExtensionSingle(rel.getExtensionSingle());
      }
      case EXTENSION_MULTI -> {
        return newExtensionMulti(rel.getExtensionMulti());
      }
      default -> {
        throw new UnsupportedOperationException("Unsupported RelTypeCase of " + relType);
      }
    }
  }

  private Rel newRead(ReadRel rel) {
    if (rel.hasVirtualTable()) {
      return newVirtualTable(rel);
    } else if (rel.hasNamedTable()) {
      return newNamedScan(rel);
    } else if (rel.hasLocalFiles()) {
      return newLocalFiles(rel);
    } else if (rel.hasExtensionTable()) {
      return newExtensionTable(rel);
    } else {
      return newEmptyScan(rel);
    }
  }

  private Filter newFilter(FilterRel rel) {
    var input = from(rel.getInput());
    var builder =
        Filter.builder()
            .input(input)
            .condition(
                new ProtoExpressionConverter(lookup, extensions, input.getRecordType())
                    .from(rel.getCondition()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private NamedStruct newNamedStruct(ReadRel rel) {
    var namedStruct = rel.getBaseSchema();
    var struct = namedStruct.getStruct();
    return ImmutableNamedStruct.builder()
        .names(namedStruct.getNamesList())
        .struct(
            Type.Struct.builder()
                .fields(
                    struct.getTypesList().stream()
                        .map(protoTypeConverter::from)
                        .collect(java.util.stream.Collectors.toList()))
                .nullable(ProtoTypeConverter.isNullable(struct.getNullability()))
                .build())
        .build();
  }

  private EmptyScan newEmptyScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    var builder =
        EmptyScan.builder()
            .initialSchema(namedStruct)
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(lookup, extensions, namedStruct.struct())
                            .from(rel.getFilter())
                        : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private ExtensionLeaf newExtensionLeaf(ExtensionLeafRel rel) {
    Extension.LeafRelDetail detail = detailFromExtensionLeafRel(rel.getDetail());
    var builder =
        ExtensionLeaf.from(detail)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  private ExtensionSingle newExtensionSingle(ExtensionSingleRel rel) {
    Extension.SingleRelDetail detail = detailFromExtensionSingleRel(rel.getDetail());
    Rel input = from(rel.getInput());
    var builder =
        ExtensionSingle.from(detail, input)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  private ExtensionMulti newExtensionMulti(ExtensionMultiRel rel) {
    Extension.MultiRelDetail detail = detailFromExtensionMultiRel(rel.getDetail());
    List<Rel> inputs = rel.getInputsList().stream().map(this::from).collect(Collectors.toList());
    var builder =
        ExtensionMulti.from(detail, inputs)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasDetail()) {
      builder.detail(detailFromExtensionMultiRel(rel.getDetail()));
    }
    return builder.build();
  }

  private NamedScan newNamedScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    var builder =
        NamedScan.builder()
            .initialSchema(namedStruct)
            .names(rel.getNamedTable().getNamesList())
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(lookup, extensions, namedStruct.struct())
                            .from(rel.getFilter())
                        : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private ExtensionTable newExtensionTable(ReadRel rel) {
    Extension.ExtensionTableDetail detail =
        detailFromExtensionTable(rel.getExtensionTable().getDetail());
    var builder = ExtensionTable.from(detail);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private LocalFiles newLocalFiles(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);

    var builder =
        LocalFiles.builder()
            .initialSchema(namedStruct)
            .addAllItems(
                rel.getLocalFiles().getItemsList().stream()
                    .map(this::newFileOrFiles)
                    .collect(java.util.stream.Collectors.toList()))
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(lookup, extensions, namedStruct.struct())
                            .from(rel.getFilter())
                        : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private FileOrFiles newFileOrFiles(ReadRel.LocalFiles.FileOrFiles file) {
    ImmutableFileOrFiles.Builder builder =
        ImmutableFileOrFiles.builder()
            .partitionIndex(file.getPartitionIndex())
            .start(file.getStart())
            .length(file.getLength());
    if (file.hasParquet()) {
      builder.fileFormat(ImmutableFileFormat.ParquetReadOptions.builder().build());
    } else if (file.hasOrc()) {
      builder.fileFormat(ImmutableFileFormat.OrcReadOptions.builder().build());
    } else if (file.hasArrow()) {
      builder.fileFormat(ImmutableFileFormat.ArrowReadOptions.builder().build());
    } else if (file.hasDwrf()) {
      builder.fileFormat(ImmutableFileFormat.DwrfReadOptions.builder().build());
    } else if (file.hasExtension()) {
      builder.fileFormat(
          ImmutableFileFormat.Extension.builder().extension(file.getExtension()).build());
    }
    if (file.hasUriFile()) {
      builder.pathType(FileOrFiles.PathType.URI_FILE).path(file.getUriFile());
    } else if (file.hasUriFolder()) {
      builder.pathType(FileOrFiles.PathType.URI_FOLDER).path(file.getUriFolder());
    } else if (file.hasUriPath()) {
      builder.pathType(FileOrFiles.PathType.URI_PATH).path(file.getUriPath());
    } else if (file.hasUriPathGlob()) {
      builder.pathType(FileOrFiles.PathType.URI_PATH_GLOB).path(file.getUriPathGlob());
    }
    return builder.build();
  }

  private VirtualTableScan newVirtualTable(ReadRel rel) {
    var virtualTable = rel.getVirtualTable();
    var converter = new ProtoExpressionConverter(lookup, extensions, EMPTY_TYPE);
    List<Expression.StructLiteral> structLiterals = new ArrayList<>(virtualTable.getValuesCount());
    for (var struct : virtualTable.getValuesList()) {
      structLiterals.add(
          ImmutableExpression.StructLiteral.builder()
              .fields(
                  struct.getFieldsList().stream()
                      .map(converter::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
    }
    var fieldNames =
        rel.getBaseSchema().getNamesList().stream().collect(java.util.stream.Collectors.toList());
    var builder =
        VirtualTableScan.builder()
            .filter(Optional.ofNullable(rel.hasFilter() ? converter.from(rel.getFilter()) : null))
            .addAllDfsNames(fieldNames)
            .rows(structLiterals);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Fetch newFetch(FetchRel rel) {
    var input = from(rel.getInput());
    var builder = Fetch.builder().input(input).count(rel.getCount()).offset(rel.getOffset());

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Project newProject(ProjectRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    var builder =
        Project.builder()
            .input(input)
            .expressions(
                rel.getExpressionsList().stream()
                    .map(converter::from)
                    .collect(java.util.stream.Collectors.toList()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Aggregate newAggregate(AggregateRel rel) {
    var input = from(rel.getInput());
    var protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    List<Aggregate.Grouping> groupings = new ArrayList<>(rel.getGroupingsCount());
    for (var grouping : rel.getGroupingsList()) {
      groupings.add(
          Aggregate.Grouping.builder()
              .expressions(
                  grouping.getGroupingExpressionsList().stream()
                      .map(protoExprConverter::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
    }
    List<Aggregate.Measure> measures = new ArrayList<>(rel.getMeasuresCount());
    var pF = new FunctionArg.ProtoFrom(protoExprConverter, protoTypeConverter);
    for (var measure : rel.getMeasuresList()) {
      var func = measure.getMeasure();
      var funcDecl = lookup.getAggregateFunction(func.getFunctionReference(), extensions);
      var args =
          IntStream.range(0, measure.getMeasure().getArgumentsCount())
              .mapToObj(i -> pF.convert(funcDecl, i, measure.getMeasure().getArguments(i)))
              .collect(java.util.stream.Collectors.toList());
      measures.add(
          Aggregate.Measure.builder()
              .function(
                  AggregateFunctionInvocation.builder()
                      .arguments(args)
                      .declaration(funcDecl)
                      .outputType(protoTypeConverter.from(func.getOutputType()))
                      .aggregationPhase(Expression.AggregationPhase.fromProto(func.getPhase()))
                      .invocation(func.getInvocation())
                      .build())
              .preMeasureFilter(
                  Optional.ofNullable(
                      measure.hasFilter() ? protoExprConverter.from(measure.getFilter()) : null))
              .build());
    }
    var builder = Aggregate.builder().input(input).groupings(groupings).measures(measures);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Sort newSort(SortRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
    var builder =
        Sort.builder()
            .input(input)
            .sortFields(
                rel.getSortsList().stream()
                    .map(
                        field ->
                            Expression.SortField.builder()
                                .direction(Expression.SortDirection.fromProto(field.getDirection()))
                                .expr(converter.from(field.getExpr()))
                                .build())
                    .collect(java.util.stream.Collectors.toList()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Join newJoin(JoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var converter = new ProtoExpressionConverter(lookup, extensions, unionedStruct);
    var builder =
        Join.builder()
            .left(left)
            .right(right)
            .condition(converter.from(rel.getExpression()))
            .joinType(Join.JoinType.fromProto(rel.getType()))
            .postJoinFilter(
                Optional.ofNullable(
                    rel.hasPostJoinFilter() ? converter.from(rel.getPostJoinFilter()) : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Rel newCross(CrossRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    var builder = Cross.builder().left(left).right(right);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private Set newSet(SetRel rel) {
    List<Rel> inputs =
        rel.getInputsList().stream()
            .map(inputRel -> from(inputRel))
            .collect(java.util.stream.Collectors.toList());
    var builder = Set.builder().inputs(inputs).setOp(Set.SetOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  private static Optional<Rel.Remap> optionalRelmap(io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasEmit() ? Rel.Remap.of(relCommon.getEmit().getOutputMappingList()) : null);
  }

  private Optional<AdvancedExtension> optionalAdvancedExtension(
      io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasAdvancedExtension()
            ? advancedExtension(relCommon.getAdvancedExtension())
            : null);
  }

  private AdvancedExtension advancedExtension(
      io.substrait.proto.AdvancedExtension advancedExtension) {
    var builder = AdvancedExtension.builder();
    if (advancedExtension.hasEnhancement()) {
      builder.enhancement(enhancementFromAdvancedExtension(advancedExtension.getEnhancement()));
    }
    if (advancedExtension.hasOptimization()) {
      builder.optimization(optimizationFromAdvancedExtension(advancedExtension.getOptimization()));
    }
    return builder.build();
  }

  /**
   * Override to provide a custom converter for {@link
   * io.substrait.proto.AdvancedExtension#getOptimization()} data
   */
  protected Extension.Optimization optimizationFromAdvancedExtension(com.google.protobuf.Any any) {
    return new EmptyOptimization();
  }

  /**
   * Override to provide a custom converter for {@link
   * io.substrait.proto.AdvancedExtension#getEnhancement()} data
   */
  protected Extension.Enhancement enhancementFromAdvancedExtension(com.google.protobuf.Any any) {
    throw new RuntimeException("enhancements cannot be ignored by consumers");
  }

  /** Override to provide a custom converter for {@link ExtensionLeafRel#getDetail()} data */
  protected Extension.LeafRelDetail detailFromExtensionLeafRel(com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /** Override to provide a custom converter for {@link ExtensionSingleRel#getDetail()} data */
  protected Extension.SingleRelDetail detailFromExtensionSingleRel(com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /** Override to provide a custom converter for {@link ExtensionMultiRel#getDetail()} data */
  protected Extension.MultiRelDetail detailFromExtensionMultiRel(com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /**
   * Override to provide a custom converter for {@link
   * io.substrait.proto.ReadRel.ExtensionTable#getDetail()} data
   */
  protected Extension.ExtensionTableDetail detailFromExtensionTable(com.google.protobuf.Any any) {
    return emptyDetail();
  }

  private EmptyDetail emptyDetail() {
    return new EmptyDetail();
  }
}
