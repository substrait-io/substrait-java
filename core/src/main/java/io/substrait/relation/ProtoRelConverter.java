package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.plan.Plan;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.proto.CrossRel;
import io.substrait.proto.ExpandRel;
import io.substrait.proto.ExtensionLeafRel;
import io.substrait.proto.ExtensionMultiRel;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortRel;
import io.substrait.proto.WriteRel;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.relation.extensions.EmptyOptimization;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.files.ImmutableFileFormat;
import io.substrait.relation.files.ImmutableFileOrFiles;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
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

  public ProtoRelConverter(ExtensionLookup lookup) {
    this(lookup, SimpleExtension.loadDefaults());
  }

  public ProtoRelConverter(ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
  }

  public Plan.Root from(io.substrait.proto.RelRoot rel) {
    return Plan.Root.builder().input(from(rel.getInput())).addAllNames(rel.getNamesList()).build();
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
      case EXPAND -> {
        return newExpand(rel.getExpand());
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
      case HASH_JOIN -> {
        return newHashJoin(rel.getHashJoin());
      }
      case MERGE_JOIN -> {
        return newMergeJoin(rel.getMergeJoin());
      }
      case NESTED_LOOP_JOIN -> {
        return newNestedLoopJoin(rel.getNestedLoopJoin());
      }
      case WINDOW -> {
        return newConsistentPartitionWindow(rel.getWindow());
      }
      case WRITE -> {
        return newWrite(rel.getWrite());
      }
      default -> {
        throw new UnsupportedOperationException("Unsupported RelTypeCase of " + relType);
      }
    }
  }

  protected Rel newRead(ReadRel rel) {
    if (rel.hasVirtualTable()) {
      var virtualTable = rel.getVirtualTable();
      if (virtualTable.getValuesCount() == 0 && virtualTable.getExpressionsCount() == 0) {
        return newEmptyScan(rel);
      } else {
        return newVirtualTable(rel);
      }
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

  protected Rel newWrite(WriteRel rel) {
    var relType = rel.getWriteTypeCase();
    switch (relType) {
      case NAMED_TABLE -> {
        return newNamedWrite(rel);
      }
      case EXTENSION_TABLE -> {
        return newExtensionWrite(rel);
      }
      default -> throw new UnsupportedOperationException("Unsupported WriteTypeCase of " + relType);
    }
  }

  protected NamedWrite newNamedWrite(WriteRel rel) {
    var input = from(rel.getInput());
    var builder =
        NamedWrite.builder()
            .input(input)
            .names(rel.getNamedTable().getNamesList())
            .tableSchema(newNamedStruct(rel.getTableSchema()))
            .createMode(NamedWrite.CreateMode.fromProto(rel.getCreateMode()))
            .outputMode(NamedWrite.OutputMode.fromProto(rel.getOutput()))
            .operation(NamedWrite.WriteOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  protected Rel newExtensionWrite(WriteRel rel) {
    var input = from(rel.getInput());
    var detail = detailFromWriteExtensionObject(rel.getExtensionTable().getDetail());
    var builder =
        ExtensionWrite.builder()
            .input(input)
            .detail(detail)
            .tableSchema(newNamedStruct(rel.getTableSchema()))
            .createMode(NamedWrite.CreateMode.fromProto(rel.getCreateMode()))
            .outputMode(NamedWrite.OutputMode.fromProto(rel.getOutput()))
            .operation(NamedWrite.WriteOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  private Filter newFilter(FilterRel rel) {
    var input = from(rel.getInput());
    var builder =
        Filter.builder()
            .input(input)
            .condition(
                new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this)
                    .from(rel.getCondition()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected NamedStruct newNamedStruct(ReadRel rel) {
    return newNamedStruct(rel.getBaseSchema());
  }

  protected NamedStruct newNamedStruct(io.substrait.proto.NamedStruct namedStruct) {
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

  protected EmptyScan newEmptyScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    var builder =
        EmptyScan.builder()
            .initialSchema(namedStruct)
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
                            .from(rel.getBestEffortFilter())
                        : null))
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
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

  protected ExtensionLeaf newExtensionLeaf(ExtensionLeafRel rel) {
    Extension.LeafRelDetail detail = detailFromExtensionLeafRel(rel.getDetail());
    var builder =
        ExtensionLeaf.from(detail)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionSingle newExtensionSingle(ExtensionSingleRel rel) {
    Extension.SingleRelDetail detail = detailFromExtensionSingleRel(rel.getDetail());
    Rel input = from(rel.getInput());
    var builder =
        ExtensionSingle.from(detail, input)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionMulti newExtensionMulti(ExtensionMultiRel rel) {
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

  protected NamedScan newNamedScan(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);
    var builder =
        NamedScan.builder()
            .initialSchema(namedStruct)
            .names(rel.getNamedTable().getNamesList())
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
                            .from(rel.getBestEffortFilter())
                        : null))
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
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

  protected ExtensionTable newExtensionTable(ReadRel rel) {
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

  protected LocalFiles newLocalFiles(ReadRel rel) {
    var namedStruct = newNamedStruct(rel);

    var builder =
        LocalFiles.builder()
            .initialSchema(namedStruct)
            .addAllItems(
                rel.getLocalFiles().getItemsList().stream()
                    .map(this::newFileOrFiles)
                    .collect(java.util.stream.Collectors.toList()))
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
                            .from(rel.getBestEffortFilter())
                        : null))
            .filter(
                Optional.ofNullable(
                    rel.hasFilter()
                        ? new ProtoExpressionConverter(
                                lookup, extensions, namedStruct.struct(), this)
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

  protected FileOrFiles newFileOrFiles(ReadRel.LocalFiles.FileOrFiles file) {
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
    } else if (file.hasText()) {
      var ffBuilder =
          ImmutableFileFormat.DelimiterSeparatedTextReadOptions.builder()
              .fieldDelimiter(file.getText().getFieldDelimiter())
              .maxLineSize(file.getText().getMaxLineSize())
              .quote(file.getText().getQuote())
              .headerLinesToSkip(file.getText().getHeaderLinesToSkip())
              .escape(file.getText().getEscape());
      if (file.getText().hasValueTreatedAsNull()) {
        ffBuilder.valueTreatedAsNull(file.getText().getValueTreatedAsNull());
      }
      builder.fileFormat(ffBuilder.build());
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

  protected VirtualTableScan newVirtualTable(ReadRel rel) {
    var virtualTable = rel.getVirtualTable();
    var virtualTableSchema = newNamedStruct(rel);

    var converter =
        new ProtoExpressionConverter(lookup, extensions, virtualTableSchema.struct(), this);

    List<Expression.StructLiteral> structLiterals = new ArrayList<>(virtualTable.getValuesCount() + virtualTable.getExpressionCount());
    for (var struct : virtualTable.getValuesList()) {
      structLiterals.add(
          ImmutableExpression.StructLiteral.builder()
              .fields(
                  struct.getFieldsList().stream()
                      .map(converter::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
    }
    for (var struct : virtualTable.getExpressionsList()) {
      structLiterals.add(
          ImmutableExpression.StructLiteral.builder()
              .fields(
                  struct.getFieldsList().stream()
                      .map(io.substrait.proto.Expression::getLiteral)
                      .map(converter::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
    }

    var builder =
        VirtualTableScan.builder()
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter() ? converter.from(rel.getBestEffortFilter()) : null))
            .filter(Optional.ofNullable(rel.hasFilter() ? converter.from(rel.getFilter()) : null))
            .initialSchema(NamedStruct.fromProto(rel.getBaseSchema(), protoTypeConverter))
            .rows(structLiterals);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Fetch newFetch(FetchRel rel) {
    var input = from(rel.getInput());
    var builder = Fetch.builder().input(input).offset(rel.getOffset());
    if (rel.getCount() != -1) {
      // -1 is used as a sentinel value to signal LIMIT ALL
      // count only needs to be set when it is not -1
      builder.count(rel.getCount());
    }

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Project newProject(ProjectRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    var builder =
        Project.builder()
            .input(input)
            .expressions(
                rel.getExpressionsList().stream()
                    .map(converter::from)
                    .collect(java.util.stream.Collectors.toList()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Expand newExpand(ExpandRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    var builder =
        Expand.builder()
            .input(input)
            .fields(
                rel.getFieldsList().stream()
                    .map(
                        expandField ->
                            switch (expandField.getFieldTypeCase()) {
                              case CONSISTENT_FIELD -> Expand.ConsistentField.builder()
                                  .expression(converter.from(expandField.getConsistentField()))
                                  .build();
                              case SWITCHING_FIELD -> Expand.SwitchingField.builder()
                                  .duplicates(
                                      expandField.getSwitchingField().getDuplicatesList().stream()
                                          .map(converter::from)
                                          .collect(java.util.stream.Collectors.toList()))
                                  .build();
                              case FIELDTYPE_NOT_SET -> throw new UnsupportedOperationException(
                                  "Expand fields not set");
                            })
                    .collect(java.util.stream.Collectors.toList()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected Aggregate newAggregate(AggregateRel rel) {
    var input = from(rel.getInput());
    var protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    var protoAggrFuncConverter =
        new ProtoAggregateFunctionConverter(lookup, extensions, protoExprConverter);

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
              .function(protoAggrFuncConverter.from(measure.getMeasure()))
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

  protected Sort newSort(SortRel rel) {
    var input = from(rel.getInput());
    var converter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
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

  protected Join newJoin(JoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var converter = new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
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

  protected Rel newCross(CrossRel rel) {
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

  protected Set newSet(SetRel rel) {
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

  protected Rel newHashJoin(HashJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    var leftKeys = rel.getLeftKeysList();
    var rightKeys = rel.getRightKeysList();

    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var leftConverter = new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    var rightConverter = new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    var unionConverter = new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    var builder =
        HashJoin.builder()
            .left(left)
            .right(right)
            .leftKeys(leftKeys.stream().map(leftConverter::from).collect(Collectors.toList()))
            .rightKeys(rightKeys.stream().map(rightConverter::from).collect(Collectors.toList()))
            .joinType(HashJoin.JoinType.fromProto(rel.getType()))
            .postJoinFilter(
                Optional.ofNullable(
                    rel.hasPostJoinFilter() ? unionConverter.from(rel.getPostJoinFilter()) : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newMergeJoin(MergeJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    var leftKeys = rel.getLeftKeysList();
    var rightKeys = rel.getRightKeysList();

    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var leftConverter = new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    var rightConverter = new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    var unionConverter = new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    var builder =
        MergeJoin.builder()
            .left(left)
            .right(right)
            .leftKeys(leftKeys.stream().map(leftConverter::from).collect(Collectors.toList()))
            .rightKeys(rightKeys.stream().map(rightConverter::from).collect(Collectors.toList()))
            .joinType(MergeJoin.JoinType.fromProto(rel.getType()))
            .postJoinFilter(
                Optional.ofNullable(
                    rel.hasPostJoinFilter() ? unionConverter.from(rel.getPostJoinFilter()) : null));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected NestedLoopJoin newNestedLoopJoin(NestedLoopJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    var converter = new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    var builder =
        NestedLoopJoin.builder()
            .left(left)
            .right(right)
            .condition(
                // defaults to true (aka cartesian join) if the join expression is missing
                rel.hasExpression()
                    ? converter.from(rel.getExpression())
                    : Expression.BoolLiteral.builder().value(true).build())
            .joinType(NestedLoopJoin.JoinType.fromProto(rel.getType()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected ConsistentPartitionWindow newConsistentPartitionWindow(
      ConsistentPartitionWindowRel rel) {

    var input = from(rel.getInput());
    var protoExpressionConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    var partitionExprs =
        rel.getPartitionExpressionsList().stream()
            .map(protoExpressionConverter::from)
            .collect(Collectors.toList());
    var sortFields =
        rel.getSortsList().stream()
            .map(protoExpressionConverter::fromSortField)
            .collect(Collectors.toList());
    var windowRelFunctions =
        rel.getWindowFunctionsList().stream()
            .map(protoExpressionConverter::fromWindowRelFunction)
            .collect(Collectors.toList());

    var builder =
        ConsistentPartitionWindow.builder()
            .input(input)
            .partitionExpressions(partitionExprs)
            .sorts(sortFields)
            .windowFunctions(windowRelFunctions);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(advancedExtension(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected static Optional<Rel.Remap> optionalRelmap(io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasEmit() ? Rel.Remap.of(relCommon.getEmit().getOutputMappingList()) : null);
  }

  protected static Optional<Hint> optionalHint(io.substrait.proto.RelCommon relCommon) {
    if (!relCommon.hasHint()) return Optional.empty();
    var hint = relCommon.getHint();
    var builder = Hint.builder().addAllOutputNames(hint.getOutputNamesList());
    if (!hint.getAlias().isEmpty()) {
      builder.alias(hint.getAlias());
    }
    return Optional.of(builder.build());
  }

  protected Optional<AdvancedExtension> optionalAdvancedExtension(
      io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasAdvancedExtension()
            ? advancedExtension(relCommon.getAdvancedExtension())
            : null);
  }

  protected AdvancedExtension advancedExtension(
      io.substrait.proto.AdvancedExtension advancedExtension) {
    var builder = AdvancedExtension.builder();
    if (advancedExtension.hasEnhancement()) {
      builder.enhancement(enhancementFromAdvancedExtension(advancedExtension.getEnhancement()));
    }
    advancedExtension
        .getOptimizationList()
        .forEach(
            optimization ->
                builder.addOptimizations(optimizationFromAdvancedExtension(optimization)));

    return builder.build();
  }

  /**
   * Override to provide a custom converter for {@link
   * io.substrait.proto.AdvancedExtension#getOptimizationList()} ()} data
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

  protected Extension.WriteExtensionObject detailFromWriteExtensionObject(
      com.google.protobuf.Any any) {
    return emptyDetail();
  }

  private EmptyDetail emptyDetail() {
    return new EmptyDetail();
  }
}
