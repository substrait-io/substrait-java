package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ProtoExtensionConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
import io.substrait.hint.Hint;
import io.substrait.hint.Hint.ComputationType;
import io.substrait.hint.Hint.LoadedComputation;
import io.substrait.hint.Hint.RuntimeConstraint;
import io.substrait.hint.Hint.SavedComputation;
import io.substrait.hint.Hint.Stats;
import io.substrait.plan.Plan;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.proto.CrossRel;
import io.substrait.proto.DdlRel;
import io.substrait.proto.ExchangeRel;
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
import io.substrait.proto.UpdateRel;
import io.substrait.proto.WriteRel;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.relation.files.FileFormat;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.physical.AbstractExchangeRel;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.ImmutableBroadcastExchange;
import io.substrait.relation.physical.ImmutableExchangeTarget;
import io.substrait.relation.physical.ImmutableMultiBucketExchange;
import io.substrait.relation.physical.ImmutableRoundRobinExchange;
import io.substrait.relation.physical.ImmutableScatterExchange;
import io.substrait.relation.physical.ImmutableSingleBucketExchange;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.relation.physical.TargetType;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jspecify.annotations.NonNull;

/** Converts from {@link io.substrait.proto.Rel} to {@link io.substrait.relation.Rel} */
public class ProtoRelConverter {

  @NonNull protected final ExtensionLookup lookup;
  @NonNull protected final ExtensionCollection extensions;
  @NonNull protected final ProtoTypeConverter protoTypeConverter;
  @NonNull protected final ProtoExtensionConverter protoExtensionConverter;

  /**
   * Constructor with custom {@link ExtensionLookup}.
   *
   * @param lookup the custom {@link ExtensionLookup} to use, must not be null
   */
  public ProtoRelConverter(@NonNull final ExtensionLookup lookup) {
    this(lookup, DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Constructor with custom {@link ExtensionLookup} and {@link ExtensionCollection}.
   *
   * @param lookup custom {@link ExtensionLookup} to use, must not be null
   * @param extensions custom {@link ExtensionCollection} to use, must not be null
   */
  public ProtoRelConverter(
      @NonNull final ExtensionLookup lookup, @NonNull final ExtensionCollection extensions) {
    this(lookup, extensions, new ProtoExtensionConverter());
  }

  /**
   * Constructor with custom {@link ExtensionLookup} and {@link ProtoExtensionConverter}.
   *
   * @param lookup custom {@link ExtensionLookup} to use, must not be null
   * @param protoExtensionConverter custom {@link ProtoExtensionConverter} to use, must not be null
   */
  public ProtoRelConverter(
      @NonNull final ExtensionLookup lookup,
      @NonNull final ProtoExtensionConverter protoExtensionConverter) {
    this(lookup, DefaultExtensionCatalog.DEFAULT_COLLECTION, protoExtensionConverter);
  }

  /**
   * Constructor with custom {@link ExtensionLookup}, {@link ExtensionCollection} and {@link
   * ProtoExtensionConverter}.
   *
   * @param lookup custom {@link ExtensionLookup} to use, must not be null
   * @param extensions custom {@link ExtensionCollection} to use, must not be null
   * @param protoExtensionConverter custom {@link ProtoExtensionConverter} to use, must not be null
   */
  public ProtoRelConverter(
      @NonNull final ExtensionLookup lookup,
      @NonNull final ExtensionCollection extensions,
      @NonNull final ProtoExtensionConverter protoExtensionConverter) {
    if (lookup == null) {
      throw new IllegalArgumentException("ExtensionLookup is required");
    }
    if (extensions == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    if (protoExtensionConverter == null) {
      throw new IllegalArgumentException("ProtoExtensionConverter is required");
    }
    this.lookup = lookup;
    this.extensions = extensions;
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
    this.protoExtensionConverter = protoExtensionConverter;
  }

  public Plan.Root from(final io.substrait.proto.RelRoot rel) {
    return Plan.Root.builder().input(from(rel.getInput())).addAllNames(rel.getNamesList()).build();
  }

  public Rel from(final io.substrait.proto.Rel rel) {
    final io.substrait.proto.Rel.RelTypeCase relType = rel.getRelTypeCase();
    switch (relType) {
      case READ:
        return newRead(rel.getRead());
      case FILTER:
        return newFilter(rel.getFilter());
      case FETCH:
        return newFetch(rel.getFetch());
      case AGGREGATE:
        return newAggregate(rel.getAggregate());
      case SORT:
        return newSort(rel.getSort());
      case JOIN:
        return newJoin(rel.getJoin());
      case SET:
        return newSet(rel.getSet());
      case PROJECT:
        return newProject(rel.getProject());
      case EXPAND:
        return newExpand(rel.getExpand());
      case CROSS:
        return newCross(rel.getCross());
      case EXTENSION_LEAF:
        return newExtensionLeaf(rel.getExtensionLeaf());
      case EXTENSION_SINGLE:
        return newExtensionSingle(rel.getExtensionSingle());
      case EXTENSION_MULTI:
        return newExtensionMulti(rel.getExtensionMulti());
      case HASH_JOIN:
        return newHashJoin(rel.getHashJoin());
      case MERGE_JOIN:
        return newMergeJoin(rel.getMergeJoin());
      case NESTED_LOOP_JOIN:
        return newNestedLoopJoin(rel.getNestedLoopJoin());
      case WINDOW:
        return newConsistentPartitionWindow(rel.getWindow());
      case WRITE:
        return newWrite(rel.getWrite());
      case DDL:
        return newDdl(rel.getDdl());
      case UPDATE:
        return newUpdate(rel.getUpdate());
      case EXCHANGE:
        return newExchange(rel.getExchange());
      default:
        throw new UnsupportedOperationException("Unsupported RelTypeCase of " + relType);
    }
  }

  protected Rel newRead(final ReadRel rel) {
    if (rel.hasVirtualTable()) {
      final ReadRel.VirtualTable virtualTable = rel.getVirtualTable();
      if (virtualTable.getValuesCount() == 0) {
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

  protected Rel newWrite(final WriteRel rel) {
    final WriteRel.WriteTypeCase relType = rel.getWriteTypeCase();
    switch (relType) {
      case NAMED_TABLE:
        return newNamedWrite(rel);
      case EXTENSION_TABLE:
        return newExtensionWrite(rel);
      default:
        throw new UnsupportedOperationException("Unsupported WriteTypeCase of " + relType);
    }
  }

  protected NamedWrite newNamedWrite(final WriteRel rel) {
    final Rel input = from(rel.getInput());
    final ImmutableNamedWrite.Builder builder =
        NamedWrite.builder()
            .input(input)
            .names(rel.getNamedTable().getNamesList())
            .tableSchema(newNamedStruct(rel.getTableSchema()))
            .createMode(NamedWrite.CreateMode.fromProto(rel.getCreateMode()))
            .outputMode(NamedWrite.OutputMode.fromProto(rel.getOutput()))
            .operation(NamedWrite.WriteOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));

    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newExtensionWrite(final WriteRel rel) {
    final Rel input = from(rel.getInput());
    final Extension.WriteExtensionObject detail =
        detailFromWriteExtensionObject(rel.getExtensionTable().getDetail());
    final ImmutableExtensionWrite.Builder builder =
        ExtensionWrite.builder()
            .input(input)
            .detail(detail)
            .tableSchema(newNamedStruct(rel.getTableSchema()))
            .createMode(NamedWrite.CreateMode.fromProto(rel.getCreateMode()))
            .outputMode(NamedWrite.OutputMode.fromProto(rel.getOutput()))
            .operation(NamedWrite.WriteOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));

    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newDdl(final DdlRel rel) {
    final DdlRel.WriteTypeCase relType = rel.getWriteTypeCase();
    switch (relType) {
      case NAMED_OBJECT:
        return newNamedDdl(rel);
      case EXTENSION_OBJECT:
        return newExtensionDdl(rel);
      default:
        throw new UnsupportedOperationException("Unsupported WriteTypeCase of " + relType);
    }
  }

  protected NamedDdl newNamedDdl(final DdlRel rel) {
    final NamedStruct tableSchema = newNamedStruct(rel.getTableSchema());
    final ImmutableNamedDdl.Builder builder =
        NamedDdl.builder()
            .names(rel.getNamedObject().getNamesList())
            .tableSchema(tableSchema)
            .tableDefaults(tableDefaults(rel.getTableDefaults(), tableSchema))
            .operation(NamedDdl.DdlOp.fromProto(rel.getOp()))
            .object(NamedDdl.DdlObject.fromProto(rel.getObject()))
            .viewDefinition(optionalViewDefinition(rel))
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));

    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }

    return builder.build();
  }

  protected ExtensionDdl newExtensionDdl(final DdlRel rel) {
    final Extension.DdlExtensionObject detail =
        detailFromDdlExtensionObject(rel.getExtensionObject().getDetail());
    final NamedStruct tableSchema = newNamedStruct(rel.getTableSchema());
    final ImmutableExtensionDdl.Builder builder =
        ExtensionDdl.builder()
            .detail(detail)
            .tableSchema(newNamedStruct(rel.getTableSchema()))
            .tableDefaults(tableDefaults(rel.getTableDefaults(), tableSchema))
            .operation(ExtensionDdl.DdlOp.fromProto(rel.getOp()))
            .object(ExtensionDdl.DdlObject.fromProto(rel.getObject()))
            .viewDefinition(optionalViewDefinition(rel))
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));

    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }

    return builder.build();
  }

  protected Optional<Rel> optionalViewDefinition(final DdlRel rel) {
    return Optional.ofNullable(rel.hasViewDefinition() ? from(rel.getViewDefinition()) : null);
  }

  protected Expression.StructLiteral tableDefaults(
      final io.substrait.proto.Expression.Literal.Struct struct, final NamedStruct tableSchema) {
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, tableSchema.struct(), this);
    return Expression.StructLiteral.builder()
        .fields(
            struct.getFieldsList().stream()
                .map(converter::from)
                .collect(java.util.stream.Collectors.toList()))
        .build();
  }

  protected Rel newUpdate(final UpdateRel rel) {
    final UpdateRel.UpdateTypeCase relType = rel.getUpdateTypeCase();
    switch (relType) {
      case NAMED_TABLE:
        return newNamedUpdate(rel);
      default:
        throw new UnsupportedOperationException("Unsupported UpdateTypeCase of " + relType);
    }
  }

  protected Rel newNamedUpdate(final UpdateRel rel) {
    final NamedStruct tableSchema = newNamedStruct(rel.getTableSchema());
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, tableSchema.struct(), this);
    final List<NamedUpdate.TransformExpression> transformations =
        new ArrayList<>(rel.getTransformationsCount());
    for (final UpdateRel.TransformExpression transformation : rel.getTransformationsList()) {
      transformations.add(
          NamedUpdate.TransformExpression.builder()
              .transformation(converter.from(transformation.getTransformation()))
              .columnTarget(transformation.getColumnTarget())
              .build());
    }
    final ImmutableNamedUpdate.Builder builder =
        NamedUpdate.builder()
            .names(rel.getNamedTable().getNamesList())
            .tableSchema(tableSchema)
            .addAllTransformations(transformations)
            .condition(converter.from(rel.getCondition()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Filter newFilter(final FilterRel rel) {
    final Rel input = from(rel.getInput());
    final ImmutableFilter.Builder builder =
        Filter.builder()
            .input(input)
            .condition(
                new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this)
                    .from(rel.getCondition()));
    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected NamedStruct newNamedStruct(final ReadRel rel) {
    return newNamedStruct(rel.getBaseSchema());
  }

  protected NamedStruct newNamedStruct(final io.substrait.proto.NamedStruct namedStruct) {
    final io.substrait.proto.Type.Struct struct = namedStruct.getStruct();
    return NamedStruct.builder()
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

  protected EmptyScan newEmptyScan(final ReadRel rel) {
    final NamedStruct namedStruct = newNamedStruct(rel);
    final ImmutableEmptyScan.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected ExtensionLeaf newExtensionLeaf(final ExtensionLeafRel rel) {
    final Extension.LeafRelDetail detail = detailFromExtensionLeafRel(rel.getDetail());
    final ImmutableExtensionLeaf.Builder builder =
        ExtensionLeaf.from(detail)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionSingle newExtensionSingle(final ExtensionSingleRel rel) {
    final Extension.SingleRelDetail detail = detailFromExtensionSingleRel(rel.getDetail());
    final Rel input = from(rel.getInput());
    final ImmutableExtensionSingle.Builder builder =
        ExtensionSingle.from(detail, input)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionMulti newExtensionMulti(final ExtensionMultiRel rel) {
    final Extension.MultiRelDetail detail = detailFromExtensionMultiRel(rel.getDetail());
    final List<Rel> inputs =
        rel.getInputsList().stream().map(this::from).collect(Collectors.toList());
    final ImmutableExtensionMulti.Builder builder =
        ExtensionMulti.from(detail, inputs)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    if (rel.hasDetail()) {
      builder.detail(detailFromExtensionMultiRel(rel.getDetail()));
    }
    return builder.build();
  }

  protected NamedScan newNamedScan(final ReadRel rel) {
    final NamedStruct namedStruct = newNamedStruct(rel);
    final ImmutableNamedScan.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected ExtensionTable newExtensionTable(final ReadRel rel) {
    final NamedStruct namedStruct = newNamedStruct(rel);
    final Extension.ExtensionTableDetail detail =
        detailFromExtensionTable(rel.getExtensionTable().getDetail());
    final ImmutableExtensionTable.Builder builder =
        ExtensionTable.from(detail).initialSchema(namedStruct);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected LocalFiles newLocalFiles(final ReadRel rel) {
    final NamedStruct namedStruct = newNamedStruct(rel);

    final ImmutableLocalFiles.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected FileOrFiles newFileOrFiles(final ReadRel.LocalFiles.FileOrFiles file) {
    final io.substrait.relation.files.ImmutableFileOrFiles.Builder builder =
        FileOrFiles.builder()
            .partitionIndex(file.getPartitionIndex())
            .start(file.getStart())
            .length(file.getLength());
    if (file.hasParquet()) {
      builder.fileFormat(FileFormat.ParquetReadOptions.builder().build());
    } else if (file.hasOrc()) {
      builder.fileFormat(FileFormat.OrcReadOptions.builder().build());
    } else if (file.hasArrow()) {
      builder.fileFormat(FileFormat.ArrowReadOptions.builder().build());
    } else if (file.hasDwrf()) {
      builder.fileFormat(FileFormat.DwrfReadOptions.builder().build());
    } else if (file.hasText()) {
      final io.substrait.relation.files.ImmutableFileFormat.DelimiterSeparatedTextReadOptions
              .Builder
          ffBuilder =
              FileFormat.DelimiterSeparatedTextReadOptions.builder()
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
      builder.fileFormat(FileFormat.Extension.builder().extension(file.getExtension()).build());
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

  protected VirtualTableScan newVirtualTable(final ReadRel rel) {
    final ReadRel.VirtualTable virtualTable = rel.getVirtualTable();
    final NamedStruct virtualTableSchema = newNamedStruct(rel);
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, virtualTableSchema.struct(), this);
    final List<Expression.StructLiteral> structLiterals =
        new ArrayList<>(virtualTable.getValuesCount());
    for (final io.substrait.proto.Expression.Literal.Struct struct : virtualTable.getValuesList()) {
      structLiterals.add(
          Expression.StructLiteral.builder()
              .fields(
                  struct.getFieldsList().stream()
                      .map(converter::from)
                      .collect(java.util.stream.Collectors.toList()))
              .build());
    }

    final ImmutableVirtualTableScan.Builder builder =
        VirtualTableScan.builder()
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter() ? converter.from(rel.getBestEffortFilter()) : null))
            .filter(Optional.ofNullable(rel.hasFilter() ? converter.from(rel.getFilter()) : null))
            .initialSchema(NamedStruct.fromProto(rel.getBaseSchema(), protoTypeConverter))
            .rows(structLiterals);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Fetch newFetch(final FetchRel rel) {
    final Rel input = from(rel.getInput());
    final ImmutableFetch.Builder builder = Fetch.builder().input(input).offset(rel.getOffset());
    if (rel.getCount() != -1) {
      // -1 is used as a sentinel value to signal LIMIT ALL
      // count only needs to be set when it is not -1
      builder.count(rel.getCount());
    }

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Project newProject(final ProjectRel rel) {
    final Rel input = from(rel.getInput());
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    final ImmutableProject.Builder builder =
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
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Expand newExpand(final ExpandRel rel) {
    final Rel input = from(rel.getInput());
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    final ImmutableExpand.Builder builder =
        Expand.builder()
            .input(input)
            .fields(
                rel.getFieldsList().stream()
                    .map(
                        expandField -> {
                          switch (expandField.getFieldTypeCase()) {
                            case CONSISTENT_FIELD:
                              return Expand.ConsistentField.builder()
                                  .expression(converter.from(expandField.getConsistentField()))
                                  .build();
                            case SWITCHING_FIELD:
                              return Expand.SwitchingField.builder()
                                  .duplicates(
                                      expandField.getSwitchingField().getDuplicatesList().stream()
                                          .map(converter::from)
                                          .collect(java.util.stream.Collectors.toList()))
                                  .build();
                            case FIELDTYPE_NOT_SET:
                            default:
                              throw new UnsupportedOperationException("Expand fields not set");
                          }
                        })
                    .collect(java.util.stream.Collectors.toList()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected Aggregate newAggregate(final AggregateRel rel) {
    final Rel input = from(rel.getInput());
    final ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    final ProtoAggregateFunctionConverter protoAggrFuncConverter =
        new ProtoAggregateFunctionConverter(lookup, extensions, protoExprConverter);

    final List<Aggregate.Grouping> groupings = new ArrayList<>(rel.getGroupingsCount());

    // Groupings are set using the AggregateRel grouping_expression mechanism
    if (!rel.getGroupingExpressionsList().isEmpty()) {
      final List<Expression> allGroupingExpressions =
          rel.getGroupingExpressionsList().stream()
              .map(protoExprConverter::from)
              .collect(java.util.stream.Collectors.toList());

      for (final AggregateRel.Grouping grouping : rel.getGroupingsList()) {
        final List<Integer> references = grouping.getExpressionReferencesList();
        final List<Expression> groupExpressions = new ArrayList<>();
        for (final int ref : references) {
          groupExpressions.add(allGroupingExpressions.get(ref));
        }
        groupings.add(Aggregate.Grouping.builder().addAllExpressions(groupExpressions).build());
      }

    } else {
      // Groupings are set using the deprecated Grouping grouping_expressions mechanism
      for (final AggregateRel.Grouping grouping : rel.getGroupingsList()) {
        groupings.add(
            Aggregate.Grouping.builder()
                .expressions(
                    grouping.getGroupingExpressionsList().stream()
                        .map(protoExprConverter::from)
                        .collect(java.util.stream.Collectors.toList()))
                .build());
      }
    }

    final List<Aggregate.Measure> measures = new ArrayList<>(rel.getMeasuresCount());
    for (final AggregateRel.Measure measure : rel.getMeasuresList()) {
      measures.add(
          Aggregate.Measure.builder()
              .function(protoAggrFuncConverter.from(measure.getMeasure()))
              .preMeasureFilter(
                  Optional.ofNullable(
                      measure.hasFilter() ? protoExprConverter.from(measure.getFilter()) : null))
              .build());
    }
    final ImmutableAggregate.Builder builder =
        Aggregate.builder().input(input).groupings(groupings).measures(measures);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Sort newSort(final SortRel rel) {
    final Rel input = from(rel.getInput());
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    final ImmutableSort.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Join newJoin(final JoinRel rel) {
    final Rel left = from(rel.getLeft());
    final Rel right = from(rel.getRight());
    final Type.Struct leftStruct = left.getRecordType();
    final Type.Struct rightStruct = right.getRecordType();
    final Type.Struct unionedStruct =
        Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    final ImmutableJoin.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newCross(final CrossRel rel) {
    final Rel left = from(rel.getLeft());
    final Rel right = from(rel.getRight());
    final ImmutableCross.Builder builder = Cross.builder().left(left).right(right);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Set newSet(final SetRel rel) {
    final List<Rel> inputs =
        rel.getInputsList().stream()
            .map(inputRel -> from(inputRel))
            .collect(java.util.stream.Collectors.toList());
    final ImmutableSet.Builder builder =
        Set.builder().inputs(inputs).setOp(Set.SetOp.fromProto(rel.getOp()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newHashJoin(final HashJoinRel rel) {
    final Rel left = from(rel.getLeft());
    final Rel right = from(rel.getRight());
    final List<io.substrait.proto.Expression.FieldReference> leftKeys = rel.getLeftKeysList();
    final List<io.substrait.proto.Expression.FieldReference> rightKeys = rel.getRightKeysList();

    final Type.Struct leftStruct = left.getRecordType();
    final Type.Struct rightStruct = right.getRecordType();
    final Type.Struct unionedStruct =
        Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    final ProtoExpressionConverter leftConverter =
        new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    final ProtoExpressionConverter rightConverter =
        new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    final ProtoExpressionConverter unionConverter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    final io.substrait.relation.physical.ImmutableHashJoin.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Rel newMergeJoin(final MergeJoinRel rel) {
    final Rel left = from(rel.getLeft());
    final Rel right = from(rel.getRight());
    final List<io.substrait.proto.Expression.FieldReference> leftKeys = rel.getLeftKeysList();
    final List<io.substrait.proto.Expression.FieldReference> rightKeys = rel.getRightKeysList();

    final Type.Struct leftStruct = left.getRecordType();
    final Type.Struct rightStruct = right.getRecordType();
    final Type.Struct unionedStruct =
        Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    final ProtoExpressionConverter leftConverter =
        new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    final ProtoExpressionConverter rightConverter =
        new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    final ProtoExpressionConverter unionConverter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    final io.substrait.relation.physical.ImmutableMergeJoin.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected NestedLoopJoin newNestedLoopJoin(final NestedLoopJoinRel rel) {
    final Rel left = from(rel.getLeft());
    final Rel right = from(rel.getRight());
    final Type.Struct leftStruct = left.getRecordType();
    final Type.Struct rightStruct = right.getRecordType();
    final Type.Struct unionedStruct =
        Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    final ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    final io.substrait.relation.physical.ImmutableNestedLoopJoin.Builder builder =
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
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected ConsistentPartitionWindow newConsistentPartitionWindow(
      final ConsistentPartitionWindowRel rel) {

    final Rel input = from(rel.getInput());
    final ProtoExpressionConverter protoExpressionConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    final List<Expression> partitionExprs =
        rel.getPartitionExpressionsList().stream()
            .map(protoExpressionConverter::from)
            .collect(Collectors.toList());
    final List<Expression.SortField> sortFields =
        rel.getSortsList().stream()
            .map(protoExpressionConverter::fromSortField)
            .collect(Collectors.toList());
    final List<ConsistentPartitionWindow.WindowRelFunctionInvocation> windowRelFunctions =
        rel.getWindowFunctionsList().stream()
            .map(protoExpressionConverter::fromWindowRelFunction)
            .collect(Collectors.toList());

    final ImmutableConsistentPartitionWindow.Builder builder =
        ConsistentPartitionWindow.builder()
            .input(input)
            .partitionExpressions(partitionExprs)
            .sorts(sortFields)
            .windowFunctions(windowRelFunctions);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected AbstractExchangeRel newExchange(final ExchangeRel rel) {
    final ExchangeRel.ExchangeKindCase exchangeKind = rel.getExchangeKindCase();
    switch (exchangeKind) {
      case SCATTER_BY_FIELDS:
        return newScatterExchange(rel);
      case SINGLE_TARGET:
        return newSingleBucketExchange(rel);
      case MULTI_TARGET:
        return newMultiBucketExchange(rel);
      case BROADCAST:
        return newBroadcastExchange(rel);
      case ROUND_ROBIN:
        return newRoundRobinExchange(rel);
      default:
        throw new UnsupportedOperationException("Unsupported ExchangeKindCase of " + exchangeKind);
    }
  }

  protected ScatterExchange newScatterExchange(final ExchangeRel rel) {
    final Rel input = from(rel.getInput());
    final List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    final ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    final List<FieldReference> fieldReferences =
        rel.getScatterByFields().getFieldsList().stream()
            .map(protoExprConverter::from)
            .collect(Collectors.toList());

    final ImmutableScatterExchange.Builder builder =
        ScatterExchange.builder()
            .input(input)
            .addAllFields(fieldReferences)
            .partitionCount(rel.getPartitionCount())
            .targets(targets);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected SingleBucketExchange newSingleBucketExchange(final ExchangeRel rel) {
    final Rel input = from(rel.getInput());
    final List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());
    final ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    final ImmutableSingleBucketExchange.Builder builder =
        SingleBucketExchange.builder()
            .input(input)
            .partitionCount(rel.getPartitionCount())
            .targets(targets)
            .expression(protoExprConverter.from(rel.getSingleTarget().getExpression()));

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected MultiBucketExchange newMultiBucketExchange(final ExchangeRel rel) {
    final Rel input = from(rel.getInput());
    final List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());
    final ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    final ImmutableMultiBucketExchange.Builder builder =
        MultiBucketExchange.builder()
            .input(input)
            .partitionCount(rel.getPartitionCount())
            .targets(targets)
            .expression(protoExprConverter.from(rel.getMultiTarget().getExpression()))
            .constrainedToCount(rel.getMultiTarget().getConstrainedToCount());

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected RoundRobinExchange newRoundRobinExchange(final ExchangeRel rel) {
    final Rel input = from(rel.getInput());
    final List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    final ImmutableRoundRobinExchange.Builder builder =
        RoundRobinExchange.builder()
            .input(input)
            .partitionCount(rel.getPartitionCount())
            .targets(targets)
            .exact(rel.getRoundRobin().getExact());

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected BroadcastExchange newBroadcastExchange(final ExchangeRel rel) {
    final Rel input = from(rel.getInput());
    final List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    final ImmutableBroadcastExchange.Builder builder =
        BroadcastExchange.builder()
            .input(input)
            .partitionCount(rel.getPartitionCount())
            .targets(targets);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected AbstractExchangeRel.ExchangeTarget newExchangeTarget(
      final ExchangeRel.ExchangeTarget target) {
    final ImmutableExchangeTarget.Builder builder = AbstractExchangeRel.ExchangeTarget.builder();
    builder.addAllPartitionIds(target.getPartitionIdList());
    switch (target.getTargetTypeCase()) {
      case URI:
        builder.type(TargetType.Uri.builder().uri(target.getUri()).build());
        break;
      case EXTENDED:
        builder.type(TargetType.Extended.builder().extended(target.getExtended()).build());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported TargetTypeCase of " + target.getTargetTypeCase());
    }
    return builder.build();
  }

  protected static Optional<Rel.Remap> optionalRelmap(
      final io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasEmit() ? Rel.Remap.of(relCommon.getEmit().getOutputMappingList()) : null);
  }

  protected Optional<Hint> optionalHint(final io.substrait.proto.RelCommon relCommon) {
    if (!relCommon.hasHint()) return Optional.empty();
    final io.substrait.proto.RelCommon.Hint hint = relCommon.getHint();
    final io.substrait.hint.ImmutableHint.Builder builder =
        Hint.builder().addAllOutputNames(hint.getOutputNamesList());
    if (!hint.getAlias().isEmpty()) {
      builder.alias(hint.getAlias());
    }
    if (hint.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(hint.getAdvancedExtension()));
    }
    if (hint.hasStats()) {
      final io.substrait.proto.RelCommon.Hint.Stats stats = hint.getStats();
      final io.substrait.hint.ImmutableStats.Builder statsBuilder = Stats.builder();
      statsBuilder.recordSize(stats.getRecordSize()).rowCount(stats.getRowCount());
      if (stats.hasAdvancedExtension()) {
        statsBuilder.extension(protoExtensionConverter.fromProto(stats.getAdvancedExtension()));
      }
      builder.stats(statsBuilder.build());
    }
    if (hint.hasConstraint()) {
      final io.substrait.proto.RelCommon.Hint.RuntimeConstraint constraint = hint.getConstraint();
      final io.substrait.hint.ImmutableRuntimeConstraint.Builder constraintBuilder =
          RuntimeConstraint.builder();
      if (constraint.hasAdvancedExtension()) {
        constraintBuilder.extension(
            protoExtensionConverter.fromProto(constraint.getAdvancedExtension()));
      }
      builder.runtimeConstraint(constraintBuilder.build());
    }

    hint.getLoadedComputationsList()
        .forEach(
            loadedComp ->
                builder.addLoadedComputations(
                    LoadedComputation.builder()
                        .computationId(loadedComp.getComputationIdReference())
                        .computationType(ComputationType.fromProto(loadedComp.getType()))
                        .build()));
    hint.getSavedComputationsList()
        .forEach(
            savedComp ->
                builder.addSavedComputations(
                    SavedComputation.builder()
                        .computationId(savedComp.getComputationId())
                        .computationType(ComputationType.fromProto(savedComp.getType()))
                        .build()));

    return Optional.of(builder.build());
  }

  protected Optional<AdvancedExtension> optionalAdvancedExtension(
      final io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasAdvancedExtension()
            ? protoExtensionConverter.fromProto(relCommon.getAdvancedExtension())
            : null);
  }

  /** Override to provide a custom converter for {@link ExtensionLeafRel#getDetail()} data */
  protected Extension.LeafRelDetail detailFromExtensionLeafRel(final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /** Override to provide a custom converter for {@link ExtensionSingleRel#getDetail()} data */
  protected Extension.SingleRelDetail detailFromExtensionSingleRel(
      final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /** Override to provide a custom converter for {@link ExtensionMultiRel#getDetail()} data */
  protected Extension.MultiRelDetail detailFromExtensionMultiRel(
      final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  /**
   * Override to provide a custom converter for {@link
   * io.substrait.proto.ReadRel.ExtensionTable#getDetail()} data
   */
  protected Extension.ExtensionTableDetail detailFromExtensionTable(
      final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  protected Extension.WriteExtensionObject detailFromWriteExtensionObject(
      final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  protected Extension.DdlExtensionObject detailFromDdlExtensionObject(
      final com.google.protobuf.Any any) {
    return emptyDetail();
  }

  private EmptyDetail emptyDetail() {
    return new EmptyDetail();
  }
}
