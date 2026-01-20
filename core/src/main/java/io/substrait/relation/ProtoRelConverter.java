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

  public Plan.Root from(io.substrait.proto.RelRoot rel) {
    return Plan.Root.builder().input(from(rel.getInput())).addAllNames(rel.getNamesList()).build();
  }

  public Rel from(io.substrait.proto.Rel rel) {
    io.substrait.proto.Rel.RelTypeCase relType = rel.getRelTypeCase();
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

  protected Rel newRead(ReadRel rel) {
    if (rel.hasNamedTable()) {
      return newNamedScan(rel);
    } else if (rel.hasLocalFiles()) {
      return newLocalFiles(rel);
    } else if (rel.hasExtensionTable()) {
      return newExtensionTable(rel);
    } else {
      return newVirtualTable(rel);
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

  protected Optional<Rel> optionalViewDefinition(DdlRel rel) {
    return Optional.ofNullable(rel.hasViewDefinition() ? from(rel.getViewDefinition()) : null);
  }

  protected Expression.StructLiteral tableDefaults(
      io.substrait.proto.Expression.Literal.Struct struct, NamedStruct tableSchema) {
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, tableSchema.struct(), this);
    return Expression.StructLiteral.builder()
        .fields(
            struct.getFieldsList().stream()
                .map(converter::from)
                .collect(java.util.stream.Collectors.toList()))
        .build();
  }

  protected Rel newUpdate(UpdateRel rel) {
    UpdateRel.UpdateTypeCase relType = rel.getUpdateTypeCase();
    switch (relType) {
      case NAMED_TABLE:
        return newNamedUpdate(rel);
      default:
        throw new UnsupportedOperationException("Unsupported UpdateTypeCase of " + relType);
    }
  }

  protected Rel newNamedUpdate(UpdateRel rel) {
    NamedStruct tableSchema = newNamedStruct(rel.getTableSchema());
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, tableSchema.struct(), this);
    List<NamedUpdate.TransformExpression> transformations =
        new ArrayList<>(rel.getTransformationsCount());
    for (UpdateRel.TransformExpression transformation : rel.getTransformationsList()) {
      transformations.add(
          NamedUpdate.TransformExpression.builder()
              .transformation(converter.from(transformation.getTransformation()))
              .columnTarget(transformation.getColumnTarget())
              .build());
    }
    ImmutableNamedUpdate.Builder builder =
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

  protected Filter newFilter(FilterRel rel) {
    Rel input = from(rel.getInput());
    ImmutableFilter.Builder builder =
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

  protected NamedStruct newNamedStruct(ReadRel rel) {
    return newNamedStruct(rel.getBaseSchema());
  }

  protected NamedStruct newNamedStruct(io.substrait.proto.NamedStruct namedStruct) {
    io.substrait.proto.Type.Struct struct = namedStruct.getStruct();
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

  protected ExtensionLeaf newExtensionLeaf(ExtensionLeafRel rel) {
    Extension.LeafRelDetail detail = detailFromExtensionLeafRel(rel.getDetail());
    ImmutableExtensionLeaf.Builder builder =
        ExtensionLeaf.from(detail)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionSingle newExtensionSingle(ExtensionSingleRel rel) {
    Extension.SingleRelDetail detail = detailFromExtensionSingleRel(rel.getDetail());
    Rel input = from(rel.getInput());
    ImmutableExtensionSingle.Builder builder =
        ExtensionSingle.from(detail, input)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    return builder.build();
  }

  protected ExtensionMulti newExtensionMulti(ExtensionMultiRel rel) {
    Extension.MultiRelDetail detail = detailFromExtensionMultiRel(rel.getDetail());
    List<Rel> inputs = rel.getInputsList().stream().map(this::from).collect(Collectors.toList());
    ImmutableExtensionMulti.Builder builder =
        ExtensionMulti.from(detail, inputs)
            .commonExtension(optionalAdvancedExtension(rel.getCommon()))
            .remap(optionalRelmap(rel.getCommon()))
            .hint(optionalHint(rel.getCommon()));
    if (rel.hasDetail()) {
      builder.detail(detailFromExtensionMultiRel(rel.getDetail()));
    }
    return builder.build();
  }

  protected NamedScan newNamedScan(ReadRel rel) {
    NamedStruct namedStruct = newNamedStruct(rel);
    ImmutableNamedScan.Builder builder =
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

  protected LocalFiles newLocalFiles(ReadRel rel) {
    NamedStruct namedStruct = newNamedStruct(rel);

    ImmutableLocalFiles.Builder builder =
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

  protected FileOrFiles newFileOrFiles(ReadRel.LocalFiles.FileOrFiles file) {
    io.substrait.relation.files.ImmutableFileOrFiles.Builder builder =
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
      io.substrait.relation.files.ImmutableFileFormat.DelimiterSeparatedTextReadOptions.Builder
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

  /**
   * Converts StructLiteral instances to NestedStruct for VirtualTableScan. This is a convenience
   * method for migrating from the legacy StructLiteral-based VirtualTable API to the new
   * NestedStruct-based API.
   *
   * @param nullable whether the resulting NestedStruct instances should be nullable
   * @param structs the StructLiteral instances to convert
   * @return a list of NestedStruct instances with the same field structure
   */
  private static List<Expression.NestedStruct> nestedStruct(
      boolean nullable, Expression.StructLiteral... structs) {
    List<Expression.NestedStruct> nestedStructs = new ArrayList<>();
    for (Expression.StructLiteral struct : structs) {
      nestedStructs.add(
          Expression.NestedStruct.builder()
              .nullable(nullable)
              .addAllFields(struct.fields())
              .build());
    }
    return nestedStructs;
  }

  protected VirtualTableScan newVirtualTable(ReadRel rel) {
    ReadRel.VirtualTable virtualTable = rel.getVirtualTable();
    // If both values and expressions are set, raise an error
    if (virtualTable.getValuesCount() > 0 && virtualTable.getExpressionsCount() > 0) {
      throw new IllegalArgumentException(
          "VirtualTable cannot have both values and expressions set");
    }
    NamedStruct virtualTableSchema = newNamedStruct(rel);
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, virtualTableSchema.struct(), this);

    List<Expression.NestedStruct> expressions =
        new ArrayList<>(virtualTable.getValuesCount() + virtualTable.getExpressionsCount());

    // We cannot have a null row in VirtualTable, therefore we set the nullability to false
    // nullability is also not supported at the Expression.Nested.Struct level
    for (io.substrait.proto.Expression.Literal.Struct struct : virtualTable.getValuesList()) {
      expressions.addAll(nestedStruct(false, converter.from(struct)));
    }

    for (io.substrait.proto.Expression.Nested.Struct expr : virtualTable.getExpressionsList()) {
      expressions.add(converter.from(expr));
    }

    ImmutableVirtualTableScan.Builder builder =
        VirtualTableScan.builder()
            .bestEffortFilter(
                Optional.ofNullable(
                    rel.hasBestEffortFilter() ? converter.from(rel.getBestEffortFilter()) : null))
            .filter(Optional.ofNullable(rel.hasFilter() ? converter.from(rel.getFilter()) : null))
            .initialSchema(NamedStruct.fromProto(rel.getBaseSchema(), protoTypeConverter))
            .rows(expressions);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()))
        .hint(optionalHint(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Fetch newFetch(FetchRel rel) {
    Rel input = from(rel.getInput());
    ImmutableFetch.Builder builder = Fetch.builder().input(input).offset(rel.getOffset());
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

  protected Project newProject(ProjectRel rel) {
    Rel input = from(rel.getInput());
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    ImmutableProject.Builder builder =
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

  protected Expand newExpand(ExpandRel rel) {
    Rel input = from(rel.getInput());
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    ImmutableExpand.Builder builder =
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

  protected Aggregate newAggregate(AggregateRel rel) {
    Rel input = from(rel.getInput());
    ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    ProtoAggregateFunctionConverter protoAggrFuncConverter =
        new ProtoAggregateFunctionConverter(lookup, extensions, protoExprConverter);

    List<Aggregate.Grouping> groupings = new ArrayList<>(rel.getGroupingsCount());

    // Groupings are set using the AggregateRel grouping_expression mechanism
    if (!rel.getGroupingExpressionsList().isEmpty()) {
      List<Expression> allGroupingExpressions =
          rel.getGroupingExpressionsList().stream()
              .map(protoExprConverter::from)
              .collect(java.util.stream.Collectors.toList());

      for (AggregateRel.Grouping grouping : rel.getGroupingsList()) {
        List<Integer> references = grouping.getExpressionReferencesList();
        List<Expression> groupExpressions = new ArrayList<>();
        for (int ref : references) {
          groupExpressions.add(allGroupingExpressions.get(ref));
        }
        groupings.add(Aggregate.Grouping.builder().addAllExpressions(groupExpressions).build());
      }

    } else {
      // Groupings are set using the deprecated Grouping grouping_expressions mechanism
      for (AggregateRel.Grouping grouping : rel.getGroupingsList()) {
        groupings.add(
            Aggregate.Grouping.builder()
                .expressions(
                    grouping.getGroupingExpressionsList().stream()
                        .map(protoExprConverter::from)
                        .collect(java.util.stream.Collectors.toList()))
                .build());
      }
    }

    List<Aggregate.Measure> measures = new ArrayList<>(rel.getMeasuresCount());
    for (AggregateRel.Measure measure : rel.getMeasuresList()) {
      measures.add(
          Aggregate.Measure.builder()
              .function(protoAggrFuncConverter.from(measure.getMeasure()))
              .preMeasureFilter(
                  Optional.ofNullable(
                      measure.hasFilter() ? protoExprConverter.from(measure.getFilter()) : null))
              .build());
    }
    ImmutableAggregate.Builder builder =
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

  protected Sort newSort(SortRel rel) {
    Rel input = from(rel.getInput());
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    ImmutableSort.Builder builder =
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

  protected Join newJoin(JoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    ImmutableJoin.Builder builder =
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

  protected Rel newCross(CrossRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    ImmutableCross.Builder builder = Cross.builder().left(left).right(right);

    builder
        .commonExtension(optionalAdvancedExtension(rel.getCommon()))
        .remap(optionalRelmap(rel.getCommon()));
    if (rel.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(rel.getAdvancedExtension()));
    }
    return builder.build();
  }

  protected Set newSet(SetRel rel) {
    List<Rel> inputs =
        rel.getInputsList().stream()
            .map(inputRel -> from(inputRel))
            .collect(java.util.stream.Collectors.toList());
    ImmutableSet.Builder builder =
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

  protected Rel newHashJoin(HashJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    List<io.substrait.proto.Expression.FieldReference> leftKeys = rel.getLeftKeysList();
    List<io.substrait.proto.Expression.FieldReference> rightKeys = rel.getRightKeysList();

    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    ProtoExpressionConverter leftConverter =
        new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    ProtoExpressionConverter rightConverter =
        new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    ProtoExpressionConverter unionConverter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    io.substrait.relation.physical.ImmutableHashJoin.Builder builder =
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

  protected Rel newMergeJoin(MergeJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    List<io.substrait.proto.Expression.FieldReference> leftKeys = rel.getLeftKeysList();
    List<io.substrait.proto.Expression.FieldReference> rightKeys = rel.getRightKeysList();

    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    ProtoExpressionConverter leftConverter =
        new ProtoExpressionConverter(lookup, extensions, leftStruct, this);
    ProtoExpressionConverter rightConverter =
        new ProtoExpressionConverter(lookup, extensions, rightStruct, this);
    ProtoExpressionConverter unionConverter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    io.substrait.relation.physical.ImmutableMergeJoin.Builder builder =
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

  protected NestedLoopJoin newNestedLoopJoin(NestedLoopJoinRel rel) {
    Rel left = from(rel.getLeft());
    Rel right = from(rel.getRight());
    Type.Struct leftStruct = left.getRecordType();
    Type.Struct rightStruct = right.getRecordType();
    Type.Struct unionedStruct = Type.Struct.builder().from(leftStruct).from(rightStruct).build();
    ProtoExpressionConverter converter =
        new ProtoExpressionConverter(lookup, extensions, unionedStruct, this);
    io.substrait.relation.physical.ImmutableNestedLoopJoin.Builder builder =
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
      ConsistentPartitionWindowRel rel) {

    Rel input = from(rel.getInput());
    ProtoExpressionConverter protoExpressionConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    List<Expression> partitionExprs =
        rel.getPartitionExpressionsList().stream()
            .map(protoExpressionConverter::from)
            .collect(Collectors.toList());
    List<Expression.SortField> sortFields =
        rel.getSortsList().stream()
            .map(protoExpressionConverter::fromSortField)
            .collect(Collectors.toList());
    List<ConsistentPartitionWindow.WindowRelFunctionInvocation> windowRelFunctions =
        rel.getWindowFunctionsList().stream()
            .map(protoExpressionConverter::fromWindowRelFunction)
            .collect(Collectors.toList());

    ImmutableConsistentPartitionWindow.Builder builder =
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

  protected AbstractExchangeRel newExchange(ExchangeRel rel) {
    ExchangeRel.ExchangeKindCase exchangeKind = rel.getExchangeKindCase();
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

  protected ScatterExchange newScatterExchange(ExchangeRel rel) {
    Rel input = from(rel.getInput());
    List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);
    List<FieldReference> fieldReferences =
        rel.getScatterByFields().getFieldsList().stream()
            .map(protoExprConverter::from)
            .collect(Collectors.toList());

    ImmutableScatterExchange.Builder builder =
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

  protected SingleBucketExchange newSingleBucketExchange(ExchangeRel rel) {
    Rel input = from(rel.getInput());
    List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());
    ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    ImmutableSingleBucketExchange.Builder builder =
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

  protected MultiBucketExchange newMultiBucketExchange(ExchangeRel rel) {
    Rel input = from(rel.getInput());
    List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());
    ProtoExpressionConverter protoExprConverter =
        new ProtoExpressionConverter(lookup, extensions, input.getRecordType(), this);

    ImmutableMultiBucketExchange.Builder builder =
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

  protected RoundRobinExchange newRoundRobinExchange(ExchangeRel rel) {
    Rel input = from(rel.getInput());
    List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    ImmutableRoundRobinExchange.Builder builder =
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

  protected BroadcastExchange newBroadcastExchange(ExchangeRel rel) {
    Rel input = from(rel.getInput());
    List<AbstractExchangeRel.ExchangeTarget> targets =
        rel.getTargetsList().stream().map(this::newExchangeTarget).collect(Collectors.toList());

    ImmutableBroadcastExchange.Builder builder =
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
      ExchangeRel.ExchangeTarget target) {
    ImmutableExchangeTarget.Builder builder = AbstractExchangeRel.ExchangeTarget.builder();
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

  protected static Optional<Rel.Remap> optionalRelmap(io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasEmit() ? Rel.Remap.of(relCommon.getEmit().getOutputMappingList()) : null);
  }

  protected Optional<Hint> optionalHint(io.substrait.proto.RelCommon relCommon) {
    if (!relCommon.hasHint()) return Optional.empty();
    io.substrait.proto.RelCommon.Hint hint = relCommon.getHint();
    io.substrait.hint.ImmutableHint.Builder builder =
        Hint.builder().addAllOutputNames(hint.getOutputNamesList());
    if (!hint.getAlias().isEmpty()) {
      builder.alias(hint.getAlias());
    }
    if (hint.hasAdvancedExtension()) {
      builder.extension(protoExtensionConverter.fromProto(hint.getAdvancedExtension()));
    }
    if (hint.hasStats()) {
      io.substrait.proto.RelCommon.Hint.Stats stats = hint.getStats();
      io.substrait.hint.ImmutableStats.Builder statsBuilder = Stats.builder();
      statsBuilder.recordSize(stats.getRecordSize()).rowCount(stats.getRowCount());
      if (stats.hasAdvancedExtension()) {
        statsBuilder.extension(protoExtensionConverter.fromProto(stats.getAdvancedExtension()));
      }
      builder.stats(statsBuilder.build());
    }
    if (hint.hasConstraint()) {
      io.substrait.proto.RelCommon.Hint.RuntimeConstraint constraint = hint.getConstraint();
      io.substrait.hint.ImmutableRuntimeConstraint.Builder constraintBuilder =
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
      io.substrait.proto.RelCommon relCommon) {
    return Optional.ofNullable(
        relCommon.hasAdvancedExtension()
            ? protoExtensionConverter.fromProto(relCommon.getAdvancedExtension())
            : null);
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

  protected Extension.DdlExtensionObject detailFromDdlExtensionObject(com.google.protobuf.Any any) {
    return emptyDetail();
  }

  private EmptyDetail emptyDetail() {
    return new EmptyDetail();
  }
}
