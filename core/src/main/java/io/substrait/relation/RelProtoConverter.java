package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ExpressionProtoConverter.BoundConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.plan.Plan;
import io.substrait.proto.AggregateFunction;
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
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.proto.TypeProtoConverter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Converts from {@link io.substrait.relation.Rel} to {@link io.substrait.proto.Rel} */
public class RelProtoConverter implements RelVisitor<Rel, RuntimeException> {

  protected final ExpressionProtoConverter exprProtoConverter;
  protected final TypeProtoConverter typeProtoConverter;

  protected final ExtensionCollector extensionCollector;

  public RelProtoConverter(ExtensionCollector extensionCollector) {
    this.extensionCollector = extensionCollector;
    this.exprProtoConverter = new ExpressionProtoConverter(extensionCollector, this);
    this.typeProtoConverter = new TypeProtoConverter(extensionCollector);
  }

  public ExpressionProtoConverter getExpressionProtoConverter() {
    return this.exprProtoConverter;
  }

  public TypeProtoConverter getTypeProtoConverter() {
    return this.typeProtoConverter;
  }

  public io.substrait.proto.RelRoot toProto(Plan.Root relRoot) {
    return RelRoot.newBuilder()
        .setInput(toProto(relRoot.getInput()))
        .addAllNames(relRoot.getNames())
        .build();
  }

  public io.substrait.proto.Rel toProto(io.substrait.relation.Rel rel) {
    return rel.accept(this);
  }

  protected io.substrait.proto.Expression toProto(io.substrait.expression.Expression expression) {
    return exprProtoConverter.toProto(expression);
  }

  protected List<io.substrait.proto.Expression> toProto(
      List<io.substrait.expression.Expression> expression) {
    return exprProtoConverter.toProto(expression);
  }

  protected io.substrait.proto.Type toProto(io.substrait.type.Type type) {
    return typeProtoConverter.toProto(type);
  }

  private List<io.substrait.proto.SortField> toProtoS(List<Expression.SortField> sorts) {
    return sorts.stream()
        .map(
            s -> {
              return SortField.newBuilder()
                  .setDirection(s.direction().toProto())
                  .setExpr(toProto(s.expr()))
                  .build();
            })
        .collect(Collectors.toList());
  }

  private io.substrait.proto.Expression.FieldReference toProto(FieldReference fieldReference) {
    return toProto((Expression) fieldReference).getSelection();
  }

  @Override
  public Rel visit(Aggregate aggregate) throws RuntimeException {
    var builder =
        AggregateRel.newBuilder()
            .setInput(toProto(aggregate.getInput()))
            .setCommon(common(aggregate))
            .addAllGroupings(
                aggregate.getGroupings().stream().map(this::toProto).collect(Collectors.toList()))
            .addAllMeasures(
                aggregate.getMeasures().stream().map(this::toProto).collect(Collectors.toList()));

    aggregate.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setAggregate(builder).build();
  }

  private AggregateRel.Measure toProto(Aggregate.Measure measure) {
    var argVisitor = FunctionArg.toProto(typeProtoConverter, exprProtoConverter);
    var args = measure.getFunction().arguments();
    var aggFuncDef = measure.getFunction().declaration();

    var func =
        AggregateFunction.newBuilder()
            .setPhase(measure.getFunction().aggregationPhase().toProto())
            .setInvocation(measure.getFunction().invocation().toProto())
            .setOutputType(toProto(measure.getFunction().getType()))
            .addAllArguments(
                IntStream.range(0, args.size())
                    .mapToObj(i -> args.get(i).accept(aggFuncDef, i, argVisitor))
                    .collect(Collectors.toList()))
            .addAllSorts(toProtoS(measure.getFunction().sort()))
            .setFunctionReference(
                extensionCollector.getFunctionReference(measure.getFunction().declaration()))
            .addAllOptions(
                measure.getFunction().options().stream()
                    .map(ExpressionProtoConverter::from)
                    .collect(Collectors.toList()));

    var builder = AggregateRel.Measure.newBuilder().setMeasure(func);

    measure.getPreMeasureFilter().ifPresent(f -> builder.setFilter(toProto(f)));
    return builder.build();
  }

  private AggregateRel.Grouping toProto(Aggregate.Grouping grouping) {
    return AggregateRel.Grouping.newBuilder()
        .addAllGroupingExpressions(toProto(grouping.getExpressions()))
        .build();
  }

  @Override
  public Rel visit(EmptyScan emptyScan) throws RuntimeException {
    return Rel.newBuilder()
        .setRead(
            ReadRel.newBuilder()
                .setCommon(common(emptyScan))
                .setVirtualTable(ReadRel.VirtualTable.newBuilder().build())
                .setBaseSchema(emptyScan.getInitialSchema().toProto(typeProtoConverter))
                .build())
        .build();
  }

  @Override
  public Rel visit(Fetch fetch) throws RuntimeException {
    var builder =
        FetchRel.newBuilder()
            .setCommon(common(fetch))
            .setInput(toProto(fetch.getInput()))
            .setOffset(fetch.getOffset())
            // -1 is used as a sentinel value to signal LIMIT ALL
            .setCount(fetch.getCount().orElse(-1));

    fetch.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setFetch(builder).build();
  }

  @Override
  public Rel visit(Filter filter) throws RuntimeException {
    var builder =
        FilterRel.newBuilder()
            .setCommon(common(filter))
            .setInput(toProto(filter.getInput()))
            .setCondition(toProto(filter.getCondition()));

    filter.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setFilter(builder).build();
  }

  @Override
  public Rel visit(Join join) throws RuntimeException {
    var builder =
        JoinRel.newBuilder()
            .setCommon(common(join))
            .setLeft(toProto(join.getLeft()))
            .setRight(toProto(join.getRight()))
            .setType(join.getJoinType().toProto());

    join.getCondition().ifPresent(t -> builder.setExpression(toProto(t)));

    join.getPostJoinFilter().ifPresent(t -> builder.setPostJoinFilter(toProto(t)));

    join.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setJoin(builder).build();
  }

  @Override
  public Rel visit(Set set) throws RuntimeException {
    var builder = SetRel.newBuilder().setCommon(common(set)).setOp(set.getSetOp().toProto());
    set.getInputs()
        .forEach(
            inputRel -> {
              builder.addInputs(toProto(inputRel));
            });

    set.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setSet(builder).build();
  }

  @Override
  public Rel visit(NamedScan namedScan) throws RuntimeException {
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(namedScan))
            .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(namedScan.getNames()))
            .setBaseSchema(namedScan.getInitialSchema().toProto(typeProtoConverter));

    namedScan.getFilter().ifPresent(f -> builder.setFilter(toProto(f)));
    namedScan.getBestEffortFilter().ifPresent(f -> builder.setBestEffortFilter(toProto(f)));

    namedScan.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setRead(builder).build();
  }

  @Override
  public Rel visit(LocalFiles localFiles) throws RuntimeException {
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(localFiles))
            .setLocalFiles(
                ReadRel.LocalFiles.newBuilder()
                    .addAllItems(
                        localFiles.getItems().stream()
                            .map(FileOrFiles::toProto)
                            .collect(Collectors.toList()))
                    .build())
            .setBaseSchema(localFiles.getInitialSchema().toProto(typeProtoConverter));
    localFiles.getFilter().ifPresent(t -> builder.setFilter(toProto(t)));
    localFiles.getBestEffortFilter().ifPresent(t -> builder.setBestEffortFilter(toProto(t)));

    localFiles.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setRead(builder.build()).build();
  }

  @Override
  public Rel visit(ExtensionTable extensionTable) throws RuntimeException {
    ReadRel.ExtensionTable.Builder extensionTableBuilder =
        ReadRel.ExtensionTable.newBuilder().setDetail(extensionTable.getDetail().toProto(this));
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(extensionTable))
            .setBaseSchema(extensionTable.getInitialSchema().toProto(typeProtoConverter))
            .setExtensionTable(extensionTableBuilder);

    extensionTable.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setRead(builder).build();
  }

  @Override
  public Rel visit(HashJoin hashJoin) throws RuntimeException {
    var builder =
        HashJoinRel.newBuilder()
            .setCommon(common(hashJoin))
            .setLeft(toProto(hashJoin.getLeft()))
            .setRight(toProto(hashJoin.getRight()))
            .setType(hashJoin.getJoinType().toProto());

    List<FieldReference> leftKeys = hashJoin.getLeftKeys();
    List<FieldReference> rightKeys = hashJoin.getRightKeys();

    if (leftKeys.size() != rightKeys.size()) {
      throw new RuntimeException("Number of left and right keys must be equal.");
    }

    builder.addAllLeftKeys(leftKeys.stream().map(this::toProto).collect(Collectors.toList()));
    builder.addAllRightKeys(rightKeys.stream().map(this::toProto).collect(Collectors.toList()));

    hashJoin.getPostJoinFilter().ifPresent(t -> builder.setPostJoinFilter(toProto(t)));

    hashJoin.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setHashJoin(builder).build();
  }

  @Override
  public Rel visit(MergeJoin mergeJoin) throws RuntimeException {
    var builder =
        MergeJoinRel.newBuilder()
            .setCommon(common(mergeJoin))
            .setLeft(toProto(mergeJoin.getLeft()))
            .setRight(toProto(mergeJoin.getRight()))
            .setType(mergeJoin.getJoinType().toProto());

    List<FieldReference> leftKeys = mergeJoin.getLeftKeys();
    List<FieldReference> rightKeys = mergeJoin.getRightKeys();

    if (leftKeys.size() != rightKeys.size()) {
      throw new RuntimeException("Number of left and right keys must be equal.");
    }

    builder.addAllLeftKeys(leftKeys.stream().map(this::toProto).collect(Collectors.toList()));
    builder.addAllRightKeys(rightKeys.stream().map(this::toProto).collect(Collectors.toList()));

    mergeJoin.getPostJoinFilter().ifPresent(t -> builder.setPostJoinFilter(toProto(t)));

    mergeJoin.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setMergeJoin(builder).build();
  }

  @Override
  public Rel visit(NestedLoopJoin nestedLoopJoin) throws RuntimeException {
    var builder =
        NestedLoopJoinRel.newBuilder()
            .setCommon(common(nestedLoopJoin))
            .setLeft(toProto(nestedLoopJoin.getLeft()))
            .setRight(toProto(nestedLoopJoin.getRight()))
            .setExpression(toProto(nestedLoopJoin.getCondition()))
            .setType(nestedLoopJoin.getJoinType().toProto());

    nestedLoopJoin.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setNestedLoopJoin(builder).build();
  }

  @Override
  public Rel visit(ConsistentPartitionWindow consistentPartitionWindow) throws RuntimeException {
    var builder =
        ConsistentPartitionWindowRel.newBuilder()
            .setCommon(common(consistentPartitionWindow))
            .setInput(toProto(consistentPartitionWindow.getInput()))
            .addAllSorts(toProtoS(consistentPartitionWindow.getSorts()))
            .addAllPartitionExpressions(
                toProto(consistentPartitionWindow.getPartitionExpressions()))
            .addAllWindowFunctions(
                toProtoWindowRelFunctions(consistentPartitionWindow.getWindowFunctions()));

    consistentPartitionWindow
        .getExtension()
        .ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));

    return Rel.newBuilder().setWindow(builder).build();
  }

  private List<ConsistentPartitionWindowRel.WindowRelFunction> toProtoWindowRelFunctions(
      Collection<ConsistentPartitionWindow.WindowRelFunctionInvocation>
          windowRelFunctionInvocations) {

    return windowRelFunctionInvocations.stream()
        .map(
            f -> {
              var argVisitor = FunctionArg.toProto(typeProtoConverter, exprProtoConverter);
              var args = f.arguments();
              var aggFuncDef = f.declaration();

              var arguments =
                  IntStream.range(0, args.size())
                      .mapToObj(i -> args.get(i).accept(aggFuncDef, i, argVisitor))
                      .collect(Collectors.toList());
              var options =
                  f.options().stream()
                      .map(ExpressionProtoConverter::from)
                      .collect(Collectors.toList());

              return ConsistentPartitionWindowRel.WindowRelFunction.newBuilder()
                  .setInvocation(f.invocation().toProto())
                  .setPhase(f.aggregationPhase().toProto())
                  .setOutputType(toProto(f.outputType()))
                  .addAllArguments(arguments)
                  .addAllOptions(options)
                  .setFunctionReference(extensionCollector.getFunctionReference(f.declaration()))
                  .setBoundsType(f.boundsType().toProto())
                  .setLowerBound(BoundConverter.convert(f.lowerBound()))
                  .setUpperBound(BoundConverter.convert(f.upperBound()))
                  .build();
            })
        .collect(Collectors.toList());
  }

  @Override
  public Rel visit(Project project) throws RuntimeException {
    var builder =
        ProjectRel.newBuilder()
            .setCommon(common(project))
            .setInput(toProto(project.getInput()))
            .addAllExpressions(toProto(project.getExpressions()));

    project.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setProject(builder).build();
  }

  @Override
  public Rel visit(Expand expand) throws RuntimeException {
    var builder =
        ExpandRel.newBuilder().setCommon(common(expand)).setInput(toProto(expand.getInput()));

    expand
        .getFields()
        .forEach(
            expandField -> {
              if (expandField instanceof Expand.ConsistentField cf) {
                builder.addFields(
                    ExpandRel.ExpandField.newBuilder()
                        .setConsistentField(toProto(cf.getExpression()))
                        .build());

              } else if (expandField instanceof Expand.SwitchingField sf) {
                builder.addFields(
                    ExpandRel.ExpandField.newBuilder()
                        .setSwitchingField(
                            ExpandRel.SwitchingField.newBuilder()
                                .addAllDuplicates(toProto(sf.getDuplicates())))
                        .build());
              } else {
                throw new RuntimeException(
                    "Consistent or Switching fields must be set for the Expand relation.");
              }
            });
    return Rel.newBuilder().setExpand(builder).build();
  }

  @Override
  public Rel visit(Sort sort) throws RuntimeException {
    var builder =
        SortRel.newBuilder()
            .setCommon(common(sort))
            .setInput(toProto(sort.getInput()))
            .addAllSorts(toProtoS(sort.getSortFields()));

    sort.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setSort(builder).build();
  }

  @Override
  public Rel visit(Cross cross) throws RuntimeException {
    var builder =
        CrossRel.newBuilder()
            .setCommon(common(cross))
            .setLeft(toProto(cross.getLeft()))
            .setRight(toProto(cross.getRight()));

    cross.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setCross(builder).build();
  }

  @Override
  public Rel visit(VirtualTableScan virtualTableScan) throws RuntimeException {
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(virtualTableScan))
            .setVirtualTable(
                ReadRel.VirtualTable.newBuilder()
                    .addAllValues(
                        virtualTableScan.getRows().stream()
                            .map(this::toProto)
                            .map(t -> t.getLiteral().getStruct())
                            .collect(Collectors.toList()))
                    .build())
            .setBaseSchema(virtualTableScan.getInitialSchema().toProto(typeProtoConverter));

    virtualTableScan.getFilter().ifPresent(f -> builder.setFilter(toProto(f)));
    virtualTableScan.getBestEffortFilter().ifPresent(f -> builder.setBestEffortFilter(toProto(f)));

    virtualTableScan.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto(this)));
    return Rel.newBuilder().setRead(builder).build();
  }

  @Override
  public Rel visit(ExtensionLeaf extensionLeaf) throws RuntimeException {
    var builder =
        ExtensionLeafRel.newBuilder()
            .setCommon(common(extensionLeaf))
            .setDetail(extensionLeaf.getDetail().toProto(this));
    return Rel.newBuilder().setExtensionLeaf(builder).build();
  }

  @Override
  public Rel visit(ExtensionSingle extensionSingle) throws RuntimeException {
    var builder =
        ExtensionSingleRel.newBuilder()
            .setCommon(common(extensionSingle))
            .setInput(toProto(extensionSingle.getInput()))
            .setDetail(extensionSingle.getDetail().toProto(this));
    return Rel.newBuilder().setExtensionSingle(builder).build();
  }

  @Override
  public Rel visit(ExtensionMulti extensionMulti) throws RuntimeException {
    List<Rel> inputs =
        extensionMulti.getInputs().stream().map(this::toProto).collect(Collectors.toList());
    var builder =
        ExtensionMultiRel.newBuilder()
            .setCommon(common(extensionMulti))
            .addAllInputs(inputs)
            .setDetail(extensionMulti.getDetail().toProto(this));
    return Rel.newBuilder().setExtensionMulti(builder).build();
  }

  private RelCommon common(io.substrait.relation.Rel rel) {
    var builder = RelCommon.newBuilder();
    rel.getCommonExtension()
        .ifPresent(extension -> builder.setAdvancedExtension(extension.toProto(this)));

    var remap = rel.getRemap().orElse(null);
    if (remap != null) {
      builder.setEmit(RelCommon.Emit.newBuilder().addAllOutputMapping(remap.indices()));
    } else {
      builder.setDirect(RelCommon.Direct.getDefaultInstance());
    }

    rel.getHint().ifPresent(md -> builder.setHint(md.toProto()));

    return builder.build();
  }
}
