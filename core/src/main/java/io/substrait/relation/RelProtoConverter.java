package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.proto.AggregateFunction;
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
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.type.proto.TypeProtoConverter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Converts from {@link io.substrait.relation.Rel} to {@link io.substrait.proto.Rel} */
public class RelProtoConverter implements RelVisitor<Rel, RuntimeException> {

  private final ExpressionProtoConverter exprProtoConverter;
  private final TypeProtoConverter typeProtoConverter;
  private final FunctionCollector functionCollector;

  public RelProtoConverter(FunctionCollector functionCollector) {
    this.functionCollector = functionCollector;
    this.exprProtoConverter = new ExpressionProtoConverter(functionCollector, this);
    this.typeProtoConverter = new TypeProtoConverter(functionCollector);
  }

  private List<io.substrait.proto.Expression> toProto(Collection<Expression> expressions) {
    return expressions.stream().map(this::toProto).collect(java.util.stream.Collectors.toList());
  }

  private io.substrait.proto.Expression toProto(Expression expression) {
    return expression.accept(exprProtoConverter);
  }

  public io.substrait.proto.Rel toProto(io.substrait.relation.Rel rel) {
    return rel.accept(this);
  }

  private io.substrait.proto.Type toProto(io.substrait.type.Type type) {
    return type.accept(typeProtoConverter);
  }

  private List<SortField> toProtoS(Collection<Expression.SortField> sorts) {
    return sorts.stream()
        .map(
            s -> {
              return SortField.newBuilder()
                  .setDirection(s.direction().toProto())
                  .setExpr(toProto(s.expr()))
                  .build();
            })
        .collect(java.util.stream.Collectors.toList());
  }

  @Override
  public Rel visit(Aggregate aggregate) throws RuntimeException {
    var builder =
        AggregateRel.newBuilder()
            .setInput(toProto(aggregate.getInput()))
            .setCommon(common(aggregate))
            .addAllGroupings(
                aggregate.getGroupings().stream()
                    .map(this::toProto)
                    .collect(java.util.stream.Collectors.toList()))
            .addAllMeasures(
                aggregate.getMeasures().stream()
                    .map(this::toProto)
                    .collect(java.util.stream.Collectors.toList()));

    aggregate.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setAggregate(builder).build();
  }

  private AggregateRel.Measure toProto(Aggregate.Measure measure) {
    var argVisitor = FunctionArg.toProto(typeProtoConverter, exprProtoConverter);
    var args = measure.getFunction().arguments();
    var aggFuncDef = measure.getFunction().declaration();

    var func =
        AggregateFunction.newBuilder()
            .setPhase(measure.getFunction().aggregationPhase().toProto())
            .setInvocation(measure.getFunction().invocation())
            .setOutputType(toProto(measure.getFunction().getType()))
            .addAllArguments(
                IntStream.range(0, args.size())
                    .mapToObj(i -> args.get(i).accept(aggFuncDef, i, argVisitor))
                    .collect(java.util.stream.Collectors.toList()))
            .addAllSorts(toProtoS(measure.getFunction().sort()))
            .setFunctionReference(
                functionCollector.getFunctionReference(measure.getFunction().declaration()));

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
            .setOffset(fetch.getOffset());

    fetch.getCount().ifPresent(f -> builder.setCount(f));

    fetch.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setFetch(builder).build();
  }

  @Override
  public Rel visit(Filter filter) throws RuntimeException {
    var builder =
        FilterRel.newBuilder()
            .setCommon(common(filter))
            .setInput(toProto(filter.getInput()))
            .setCondition(filter.getCondition().accept(exprProtoConverter));

    filter.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
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

    join.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
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

    set.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setSet(builder).build();
  }

  @Override
  public Rel visit(NamedScan namedScan) throws RuntimeException {
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(namedScan))
            .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(namedScan.getNames()))
            .setBaseSchema(namedScan.getInitialSchema().toProto(typeProtoConverter));

    namedScan.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
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
                            .collect(java.util.stream.Collectors.toList()))
                    .build())
            .setBaseSchema(localFiles.getInitialSchema().toProto(typeProtoConverter));
    localFiles.getFilter().ifPresent(t -> builder.setFilter(toProto(t)));

    localFiles.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setRead(builder.build()).build();
  }

  @Override
  public Rel visit(ExtensionTable extensionTable) throws RuntimeException {
    ReadRel.ExtensionTable.Builder extensionTableBuilder =
        ReadRel.ExtensionTable.newBuilder().setDetail(extensionTable.getDetail().toProto());
    var builder =
        ReadRel.newBuilder()
            .setCommon(common(extensionTable))
            .setBaseSchema(extensionTable.getInitialSchema().toProto(typeProtoConverter))
            .setExtensionTable(extensionTableBuilder);

    extensionTable.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setRead(builder).build();
  }

  @Override
  public Rel visit(Project project) throws RuntimeException {
    var builder =
        ProjectRel.newBuilder()
            .setCommon(common(project))
            .setInput(toProto(project.getInput()))
            .addAllExpressions(
                project.getExpressions().stream()
                    .map(this::toProto)
                    .collect(java.util.stream.Collectors.toList()));

    project.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setProject(builder).build();
  }

  @Override
  public Rel visit(Sort sort) throws RuntimeException {
    var builder =
        SortRel.newBuilder()
            .setCommon(common(sort))
            .setInput(toProto(sort.getInput()))
            .addAllSorts(toProtoS(sort.getSortFields()));

    sort.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setSort(builder).build();
  }

  @Override
  public Rel visit(Cross cross) throws RuntimeException {
    var builder =
        CrossRel.newBuilder()
            .setCommon(common(cross))
            .setLeft(toProto(cross.getLeft()))
            .setRight(toProto(cross.getRight()));

    cross.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
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
                            .collect(java.util.stream.Collectors.toList()))
                    .build())
            .setBaseSchema(virtualTableScan.getInitialSchema().toProto(typeProtoConverter));

    virtualTableScan.getExtension().ifPresent(ae -> builder.setAdvancedExtension(ae.toProto()));
    return Rel.newBuilder().setRead(builder).build();
  }

  @Override
  public Rel visit(ExtensionLeaf extensionLeaf) throws RuntimeException {
    var builder =
        ExtensionLeafRel.newBuilder()
            .setCommon(common(extensionLeaf))
            .setDetail(extensionLeaf.getDetail().toProto());
    return Rel.newBuilder().setExtensionLeaf(builder).build();
  }

  @Override
  public Rel visit(ExtensionSingle extensionSingle) throws RuntimeException {
    var builder =
        ExtensionSingleRel.newBuilder()
            .setCommon(common(extensionSingle))
            .setInput(toProto(extensionSingle.getInput()))
            .setDetail(extensionSingle.getDetail().toProto());
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
            .setDetail(extensionMulti.getDetail().toProto());
    return Rel.newBuilder().setExtensionMulti(builder).build();
  }

  private RelCommon common(io.substrait.relation.Rel rel) {
    var builder = RelCommon.newBuilder();
    rel.getCommonExtension()
        .ifPresent(extension -> builder.setAdvancedExtension(extension.toProto()));

    var remap = rel.getRemap().orElse(null);
    if (remap != null) {
      builder.setEmit(RelCommon.Emit.newBuilder().addAllOutputMapping(remap.indices()));
    } else {
      builder.setDirect(RelCommon.Direct.getDefaultInstance());
    }
    return builder.build();
  }
}
