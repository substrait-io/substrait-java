package io.substrait.utils;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractUpdate;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Cross;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionDdl;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.ExtensionWrite;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.HasExtension;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.relation.physical.TopN;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * One sample relation per type that {@link RelVisitor} dispatches on, for round-trip tests that
 * have to cover every relation rather than a hand-picked few.
 *
 * <p>Coverage of per-relation data — an advanced extension, a hint, an emit mapping, a rel anchor —
 * is easy to add for some relations and forget for others, and a converter that reads a field back
 * but never writes it discards data silently on a read-modify-write round trip. Pairing these
 * samples with {@link #relTypes()} lets a test assert its own exhaustiveness, so a relation type
 * added without that coverage fails the build instead.
 *
 * <p>Samples use {@link StringHolder} for their extension detail objects, so a consumer needs the
 * {@code StringHolderHandling} converters to round-trip them.
 */
public class RelSamples {

  private static final TypeCreator R = TypeCreator.REQUIRED;

  private final SubstraitBuilder sb;

  private final SimpleExtension.ExtensionCollection extensions;

  private final StringHolder detail = new StringHolder("DETAIL");

  private final NamedScan emptyTable;

  private final Rel typedTable;

  private final NamedStruct boolSchema =
      NamedStruct.builder().addNames("KEY").struct(R.struct(R.BOOLEAN)).build();

  public RelSamples(SubstraitBuilder sb, SimpleExtension.ExtensionCollection extensions) {
    this.sb = sb;
    this.extensions = extensions;
    this.emptyTable =
        sb.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    this.typedTable =
        sb.namedScan(
            Collections.singletonList("test_table"),
            Arrays.asList("a", "b", "c"),
            Arrays.asList(R.I64, R.I16, R.I32));
  }

  /**
   * Every relation type {@link RelVisitor} dispatches on, derived from its {@code visit} overloads
   * so that it cannot fall behind the model.
   *
   * @return the relation types a visitor has to handle
   */
  public static List<Class<?>> relTypes() {
    return Arrays.stream(RelVisitor.class.getMethods())
        .filter(method -> "visit".equals(method.getName()))
        .map(Method::getParameterTypes)
        .map(parameterTypes -> parameterTypes[0])
        .collect(Collectors.toList());
  }

  /**
   * One sample per relation type, each carrying the {@link AdvancedExtension} slots its type
   * supports: the common-level one on every {@link Rel}, and the rel-level one on {@link
   * HasExtension} implementors.
   *
   * <p>Layer further per-relation data on top with the type-agnostic {@code Rel} copy methods
   * rather than re-listing the samples here.
   *
   * @param commonExtension the common-level extension to set on every sample
   * @param relExtension the rel-level extension to set on every {@link HasExtension} sample
   * @return the samples, keyed by relation type
   */
  public Map<Class<? extends Rel>, Rel> withAdvancedExtensions(
      AdvancedExtension commonExtension, AdvancedExtension relExtension) {
    Map<Class<? extends Rel>, Rel> samples = new LinkedHashMap<>();

    // Read rels
    samples.put(
        VirtualTableScan.class,
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct()))
            .addRows(Expression.NestedStruct.builder().fields(Collections.emptyList()).build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        LocalFiles.class,
        LocalFiles.builder()
            .initialSchema(
                NamedStruct.of(
                    Collections.emptyList(), Type.Struct.builder().nullable(false).build()))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        NamedScan.class,
        NamedScan.builder()
            .from(emptyTable)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        ExtensionTable.class,
        ExtensionTable.from(detail)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());

    // Logical rels
    samples.put(
        Filter.class,
        Filter.builder()
            .from(sb.filter(__ -> sb.bool(true), emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Fetch.class,
        Fetch.builder()
            .from(sb.fetch(1, 2, emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Aggregate.class,
        Aggregate.builder()
            .from(sb.aggregate(sb::grouping, __ -> Collections.emptyList(), emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Sort.class,
        Sort.builder()
            .from(sb.sort(__ -> Collections.emptyList(), emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Join.class,
        Join.builder()
            .from(sb.innerJoin(__ -> sb.bool(true), emptyTable, emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        LateralJoin.class,
        LateralJoin.builder()
            .left(emptyTable)
            .right(emptyTable)
            .condition(sb.bool(true))
            .joinType(Join.JoinType.INNER)
            .relAnchor(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Project.class,
        Project.builder()
            .from(sb.project(__ -> Collections.emptyList(), emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Expand.class,
        Expand.builder()
            .from(sb.expand(__ -> Collections.emptyList(), emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Set.class,
        Set.builder()
            .from(sb.set(Set.SetOp.UNION_ALL, emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Cross.class,
        Cross.builder()
            .from(sb.cross(emptyTable, emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        ConsistentPartitionWindow.class,
        ConsistentPartitionWindow.builder()
            .input(typedTable)
            // lead(a) OVER (PARTITION BY b ORDER BY c)
            .addWindowFunctions(
                ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                    .declaration(
                        extensions.getWindowFunction(
                            SimpleExtension.FunctionAnchor.of(
                                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any")))
                    .addArguments(sb.fieldReference(typedTable, 0))
                    .addOptions(FunctionOption.builder().name("option").addValues("VALUE1").build())
                    .outputType(R.I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(Expression.AggregationInvocation.ALL)
                    .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                    .upperBound(WindowBound.Following.CURRENT_ROW)
                    .boundsType(Expression.WindowBoundsType.RANGE)
                    .build())
            .addPartitionExpressions(sb.fieldReference(typedTable, 1))
            .addSorts(
                Expression.SortField.builder()
                    .expr(sb.fieldReference(typedTable, 2))
                    .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                    .build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());

    // Extension rels, which carry only a common-level extension
    samples.put(
        ExtensionLeaf.class, ExtensionLeaf.from(detail).commonExtension(commonExtension).build());
    samples.put(
        ExtensionSingle.class,
        ExtensionSingle.from(detail, emptyTable).commonExtension(commonExtension).build());
    samples.put(
        ExtensionMulti.class,
        ExtensionMulti.from(detail, emptyTable, emptyTable)
            .commonExtension(commonExtension)
            .build());

    // Write, DDL and update rels
    samples.put(
        NamedWrite.class,
        NamedWrite.builder()
            .addNames("CUSTOMER")
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .operation(AbstractWriteRel.WriteOp.INSERT)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .tableSchema(boolSchema)
            .input(VirtualTableScan.builder().initialSchema(boolSchema).build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        ExtensionWrite.class,
        ExtensionWrite.builder()
            .detail(detail)
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .operation(AbstractWriteRel.WriteOp.INSERT)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .tableSchema(boolSchema)
            .input(VirtualTableScan.builder().initialSchema(boolSchema).build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        NamedDdl.class,
        NamedDdl.builder()
            .addNames("CUSTOMER")
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .tableSchema(boolSchema)
            .viewDefinition(VirtualTableScan.builder().initialSchema(boolSchema).build())
            .tableDefaults(
                Expression.StructLiteral.builder()
                    .nullable(false)
                    .addFields(sb.bool(false))
                    .build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        ExtensionDdl.class,
        ExtensionDdl.builder()
            .detail(detail)
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .tableSchema(boolSchema)
            .viewDefinition(VirtualTableScan.builder().initialSchema(boolSchema).build())
            .tableDefaults(
                Expression.StructLiteral.builder()
                    .nullable(false)
                    .addFields(sb.bool(false))
                    .build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        NamedUpdate.class,
        NamedUpdate.builder()
            .addNames("CUSTOMER")
            .tableSchema(boolSchema)
            .condition(
                sb.equal(
                    FieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(R.BOOLEAN)
                        .build(),
                    sb.bool(true)))
            .addTransformations(
                AbstractUpdate.TransformExpression.builder()
                    .columnTarget(0)
                    .transformation(sb.bool(false))
                    .build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());

    // Physical rels
    samples.put(
        HashJoin.class,
        HashJoin.builder()
            .from(
                sb.hashJoin(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    HashJoin.JoinType.INNER,
                    emptyTable,
                    emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        MergeJoin.class,
        MergeJoin.builder()
            .from(
                sb.mergeJoin(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    MergeJoin.JoinType.INNER,
                    emptyTable,
                    emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        NestedLoopJoin.class,
        NestedLoopJoin.builder()
            .from(
                sb.nestedLoopJoin(
                    __ -> sb.bool(true), NestedLoopJoin.JoinType.INNER, emptyTable, emptyTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        TopN.class,
        TopN.builder()
            .input(typedTable)
            .addSortFields(
                Expression.SortField.builder()
                    .expr(sb.fieldReference(typedTable, 0))
                    .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                    .build())
            .count(sb.i64(10))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());

    // Exchange rels
    samples.put(
        ScatterExchange.class,
        ScatterExchange.builder()
            .input(typedTable)
            .addFields(sb.fieldReference(typedTable, 0))
            .partitionCount(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        SingleBucketExchange.class,
        SingleBucketExchange.builder()
            .input(typedTable)
            .expression(sb.fieldReference(typedTable, 0))
            .partitionCount(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        MultiBucketExchange.class,
        MultiBucketExchange.builder()
            .input(typedTable)
            .expression(sb.fieldReference(typedTable, 0))
            .constrainedToCount(true)
            .partitionCount(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        RoundRobinExchange.class,
        RoundRobinExchange.builder()
            .input(typedTable)
            .exact(true)
            .partitionCount(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        BroadcastExchange.class,
        BroadcastExchange.builder()
            .input(typedTable)
            .partitionCount(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());

    return samples;
  }
}
