package io.substrait.utils;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.WindowBound;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
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
import io.substrait.type.TypeCreator;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 *
 * <p>Their record types are deliberately non-empty: a consumer derives per-relation data from a
 * sample's fields — an emit mapping most of all — so a sample over an empty schema quietly weakens
 * the assertion it feeds.
 */
public class RelSamples {

  private static final TypeCreator R = TypeCreator.REQUIRED;

  private final SubstraitBuilder sb;

  private final SimpleExtension.ExtensionCollection extensions;

  private final StringHolder detail = new StringHolder("DETAIL");

  private final NamedScan left;

  private final NamedScan right;

  private final NamedStruct schema =
      NamedStruct.of(Arrays.asList("a", "b"), R.struct(R.I64, R.STRING));

  public RelSamples(SubstraitBuilder sb, SimpleExtension.ExtensionCollection extensions) {
    this.sb = sb;
    this.extensions = extensions;
    this.left =
        sb.namedScan(
            Arrays.asList("left_table"), Arrays.asList("a", "b"), Arrays.asList(R.I64, R.STRING));
    this.right =
        sb.namedScan(
            Arrays.asList("right_table"), Arrays.asList("c", "d"), Arrays.asList(R.I64, R.STRING));
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
   * One sample per relation type, carrying no {@link AdvancedExtension}s.
   *
   * @return the samples, keyed by relation type
   */
  public Map<Class<? extends Rel>, Rel> samples() {
    Map<Class<? extends Rel>, Rel> samples = new LinkedHashMap<>();

    // Read relations. The NamedScan sample is a copy of left rather than left itself, which is the
    // input of most other samples and has to stay free of the data under test.
    samples.put(NamedScan.class, NamedScan.builder().from(left).build());
    samples.put(
        VirtualTableScan.class,
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(false, 1))
                    .addFields(ExpressionCreator.string(false, "one"))
                    .build())
            .build());
    samples.put(LocalFiles.class, LocalFiles.builder().initialSchema(schema).build());
    // A real schema rather than the detail's empty one, so data derived from this sample's fields
    // is
    // non-empty here too. from(detail) seeds the schema, so the override has to follow it.
    samples.put(ExtensionTable.class, ExtensionTable.from(detail).initialSchema(schema).build());

    // Logical relations
    samples.put(
        Filter.class,
        Filter.builder()
            .from(sb.filter(input -> sb.equal(sb.fieldReference(input, 0), sb.i64(1)), left))
            .build());
    samples.put(Fetch.class, Fetch.builder().from(sb.limit(10, left)).build());
    samples.put(
        Project.class,
        Project.builder()
            .from(sb.project(input -> Arrays.asList(sb.fieldReference(input, 0)), left))
            .build());
    samples.put(
        Aggregate.class,
        Aggregate.builder()
            .from(
                sb.aggregate(
                    input -> sb.grouping(input, 0),
                    input -> Arrays.asList(sb.count(input, 0)),
                    left))
            .build());
    samples.put(
        Sort.class, Sort.builder().from(sb.sort(input -> sb.sortFields(input, 0), left)).build());
    samples.put(
        Join.class,
        Join.builder()
            .from(
                sb.innerJoin(
                    inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 2)),
                    left,
                    right))
            .build());
    samples.put(Cross.class, Cross.builder().from(sb.cross(left, right)).build());
    samples.put(Set.class, Set.builder().from(sb.set(Set.SetOp.UNION_ALL, left, right)).build());
    samples.put(
        Expand.class,
        Expand.builder()
            .from(
                sb.expand(
                    input ->
                        Arrays.asList(
                            Expand.ConsistentField.builder()
                                .expression(sb.fieldReference(input, 0))
                                .build()),
                    left))
            .build());
    // The anchor is set on the builder rather than layered on, because LateralJoin checks for it on
    // every copy. A consumer stamping its own anchor has to leave this one alone: the right input
    // resolves its outer references against it.
    samples.put(
        LateralJoin.class,
        LateralJoin.builder()
            .left(left)
            .right(right)
            .joinType(Join.JoinType.INNER)
            .relAnchor(7)
            .build());
    samples.put(ConsistentPartitionWindow.class, consistentPartitionWindow());

    // Physical relations
    samples.put(
        TopN.class,
        TopN.builder().from(sb.topN(input -> sb.sortFields(input, 0), 0, 10, left)).build());
    samples.put(
        HashJoin.class,
        HashJoin.builder()
            .from(
                sb.hashJoin(
                    Arrays.asList(0), Arrays.asList(0), HashJoin.JoinType.INNER, left, right))
            .build());
    samples.put(
        MergeJoin.class,
        MergeJoin.builder()
            .from(
                sb.mergeJoin(
                    Arrays.asList(0), Arrays.asList(0), MergeJoin.JoinType.INNER, left, right))
            .build());
    samples.put(
        NestedLoopJoin.class,
        NestedLoopJoin.builder()
            .from(
                sb.nestedLoopJoin(
                    inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 2)),
                    NestedLoopJoin.JoinType.INNER,
                    left,
                    right))
            .build());
    samples.put(
        BroadcastExchange.class, BroadcastExchange.builder().input(left).partitionCount(1).build());
    samples.put(
        RoundRobinExchange.class,
        RoundRobinExchange.builder().input(left).exact(true).partitionCount(1).build());
    samples.put(
        ScatterExchange.class,
        ScatterExchange.builder()
            .input(left)
            .addFields(sb.fieldReference(left, 0))
            .partitionCount(1)
            .build());
    samples.put(
        SingleBucketExchange.class,
        SingleBucketExchange.builder()
            .input(left)
            .expression(sb.fieldReference(left, 0))
            .partitionCount(1)
            .build());
    samples.put(
        MultiBucketExchange.class,
        MultiBucketExchange.builder()
            .input(left)
            .expression(sb.fieldReference(left, 0))
            .constrainedToCount(true)
            .partitionCount(1)
            .build());

    // Write, DDL and update relations
    samples.put(
        NamedWrite.class,
        NamedWrite.builder()
            .from(
                sb.namedWrite(
                    Arrays.asList("target_table"),
                    Arrays.asList("a", "b"),
                    AbstractWriteRel.WriteOp.INSERT,
                    AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS,
                    AbstractWriteRel.OutputMode.NO_OUTPUT,
                    left))
            .build());
    samples.put(
        ExtensionWrite.class,
        ExtensionWrite.builder()
            .input(left)
            .detail(detail)
            .tableSchema(schema)
            .operation(ExtensionWrite.WriteOp.INSERT)
            .createMode(ExtensionWrite.CreateMode.APPEND_IF_EXISTS)
            .outputMode(ExtensionWrite.OutputMode.NO_OUTPUT)
            .build());
    // CREATE VIEW rather than CREATE TABLE, so that this sample carries a view definition: writing
    // one is per-relation code and the DDL round-trip test only covers the extension variant.
    samples.put(
        NamedDdl.class,
        NamedDdl.builder()
            .names(Arrays.asList("target_table"))
            .tableSchema(schema)
            .tableDefaults(tableDefaults())
            .operation(NamedDdl.DdlOp.CREATE)
            .object(NamedDdl.DdlObject.VIEW)
            .viewDefinition(left)
            .build());
    samples.put(
        ExtensionDdl.class,
        ExtensionDdl.builder()
            .detail(detail)
            .tableSchema(schema)
            .tableDefaults(tableDefaults())
            .operation(ExtensionDdl.DdlOp.ALTER)
            .object(ExtensionDdl.DdlObject.TABLE)
            .build());
    samples.put(
        NamedUpdate.class,
        NamedUpdate.builder()
            .from(
                sb.namedUpdate(
                    Arrays.asList("target_table"),
                    Arrays.asList("a"),
                    Arrays.asList(
                        NamedUpdate.TransformExpression.builder()
                            .columnTarget(0)
                            .transformation(sb.i64(1))
                            .build()),
                    sb.bool(true),
                    false))
            .build());

    // Extension relations. These do not implement HasExtension, so they carry no rel-level
    // extension. Their record type is derived from the detail on both the write and the read side,
    // and StringHolder derives an empty struct — so unlike every other sample, data a consumer
    // derives from their fields is empty. That is a property of extension relations, not something
    // to work around by giving StringHolder a record type.
    samples.put(ExtensionLeaf.class, ExtensionLeaf.from(detail).build());
    samples.put(ExtensionSingle.class, ExtensionSingle.from(detail, left).build());
    samples.put(
        ExtensionMulti.class, ExtensionMulti.from(detail, Arrays.asList(left, right)).build());

    return samples;
  }

  /**
   * The same samples, each carrying the {@link AdvancedExtension} slots its type supports: the
   * common-level one on every {@link Rel}, and the rel-level one on {@link HasExtension}
   * implementors.
   *
   * @param commonExtension the common-level extension every sample carries
   * @param relExtension the rel-level extension every {@link HasExtension} sample carries
   * @return the samples, keyed by relation type
   */
  public Map<Class<? extends Rel>, Rel> withAdvancedExtensions(
      AdvancedExtension commonExtension, AdvancedExtension relExtension) {
    Map<Class<? extends Rel>, Rel> stamped = new LinkedHashMap<>();
    samples()
        .forEach(
            (relType, rel) -> {
              // Both slots are layered on rather than set on each builder, so a relation added
              // to the samples above carries them without per-relation code. The cast is what
              // HasExtension#withExtension returning HasExtension costs, and never fails for
              // the Immutables-backed samples here: their generated override returns the
              // concrete relation.
              Rel extended =
                  rel instanceof HasExtension
                      ? (Rel) ((HasExtension) rel).withExtension(Optional.of(relExtension))
                      : rel;
              stamped.put(relType, extended.withCommonExtension(Optional.of(commonExtension)));
            });
    return stamped;
  }

  private Expression.StructLiteral tableDefaults() {
    return ExpressionCreator.struct(
        false, ExpressionCreator.i64(false, 1), ExpressionCreator.string(false, "one"));
  }

  private Rel consistentPartitionWindow() {
    SimpleExtension.WindowFunctionVariant lead =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    return ConsistentPartitionWindow.builder()
        .input(left)
        .addWindowFunctions(
            ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                .declaration(lead)
                .arguments(Arrays.asList(sb.fieldReference(left, 0)))
                .outputType(R.I64)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                .invocation(Expression.AggregationInvocation.ALL)
                .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                .upperBound(WindowBound.Following.CURRENT_ROW)
                .boundsType(Expression.WindowBoundsType.RANGE)
                .build())
        .addPartitionExpressions(sb.fieldReference(left, 1))
        .sorts(sb.sortFields(left, 0))
        .build();
  }
}
