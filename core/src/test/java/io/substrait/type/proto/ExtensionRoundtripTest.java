package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.substrait.TestBase;
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
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
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
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.utils.StringHolderHandlingRelProtoConverter;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Verify that the various extension types in {@link io.substrait.relation.Extension} roundtrip
 * correctly.
 *
 * <p>Every relation type that {@link RelVisitor} knows about and that implements {@link
 * HasExtension} must appear in {@link #relSamples()}; {@link
 * #everyRelWithAnAdvancedExtensionIsCovered()} fails the build otherwise. A converter that reads a
 * rel-level {@code advanced_extension} back but never writes it silently discards third-party
 * extensions on a read-modify-write roundtrip, which is invisible without per-rel coverage.
 */
class ExtensionRoundtripTest extends TestBase {

  final ProtoRelConverter protoRelConverter =
      new StringHolderHandlingProtoRelConverter(functionCollector, extensions);

  final NamedScan commonTable =
      sb.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  final Rel typedTable =
      sb.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("a", "b", "c"),
          Arrays.asList(R.I64, R.I16, R.I32));

  final NamedStruct boolSchema =
      NamedStruct.builder()
          .addNames("KEY")
          .struct(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.BOOLEAN))
          .build();

  final AdvancedExtension commonExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("COMMON ENHANCEMENT"))
          .addOptimizations(new StringHolder("COMMON OPTIMIZATION"))
          .build();

  final StringHolder detail = new StringHolder("DETAIL");

  final AdvancedExtension relExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("REL ENHANCEMENT"))
          .addOptimizations(new StringHolder("REL OPTIMIZATION"))
          .build();

  @Override
  protected void verifyRoundTrip(Rel rel) {
    RelProtoConverter relProtoConverter =
        new StringHolderHandlingRelProtoConverter(functionCollector);
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  /**
   * One sample per relation type, carrying a common-level {@link AdvancedExtension} and — for the
   * {@link HasExtension} types — a rel-level one as well.
   */
  Map<Class<? extends Rel>, Rel> relSamples() {
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
            .from(commonTable)
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
            .from(sb.filter(__ -> sb.bool(true), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Fetch.class,
        Fetch.builder()
            .from(sb.fetch(1, 2, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Aggregate.class,
        Aggregate.builder()
            .from(sb.aggregate(sb::grouping, __ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Sort.class,
        Sort.builder()
            .from(sb.sort(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Join.class,
        Join.builder()
            .from(sb.innerJoin(__ -> sb.bool(true), commonTable, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        LateralJoin.class,
        LateralJoin.builder()
            .left(commonTable)
            .right(commonTable)
            .condition(sb.bool(true))
            .joinType(Join.JoinType.INNER)
            .relAnchor(1)
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Project.class,
        Project.builder()
            .from(sb.project(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Expand.class,
        Expand.builder()
            .from(sb.expand(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Set.class,
        Set.builder()
            .from(sb.set(Set.SetOp.UNION_ALL, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        Cross.class,
        Cross.builder()
            .from(sb.cross(commonTable, commonTable))
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
        ExtensionSingle.from(detail, commonTable).commonExtension(commonExtension).build());
    samples.put(
        ExtensionMulti.class,
        ExtensionMulti.from(detail, commonTable, commonTable)
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
    // UpdateRel is the one rel message with no RelCommon, so a NamedUpdate can only carry the
    // rel-level extension — a common-level one has nowhere to be written.
    samples.put(
        NamedUpdate.class,
        NamedUpdate.builder()
            .addNames("CUSTOMER")
            .tableSchema(boolSchema)
            .condition(
                sb.equal(
                    FieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(TypeCreator.REQUIRED.BOOLEAN)
                        .build(),
                    sb.bool(true)))
            .addTransformations(
                AbstractUpdate.TransformExpression.builder()
                    .columnTarget(0)
                    .transformation(sb.bool(false))
                    .build())
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
                    commonTable,
                    commonTable))
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
                    commonTable,
                    commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build());
    samples.put(
        NestedLoopJoin.class,
        NestedLoopJoin.builder()
            .from(
                sb.nestedLoopJoin(
                    __ -> sb.bool(true), NestedLoopJoin.JoinType.INNER, commonTable, commonTable))
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

  @TestFactory
  Stream<DynamicTest> relExtensions() {
    return relSamples().entrySet().stream()
        .map(
            sample ->
                DynamicTest.dynamicTest(
                    sample.getKey().getSimpleName(), () -> verifyRoundTrip(sample.getValue())));
  }

  /**
   * Guards {@link #relSamples()} against drift: a relation type that can hold a rel-level {@link
   * AdvancedExtension} but has no sample here would let a missing converter side go unnoticed.
   */
  @Test
  void everyRelWithAnAdvancedExtensionIsCovered() {
    List<Class<?>> relTypes =
        Arrays.stream(RelVisitor.class.getMethods())
            .filter(method -> "visit".equals(method.getName()))
            .map(Method::getParameterTypes)
            .map(parameterTypes -> parameterTypes[0])
            .filter(HasExtension.class::isAssignableFrom)
            .collect(Collectors.toList());
    // Without this the check below passes vacuously if the reflection above stops finding rels.
    assertFalse(relTypes.isEmpty(), "No HasExtension relation types found on RelVisitor");

    Map<Class<? extends Rel>, Rel> samples = relSamples();
    List<String> uncovered =
        relTypes.stream()
            .filter(relType -> !samples.containsKey(relType))
            .map(Class::getSimpleName)
            .sorted()
            .collect(Collectors.toList());

    assertEquals(
        Collections.emptyList(),
        uncovered,
        "Relation types implementing HasExtension without an advanced extension roundtrip sample");
  }

  @Nested
  class ExtensionThroughExpression {
    // There are some expression that can contains relations.
    // Check that custom extensions in these relations can be handled.

    Rel baseTable =
        sb.namedScan(
            Stream.of("test_table").collect(Collectors.toList()),
            Stream.of("test_column").collect(Collectors.toList()),
            Stream.of(TypeCreator.REQUIRED.I64).collect(Collectors.toList()));
    Rel relWithEnhancement =
        Project.builder()
            .from(sb.project(input -> Collections.emptyList(), baseTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();

    @Test
    void scalarSubquery() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.ScalarSubquery.builder()
                              .input(relWithEnhancement)
                              .type(TypeCreator.REQUIRED.I64)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);

      verifyRoundTrip(rel);
    }

    @Test
    void inPredicate() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.InPredicate.builder()
                              .needles(Collections.emptyList())
                              .haystack(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }

    @Test
    void setPredicate() {
      Project rel =
          sb.project(
              input ->
                  Stream.of(
                          Expression.SetPredicate.builder()
                              .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
                              .tuples(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }
  }
}
