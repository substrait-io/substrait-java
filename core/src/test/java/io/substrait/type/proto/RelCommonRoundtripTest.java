package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.WindowBound;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.hint.Hint;
import io.substrait.proto.RelCommon;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionDdl;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.ExtensionWrite;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.SingleInputRel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that every relation type carries the {@link io.substrait.proto.RelCommon} data the POJO
 * model represents — the {@link Rel#getHint() hint}, {@link Rel#getRemap() emit mapping}, {@link
 * Rel#getCommonExtension() common extension} and {@link Rel#getRelAnchor() rel anchor} — through a
 * POJO → proto → POJO round trip.
 *
 * <p>{@link #everyRelationTypeIsCovered()} keeps the sample set exhaustive: it fails when a
 * relation is added to {@link RelVisitor} without a sample here, so a new relation cannot silently
 * miss its {@code RelCommon} wiring.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RelCommonRoundtripTest extends TestBase {

  /**
   * Relations whose protobuf message has no {@code common} field and which therefore cannot carry
   * any {@code RelCommon} data at all (spec v0.99.0).
   */
  static final java.util.Set<Class<? extends Rel>> WITHOUT_REL_COMMON =
      Collections.singleton(NamedUpdate.class);

  static final Hint HINT =
      Hint.builder()
          .extension(AdvancedExtension.builder().build())
          .alias("an_alias")
          .addAllOutputNames(Arrays.asList("name1", "name2"))
          .stats(Hint.Stats.builder().rowCount(42).recordSize(13).build())
          .runtimeConstraint(Hint.RuntimeConstraint.builder().build())
          .addLoadedComputations(
              Hint.LoadedComputation.builder()
                  .computationId(1)
                  .computationType(Hint.ComputationType.COMPUTATION_TYPE_HASHTABLE)
                  .build())
          .addSavedComputations(
              Hint.SavedComputation.builder()
                  .computationId(2)
                  .computationType(Hint.ComputationType.COMPUTATION_TYPE_BLOOM_FILTER)
                  .build())
          .build();

  static final AdvancedExtension COMMON_EXTENSION = AdvancedExtension.builder().build();

  static final int REL_ANCHOR = 11;

  final Rel left =
      sb.namedScan(
          Arrays.asList("left_table"), Arrays.asList("a", "b"), Arrays.asList(R.I64, R.STRING));

  final Rel right =
      sb.namedScan(
          Arrays.asList("right_table"), Arrays.asList("c", "d"), Arrays.asList(R.I64, R.STRING));

  final NamedStruct schema = NamedStruct.of(Arrays.asList("a", "b"), R.struct(R.I64, R.STRING));

  @ParameterizedTest(name = "{0}")
  @MethodSource("samples")
  void relCommonRoundtrips(String relationType, Rel rel) {
    if (WITHOUT_REL_COMMON.contains(relationType(rel))) {
      // The message has nowhere to carry the data, so serialization must fail loudly instead of
      // emitting a plan that silently lost it (an emit mapping even changes the record type).
      // Each field is rejected on its own, so none of them can start being dropped silently.
      for (Rel sample :
          Arrays.asList(
              rel.withRemap(Optional.of(reversedRemap(rel))),
              rel.withHint(Optional.of(HINT)),
              rel.withRelAnchor(Optional.of(REL_ANCHOR)),
              rel.withCommonExtension(Optional.of(COMMON_EXTENSION)),
              withRelCommon(rel))) {
        assertThrows(
            UnsupportedOperationException.class,
            () -> relProtoConverter.toProto(sample),
            relationType);
      }
      // Without any RelCommon data the relation still round-trips.
      verifyRoundTrip(rel);
      return;
    }
    verifyRoundTrip(withRelCommon(rel));
  }

  @Test
  void everyRelationTypeIsCovered() {
    java.util.Set<Class<?>> visitable =
        Arrays.stream(RelVisitor.class.getDeclaredMethods())
            .filter(method -> "visit".equals(method.getName()))
            .map(method -> method.getParameterTypes()[0])
            .collect(Collectors.toCollection(HashSet::new));

    java.util.Set<Class<?>> sampled =
        allRelationTypes().stream()
            .map(RelCommonRoundtripTest::relationType)
            .collect(Collectors.toCollection(HashSet::new));

    assertEquals(visitable, sampled, "every relation type needs a RelCommon round-trip sample");
  }

  @Test
  void samplesCarryTheDataUnderTest() {
    // Guards the round-trip assertions above against silently asserting on empty optionals.
    for (Rel rel : allRelationTypes()) {
      Rel sample = withRelCommon(rel);
      assertEquals(Optional.of(HINT), sample.getHint());
      assertEquals(Optional.of(COMMON_EXTENSION), sample.getCommonExtension());
      assertTrue(sample.getRemap().isPresent());
      assertTrue(sample.getRelAnchor().isPresent());
    }
  }

  @Test
  void applyRelCommonLeavesACustomRelAloneWhenThereIsNothingToApply() {
    // A custom, non-Immutables Rel inherits Rel's throwing withXxx defaults. applyRelCommon is a
    // documented extension point that every newXxx must route through, so it must not fail on a
    // relation whose common message carries no data.
    ApplyRelCommonConverter converter = new ApplyRelCommonConverter(functionCollector, extensions);
    Rel custom = new PassThroughRel(left);

    assertEquals(custom, converter.applyRelCommon(custom, RelCommon.getDefaultInstance()));
    assertEquals(
        custom,
        converter.applyRelCommon(
            custom,
            RelCommon.newBuilder().setDirect(RelCommon.Direct.getDefaultInstance()).build()));
  }

  /** Exposes {@link ProtoRelConverter#applyRelCommon} so the test can call it directly. */
  static final class ApplyRelCommonConverter extends ProtoRelConverter {
    ApplyRelCommonConverter(
        ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
      super(lookup, extensions);
    }

    @Override
    public <R extends Rel> R applyRelCommon(R rel, RelCommon relCommon) {
      return super.applyRelCommon(rel, relCommon);
    }
  }

  /** A minimal hand-written {@link Rel} that inherits {@code Rel}'s throwing {@code withXxx}. */
  static final class PassThroughRel extends SingleInputRel {
    private final Rel input;

    PassThroughRel(Rel input) {
      this.input = input;
    }

    @Override
    public Rel getInput() {
      return input;
    }

    @Override
    protected Type.Struct deriveRecordType() {
      return input.getRecordType();
    }

    @Override
    public Optional<Rel.Remap> getRemap() {
      return Optional.empty();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
      return Optional.empty();
    }

    @Override
    public Optional<Hint> getHint() {
      return Optional.empty();
    }

    @Override
    public Optional<Integer> getRelAnchor() {
      return Optional.empty();
    }

    @Override
    public <O, C extends VisitationContext, E extends Exception> O accept(
        RelVisitor<O, C, E> visitor, C context) {
      throw new UnsupportedOperationException("not visitable");
    }
  }

  Stream<Arguments> samples() {
    return allRelationTypes().stream()
        .map(rel -> Arguments.of(relationType(rel).getSimpleName(), rel));
  }

  /**
   * Stamps the full set of {@code RelCommon} data onto {@code rel} using the type-agnostic {@code
   * Rel.withXxx} copy methods. An existing rel anchor is kept because a {@link LateralJoin}'s right
   * input resolves its outer references against it.
   */
  static Rel withRelCommon(Rel rel) {
    Optional<Integer> relAnchor =
        rel.getRelAnchor().isPresent() ? rel.getRelAnchor() : Optional.of(REL_ANCHOR);
    return rel.withHint(Optional.of(HINT))
        .withCommonExtension(Optional.of(COMMON_EXTENSION))
        .withRemap(Optional.of(reversedRemap(rel)))
        .withRelAnchor(relAnchor);
  }

  /**
   * An emit mapping that reverses {@code rel}'s fields. Reversing rather than using the identity
   * makes the assertions sensitive to the order and count of {@code RelCommon.Emit.output_mapping},
   * not just to its presence.
   */
  static Rel.Remap reversedRemap(Rel rel) {
    List<Integer> indices = new ArrayList<>();
    for (int i = rel.getRecordType().fields().size() - 1; i >= 0; i--) {
      indices.add(i);
    }
    return Rel.Remap.of(indices);
  }

  /** The declared relation type of {@code rel}, i.e. the type its generated Immutable extends. */
  static Class<?> relationType(Rel rel) {
    return rel.getClass().getSuperclass();
  }

  /** One sample of every relation type reachable through {@link RelVisitor}. */
  List<Rel> allRelationTypes() {
    List<Rel> rels = new ArrayList<>();

    // Read relations
    rels.add(left);
    rels.add(
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(false, 1))
                    .addFields(ExpressionCreator.string(false, "one"))
                    .build())
            .build());
    rels.add(LocalFiles.builder().initialSchema(schema).build());
    // A real schema rather than EmptyDetail's empty one, so the reversed emit mapping is non-empty
    // and the assertions cover the order of RelCommon.Emit.output_mapping for this relation too.
    rels.add(ExtensionTable.from(new EmptyDetail()).initialSchema(schema).build());

    // Logical relations
    rels.add(sb.filter(input -> sb.equal(sb.fieldReference(input, 0), sb.i64(1)), left));
    rels.add(sb.limit(10, left));
    rels.add(sb.project(input -> Arrays.asList(sb.fieldReference(input, 0)), left));
    rels.add(
        sb.aggregate(
            input -> sb.grouping(input, 0), input -> Arrays.asList(sb.count(input, 0)), left));
    rels.add(sb.sort(input -> sb.sortFields(input, 0), left));
    rels.add(
        sb.innerJoin(
            inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 2)),
            left,
            right));
    rels.add(sb.cross(left, right));
    rels.add(sb.set(Set.SetOp.UNION_ALL, left, right));
    rels.add(
        sb.expand(
            input ->
                Arrays.asList(
                    Expand.ConsistentField.builder()
                        .expression(sb.fieldReference(input, 0))
                        .build()),
            left));
    rels.add(
        LateralJoin.builder()
            .left(left)
            .right(right)
            .joinType(Join.JoinType.INNER)
            .relAnchor(7)
            .build());
    rels.add(consistentPartitionWindow());

    // Physical relations
    rels.add(sb.topN(input -> sb.sortFields(input, 0), 0, 10, left));
    rels.add(sb.hashJoin(Arrays.asList(0), Arrays.asList(0), HashJoin.JoinType.INNER, left, right));
    rels.add(
        sb.mergeJoin(Arrays.asList(0), Arrays.asList(0), MergeJoin.JoinType.INNER, left, right));
    rels.add(
        sb.nestedLoopJoin(
            inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 2)),
            NestedLoopJoin.JoinType.INNER,
            left,
            right));
    rels.add(BroadcastExchange.builder().input(left).partitionCount(1).build());
    rels.add(RoundRobinExchange.builder().input(left).exact(true).partitionCount(1).build());
    rels.add(
        ScatterExchange.builder()
            .input(left)
            .addFields(sb.fieldReference(left, 0))
            .partitionCount(1)
            .build());
    rels.add(
        SingleBucketExchange.builder()
            .input(left)
            .expression(sb.fieldReference(left, 0))
            .partitionCount(1)
            .build());
    rels.add(
        MultiBucketExchange.builder()
            .input(left)
            .expression(sb.fieldReference(left, 0))
            .constrainedToCount(true)
            .partitionCount(1)
            .build());

    // Write, DDL and update relations
    rels.add(
        sb.namedWrite(
            Arrays.asList("target_table"),
            Arrays.asList("a", "b"),
            AbstractWriteRel.WriteOp.INSERT,
            AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS,
            AbstractWriteRel.OutputMode.NO_OUTPUT,
            left));
    rels.add(
        ExtensionWrite.builder()
            .input(left)
            .detail(new EmptyDetail())
            .tableSchema(schema)
            .operation(ExtensionWrite.WriteOp.INSERT)
            .createMode(ExtensionWrite.CreateMode.APPEND_IF_EXISTS)
            .outputMode(ExtensionWrite.OutputMode.NO_OUTPUT)
            .build());
    rels.add(
        NamedDdl.builder()
            .names(Arrays.asList("target_table"))
            .tableSchema(schema)
            .tableDefaults(tableDefaults())
            .operation(NamedDdl.DdlOp.CREATE)
            .object(NamedDdl.DdlObject.TABLE)
            .build());
    rels.add(
        ExtensionDdl.builder()
            .detail(new EmptyDetail())
            .tableSchema(schema)
            .tableDefaults(tableDefaults())
            .operation(ExtensionDdl.DdlOp.ALTER)
            .object(ExtensionDdl.DdlObject.TABLE)
            .build());
    rels.add(
        sb.namedUpdate(
            Arrays.asList("target_table"),
            Arrays.asList("a"),
            Arrays.asList(
                NamedUpdate.TransformExpression.builder()
                    .columnTarget(0)
                    .transformation(sb.i64(1))
                    .build()),
            sb.bool(true),
            false));

    // Extension relations
    rels.add(ExtensionLeaf.from(new EmptyDetail()).build());
    rels.add(ExtensionSingle.from(new EmptyDetail(), left).build());
    rels.add(ExtensionMulti.from(new EmptyDetail(), Arrays.asList(left, right)).build());

    return rels;
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
