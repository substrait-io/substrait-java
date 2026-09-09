package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FieldReference.ListElement;
import io.substrait.expression.FieldReference.MapKey;
import io.substrait.expression.FieldReference.StructField;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.relation.physical.TopN;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import io.substrait.utils.RelSamples;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Covers the re-derivation of the type cached on a {@link FieldReference} when a copy-on-write
 * visitation replaces a relation with one that emits a different record type.
 */
class RelCopyOnWriteVisitorTest extends TestBase {

  private Rel scan(String table, Type... columnTypes) {
    return sb.namedScan(
        Arrays.asList(table),
        Arrays.asList("a", "b", "c").subList(0, columnTypes.length),
        Arrays.asList(columnTypes));
  }

  /**
   * Rewrites the given relation tree, replacing every {@link NamedScan} in it with one that reads
   * the given column types instead of the ones it was built with.
   */
  private static Rel replaceScanTypes(Rel rel, Type... columnTypes) {
    return rel.accept(scanTypeReplacer(null, columnTypes), EmptyVisitationContext.INSTANCE)
        .orElseThrow(() -> new AssertionError("expected the visitation to replace the scan"));
  }

  /** The same, replacing only the {@link NamedScan} that reads the given table. */
  private static Rel replaceScanTypesOf(Rel rel, String table, Type... columnTypes) {
    return rel.accept(scanTypeReplacer(table, columnTypes), EmptyVisitationContext.INSTANCE)
        .orElseThrow(() -> new AssertionError("expected the visitation to replace the scan"));
  }

  private static RelCopyOnWriteVisitor<RuntimeException> scanTypeReplacer(
      String table, Type... columnTypes) {
    return new RelCopyOnWriteVisitor<RuntimeException>() {
      @Override
      public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
        if (table != null && !namedScan.getNames().equals(Arrays.asList(table))) {
          return Optional.empty();
        }
        return Optional.of(
            NamedScan.builder()
                .from(namedScan)
                .initialSchema(
                    NamedStruct.of(namedScan.getInitialSchema().names(), R.struct(columnTypes)))
                .build());
      }
    };
  }

  /**
   * Projects a reference that navigates the given segments into the single column of a scan of
   * {@code columnType}, then rewrites the plan with that column replaced by {@code
   * replacementType}, and returns the resulting reference. The segments are given outermost first,
   * the order they are navigated in.
   */
  private FieldReference rewriteNestedReference(
      Type columnType, Type replacementType, FieldReference.ReferenceSegment... segments) {
    Rel input = scan("t", columnType);
    FieldReference navigated = sb.fieldReference(input, 0);
    for (FieldReference.ReferenceSegment segment : segments) {
      navigated = segment.apply(navigated);
    }
    FieldReference reference = navigated;
    Rel plan = sb.project(in -> Arrays.asList(reference), input);

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, replacementType));
    return assertInstanceOf(FieldReference.class, rewritten.getExpressions().get(0));
  }

  private static List<Type> argumentTypes(List<FunctionArg> arguments) {
    return arguments.stream()
        .map(argument -> ((Expression) argument).getType())
        .collect(Collectors.toList());
  }

  private static List<Type> argumentTypes(Expression expression) {
    return argumentTypes(
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, expression).arguments());
  }

  @Test
  void projectExpression() {
    Rel plan = sb.project(input -> sb.fieldReferences(input, 0), scan("t", R.I64));

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getExpressions().get(0).getType());
    // A project derives its record type from its expressions, so it follows the reference.
    assertEquals(R.struct(N.I64, N.I64), rewritten.getRecordType());
  }

  @Test
  void filterCondition() {
    Rel plan = sb.filter(input -> sb.fieldReference(input, 0), scan("t", R.I64));

    Filter rewritten = assertInstanceOf(Filter.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getCondition().getType());
  }

  @Test
  void sortField() {
    Rel plan = sb.sort(input -> sb.sortFields(input, 0), scan("t", R.I64));

    Sort rewritten = assertInstanceOf(Sort.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getSortFields().get(0).expr().getType());
  }

  @Test
  void aggregateGroupingAndMeasure() {
    Rel input = scan("t", R.I64);
    Rel plan =
        Aggregate.builder()
            .input(input)
            .addGroupings(sb.grouping(input, 0))
            .addMeasures(sb.max(sb.fieldReference(input, 0)))
            .build();

    Aggregate rewritten = assertInstanceOf(Aggregate.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getGroupings().get(0).getExpressions().get(0).getType());
    assertEquals(
        Arrays.asList(N.I64),
        argumentTypes(rewritten.getMeasures().get(0).getFunction().arguments()));
  }

  @Test
  void joinConditionSpansBothInputs() {
    Rel plan =
        sb.innerJoin(
            inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 1)),
            scan("l", R.I64),
            scan("r", R.I64));

    Join rewritten = assertInstanceOf(Join.class, replaceScanTypes(plan, N.I64));
    assertEquals(
        Arrays.asList(N.I64, N.I64), argumentTypes(rewritten.getCondition().orElseThrow()));
  }

  @Test
  void exchangeFields() {
    Rel input = scan("t", R.I64);
    Rel plan =
        ScatterExchange.builder()
            .input(input)
            .partitionCount(2)
            .addFields(sb.fieldReference(input, 0))
            .build();

    ScatterExchange rewritten =
        assertInstanceOf(ScatterExchange.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getFields().get(0).getType());
  }

  @Test
  void windowRelationInputIsTraversedAndReferencesRetyped() {
    SimpleExtension.WindowFunctionVariant lead =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = scan("t", R.I64);
    Rel plan =
        ConsistentPartitionWindow.builder()
            .input(input)
            .addPartitionExpressions(sb.fieldReference(input, 0))
            .sorts(sb.sortFields(input, 0))
            .addWindowFunctions(
                ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                    .declaration(lead)
                    .addArguments(sb.fieldReference(input, 0))
                    .outputType(R.I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(Expression.AggregationInvocation.ALL)
                    .boundsType(Expression.WindowBoundsType.RANGE)
                    .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                    .upperBound(WindowBound.Following.CURRENT_ROW)
                    .build())
            .build();

    ConsistentPartitionWindow rewritten =
        assertInstanceOf(ConsistentPartitionWindow.class, replaceScanTypes(plan, N.I64));
    // The input of a window relation used not to be traversed at all.
    assertEquals(R.struct(N.I64), rewritten.getInput().getRecordType());
    assertEquals(N.I64, rewritten.getPartitionExpressions().get(0).getType());
    assertEquals(N.I64, rewritten.getSorts().get(0).expr().getType());
    assertEquals(
        Arrays.asList(N.I64), argumentTypes(rewritten.getWindowFunctions().get(0).arguments()));
  }

  @Test
  void referenceRootedAtAnotherExpression() {
    // A reference into a struct-typed column is rooted at the reference to that column rather than
    // at the input relation, so its type comes from the rewritten root expression.
    Rel input = scan("t", R.struct(R.I64));
    Rel plan =
        sb.project(
            in -> Arrays.asList(FieldReference.newStructReference(0, sb.fieldReference(in, 0))),
            input);

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, R.struct(N.I64)));
    FieldReference reference =
        assertInstanceOf(FieldReference.class, rewritten.getExpressions().get(0));
    assertEquals(N.I64, reference.getType());
    assertEquals(R.struct(N.I64), reference.inputExpression().orElseThrow().getType());
    // Rewriting the root must not drop the segments the reference navigates through.
    assertEquals(1, reference.segments().size());
  }

  @Test
  void referenceRootedAtAnUnchangedExpressionWithAStaleTypeIsCorrected() {
    // Re-derivation does not depend on anything having been replaced: a reference whose cached type
    // disagrees with the root it resolves against is corrected either way, whether that root is a
    // relation or, as here, another expression that the rewrite left alone.
    Rel input = scan("t", R.struct(R.I64));
    FieldReference stale =
        ImmutableFieldReference.builder()
            .from(FieldReference.newStructReference(0, sb.fieldReference(input, 0)))
            .type(N.FP64)
            .build();
    Rel plan = sb.project(in -> Arrays.asList(stale), input);

    Project rewritten =
        assertInstanceOf(
            Project.class,
            plan.accept(
                    new RelCopyOnWriteVisitor<RuntimeException>(), EmptyVisitationContext.INSTANCE)
                .orElseThrow(() -> new AssertionError("expected the stale type to be corrected")));
    assertEquals(R.I64, rewritten.getExpressions().get(0).getType());
  }

  @Test
  void outerReferenceInCorrelatedSubquery() {
    Rel correlated =
        sb.filter(
            in ->
                sb.equal(
                    sb.fieldReference(in, 0),
                    FieldReference.newRootStructOuterReference(0, R.I64, 1)),
            scan("inner", R.I64));
    Rel plan = sb.filter(in -> sb.exists(correlated), scan("outer", R.I64));

    Filter rewritten = assertInstanceOf(Filter.class, replaceScanTypes(plan, N.I64));
    Expression.SetPredicate exists =
        assertInstanceOf(Expression.SetPredicate.class, rewritten.getCondition());
    Filter innerFilter = assertInstanceOf(Filter.class, exists.tuples());
    // The first argument resolves against the subquery's own input, the second steps out one level
    // to the relation the subquery is correlated with.
    assertEquals(Arrays.asList(N.I64, N.I64), argumentTypes(innerFilter.getCondition()));
  }

  @Test
  void readRelationFilterIsNotRetypedAgainstTheEnclosingScope() {
    // The filter of a read relation resolves against the schema being read, so the record type of
    // the enclosing relation's input must not be applied to it.
    Rel scan =
        NamedScan.builder()
            .from((NamedScan) scan("t", R.I64, R.STRING))
            .filter(FieldReference.newRootStructReference(1, R.STRING))
            .build();
    Rel plan = sb.project(input -> sb.fieldReferences(input, 0), scan);

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, N.I64, R.STRING));
    NamedScan rewrittenScan = assertInstanceOf(NamedScan.class, rewritten.getInput());
    assertEquals(R.STRING, rewrittenScan.getFilter().orElseThrow().getType());
  }

  @Test
  void referenceBeyondTheNewInputIsLeftAlone() {
    // A rewrite that drops a column leaves the reference to it selecting a field the input no
    // longer has. Its type cannot be derived, and that must not fail the rewrite.
    Rel plan = sb.project(input -> sb.fieldReferences(input, 1), scan("t", R.I64, R.STRING));

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, N.I64));
    assertEquals(R.STRING, rewritten.getExpressions().get(0).getType());
  }

  @Test
  void nestedStructFieldIsRetyped() {
    FieldReference reference =
        rewriteNestedReference(R.struct(R.I64), R.struct(N.I64), StructField.of(0));
    assertEquals(N.I64, reference.getType());
    assertEquals(2, reference.segments().size());
  }

  @Test
  void nestedStructFieldTheInputNoLongerHasIsLeftAlone() {
    // Resolution has to hold at every depth, not just for the segment that selects out of the
    // record type: this reference selects a field of a struct column that has since shrunk.
    FieldReference reference =
        rewriteNestedReference(R.struct(R.I64, R.STRING), R.struct(R.I64), StructField.of(1));
    assertEquals(R.STRING, reference.getType());
    assertEquals(2, reference.segments().size());
  }

  @Test
  void threeSegmentsDeepIsRetyped() {
    FieldReference reference =
        rewriteNestedReference(
            R.struct(R.struct(R.I64, R.STRING)),
            R.struct(R.struct(R.I64, N.STRING)),
            StructField.of(0),
            StructField.of(1));
    assertEquals(N.STRING, reference.getType());
    assertEquals(3, reference.segments().size());
  }

  @Test
  void threeSegmentsDeepWithTheInnermostGoneIsLeftAlone() {
    FieldReference reference =
        rewriteNestedReference(
            R.struct(R.struct(R.I64, R.STRING)),
            R.struct(R.struct(R.I64)),
            StructField.of(0),
            StructField.of(1));
    assertEquals(R.STRING, reference.getType());
    assertEquals(3, reference.segments().size());
  }

  @Test
  void structFieldSegmentOnAContainerIsLeftAlone() {
    // The column keeps its field count but stops being a struct, so the inner segment no longer
    // applies. Retyping it against the container's element or value type would be wrong.
    assertEquals(
        R.STRING,
        rewriteNestedReference(R.struct(R.STRING), R.list(R.I64), StructField.of(0)).getType());
    assertEquals(
        R.STRING,
        rewriteNestedReference(R.struct(R.STRING), R.map(R.STRING, R.I64), StructField.of(0))
            .getType());
    assertEquals(
        R.STRING, rewriteNestedReference(R.struct(R.STRING), R.I64, StructField.of(0)).getType());
  }

  @Test
  void listElementSegmentIsRetyped() {
    assertEquals(
        N.I64, rewriteNestedReference(R.list(R.I64), R.list(N.I64), ListElement.of(0)).getType());
    // The length of a list is not part of its type, so the offset never affects the result and is
    // deliberately not bounds checked.
    assertEquals(
        N.I64, rewriteNestedReference(R.list(R.I64), R.list(N.I64), ListElement.of(7)).getType());
  }

  @Test
  void listElementSegmentOnANonListIsLeftAlone() {
    assertEquals(
        R.STRING,
        rewriteNestedReference(R.list(R.STRING), R.struct(R.I64), ListElement.of(0)).getType());
  }

  @Test
  void mapKeySegmentIsRetyped() {
    assertEquals(
        N.I64,
        rewriteNestedReference(
                R.map(R.STRING, R.I64), R.map(R.STRING, N.I64), MapKey.of(sb.str("k")))
            .getType());
  }

  @Test
  void mapKeySegmentWhoseKeyTypeNoLongerMatchesIsLeftAlone() {
    // The derivation compares the key type exactly, nullability included, so a map whose key became
    // nullable no longer accepts this segment.
    assertEquals(
        R.STRING,
        rewriteNestedReference(
                R.map(R.STRING, R.STRING), R.map(N.STRING, R.I64), MapKey.of(sb.str("k")))
            .getType());
  }

  @Test
  void mapKeySegmentOnANonMapIsLeftAlone() {
    assertEquals(
        R.STRING,
        rewriteNestedReference(R.map(R.STRING, R.STRING), R.list(R.I64), MapKey.of(sb.str("k")))
            .getType());
  }

  @Test
  void referenceThatDoesNotStartAtAStructFieldIsLeftAlone() {
    // A root reference selects out of the input's record type, which is a struct, so a reference
    // whose outermost segment is a list element or a map key cannot resolve against it.
    for (FieldReference.ReferenceSegment outermost :
        Arrays.asList(ListElement.of(0), MapKey.of(sb.str("k")))) {
      Rel plan =
          sb.project(
              in ->
                  Arrays.asList(
                      FieldReference.builder().addSegments(outermost).type(R.STRING).build()),
              scan("t", R.I64));

      Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, N.I64));
      assertEquals(R.STRING, rewritten.getExpressions().get(0).getType());
    }
  }

  @Test
  void referenceRootedAtAnotherExpressionThatNoLongerResolves() {
    Rel input = scan("t", R.struct(R.I64, R.STRING));
    Rel plan =
        sb.project(
            in -> Arrays.asList(FieldReference.newStructReference(1, sb.fieldReference(in, 0))),
            input);

    Project rewritten = assertInstanceOf(Project.class, replaceScanTypes(plan, R.struct(R.I64)));
    FieldReference reference =
        assertInstanceOf(FieldReference.class, rewritten.getExpressions().get(0));
    // The expression it is rooted at was rewritten, so the reference is rewritten too, but its own
    // type is kept because the field it selects is gone.
    assertEquals(R.STRING, reference.getType());
    assertEquals(R.struct(R.I64), reference.inputExpression().orElseThrow().getType());
    assertEquals(1, reference.segments().size());
  }

  @Test
  void hashJoinKeysAreRetypedAgainstTheirOwnSide() {
    Rel left = scan("l", R.I64, R.I64);
    Rel right = scan("r", R.STRING);
    // A join key's offsets are relative to the side it selects from, so the right key's offset is 0
    // even though that column is the third of the joined output.
    Rel plan =
        HashJoin.builder()
            .left(left)
            .right(right)
            .joinType(HashJoin.JoinType.INNER)
            .addKeys(
                ComparisonJoinKey.of(
                    sb.fieldReference(left, 0),
                    sb.fieldReference(right, 0),
                    ComparisonJoinKey.SimpleComparisonType.EQ))
            .build();

    HashJoin rewritten = assertInstanceOf(HashJoin.class, replaceScanTypesOf(plan, "r", N.STRING));
    ComparisonJoinKey key = rewritten.getKeys().get(0);
    assertEquals(R.I64, key.getLeft().getType());
    // Resolving the right key against the two inputs combined would have found a left column here.
    assertEquals(N.STRING, key.getRight().getType());
  }

  @Test
  void hashJoinFiltersAreRetypedAgainstBothInputs() {
    Rel left = scan("l", R.I64);
    Rel right = scan("r", R.I64);
    List<Rel> inputs = Arrays.asList(left, right);
    Rel plan =
        HashJoin.builder()
            .left(left)
            .right(right)
            .joinType(HashJoin.JoinType.INNER)
            .postJoinFilter(sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 1)))
            .residualExpression(
                sb.equal(sb.fieldReference(inputs, 1), sb.fieldReference(inputs, 0)))
            .build();

    HashJoin rewritten = assertInstanceOf(HashJoin.class, replaceScanTypesOf(plan, "r", N.I64));
    // Unlike the keys, these resolve against the concatenation of the two inputs.
    assertEquals(
        Arrays.asList(R.I64, N.I64), argumentTypes(rewritten.getPostJoinFilter().orElseThrow()));
    assertEquals(
        Arrays.asList(N.I64, R.I64),
        argumentTypes(rewritten.getResidualExpression().orElseThrow()));
  }

  @Test
  void lateralJoinConditionSpansBothInputs() {
    Rel left = scan("l", R.I64);
    Rel right = scan("r", R.I64);
    List<Rel> inputs = Arrays.asList(left, right);
    Rel plan =
        LateralJoin.builder()
            .left(left)
            .right(right)
            .joinType(Join.JoinType.INNER)
            .relAnchor(1)
            .condition(sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 1)))
            .build();

    LateralJoin rewritten =
        assertInstanceOf(LateralJoin.class, replaceScanTypesOf(plan, "l", N.I64));
    assertEquals(
        Arrays.asList(N.I64, R.I64), argumentTypes(rewritten.getCondition().orElseThrow()));
  }

  @Test
  void anchorBasedOuterReferenceIsRetypedAgainstTheLeftInput() {
    // A lateral join's right input references the current left row by the join's rel anchor rather
    // than by stepping out of subquery scopes. The left input is rewritten first, so the type it
    // emits is what that anchor exposes while the right input is rewritten.
    Rel plan = lateralJoinCorrelatedByAnchor(1, 1);

    LateralJoin rewritten =
        assertInstanceOf(LateralJoin.class, replaceScanTypesOf(plan, "l", N.I64));
    Filter right = assertInstanceOf(Filter.class, rewritten.getRight());
    assertEquals(Arrays.asList(R.I64, N.I64), argumentTypes(right.getCondition()));
  }

  @Test
  void outerReferenceToAnAnchorThatIsNotInScopeIsLeftAlone() {
    // The reference names an anchor no relation enclosing it exposes. Resolving that needs the
    // whole
    // plan, which this visitor does not track, so the reference keeps the type it has.
    Rel plan = lateralJoinCorrelatedByAnchor(1, 7);

    LateralJoin rewritten =
        assertInstanceOf(LateralJoin.class, replaceScanTypesOf(plan, "l", N.I64));
    Filter right = assertInstanceOf(Filter.class, rewritten.getRight());
    assertEquals(Arrays.asList(R.I64, R.I64), argumentTypes(right.getCondition()));
  }

  /**
   * A lateral join anchored at {@code anchor} whose right input filters on an outer reference to
   * the left row identified by {@code referencedAnchor}.
   */
  private Rel lateralJoinCorrelatedByAnchor(int anchor, int referencedAnchor) {
    return LateralJoin.builder()
        .left(scan("l", R.I64))
        .right(
            sb.filter(
                in ->
                    sb.equal(
                        sb.fieldReference(in, 0),
                        FieldReference.newRootStructOuterReferenceByRelReference(
                            0, R.I64, referencedAnchor)),
                scan("r", R.I64)))
        .joinType(Join.JoinType.INNER)
        .relAnchor(anchor)
        .build();
  }

  @Test
  void topNSortFieldAndCount() {
    Rel input = scan("t", R.I64);
    Rel plan =
        TopN.builder()
            .input(input)
            .sortFields(sb.sortFields(input, 0))
            .count(sb.fieldReference(input, 0))
            .build();

    TopN rewritten = assertInstanceOf(TopN.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getSortFields().get(0).expr().getType());
    assertEquals(N.I64, rewritten.getCount().orElseThrow().getType());
  }

  @Test
  void fetchOffsetAndCount() {
    Rel input = scan("t", R.I64);
    Rel plan =
        Fetch.builder()
            .input(input)
            .offset(sb.fieldReference(input, 0))
            .count(sb.fieldReference(input, 0))
            .build();

    Fetch rewritten = assertInstanceOf(Fetch.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getOffset().orElseThrow().getType());
    assertEquals(N.I64, rewritten.getCount().orElseThrow().getType());
  }

  @Test
  void singleBucketExchangeExpression() {
    Rel input = scan("t", R.I64);
    Rel plan =
        SingleBucketExchange.builder()
            .input(input)
            .partitionCount(2)
            .expression(sb.fieldReference(input, 0))
            .build();

    SingleBucketExchange rewritten =
        assertInstanceOf(SingleBucketExchange.class, replaceScanTypes(plan, N.I64));
    assertEquals(N.I64, rewritten.getExpression().getType());
  }

  @Test
  void multiBucketExchangeExpressionIsRetypedWithAnUnchangedInput() {
    // Nothing here replaces the input; the reference simply carries a type its input does not emit.
    // A guard that only asked whether the input had changed would discard the retyped expression.
    Rel plan =
        MultiBucketExchange.builder()
            .input(scan("t", R.I64))
            .partitionCount(2)
            .constrainedToCount(true)
            .expression(FieldReference.newRootStructReference(0, N.I64))
            .build();

    MultiBucketExchange rewritten =
        assertInstanceOf(
            MultiBucketExchange.class,
            plan.accept(
                    new RelCopyOnWriteVisitor<RuntimeException>(), EmptyVisitationContext.INSTANCE)
                .orElseThrow(() -> new AssertionError("expected the expression to be retyped")));
    assertEquals(R.I64, rewritten.getExpression().getType());
  }

  @Test
  void outerReferenceInScalarSubquery() {
    Rel correlated =
        sb.filter(
            in ->
                sb.equal(
                    sb.fieldReference(in, 0),
                    FieldReference.newRootStructOuterReference(0, R.I64, 1)),
            scan("inner", R.I64));
    Rel plan =
        sb.filter(
            in -> sb.equal(sb.fieldReference(in, 0), sb.scalarSubquery(correlated, R.I64)),
            scan("outer", R.I64));

    Filter rewritten = assertInstanceOf(Filter.class, replaceScanTypes(plan, N.I64));
    Expression.ScalarSubquery subquery =
        assertInstanceOf(
            Expression.ScalarSubquery.class,
            assertInstanceOf(Expression.ScalarFunctionInvocation.class, rewritten.getCondition())
                .arguments()
                .get(1));
    Filter innerFilter = assertInstanceOf(Filter.class, subquery.input());
    assertEquals(Arrays.asList(N.I64, N.I64), argumentTypes(innerFilter.getCondition()));
  }

  @Test
  void outerReferenceInInPredicateHaystack() {
    Rel correlated =
        sb.filter(
            in ->
                sb.equal(
                    sb.fieldReference(in, 0),
                    FieldReference.newRootStructOuterReference(0, R.I64, 1)),
            scan("inner", R.I64));
    Rel plan =
        sb.filter(in -> sb.inPredicate(correlated, sb.fieldReference(in, 0)), scan("outer", R.I64));

    Filter rewritten = assertInstanceOf(Filter.class, replaceScanTypes(plan, N.I64));
    Expression.InPredicate inPredicate =
        assertInstanceOf(Expression.InPredicate.class, rewritten.getCondition());
    // The needles are evaluated in the enclosing scope; only the haystack is a subquery boundary.
    assertEquals(N.I64, inPredicate.needles().get(0).getType());
    Filter innerFilter = assertInstanceOf(Filter.class, inPredicate.haystack());
    assertEquals(Arrays.asList(N.I64, N.I64), argumentTypes(innerFilter.getCondition()));
  }

  @Test
  void aVisitorCanBeReusedForASecondTraversal() {
    // The scope bookkeeping is pushed and popped around every rewrite, so a traversal leaves no
    // residue behind that would mistype the next one.
    RelCopyOnWriteVisitor<RuntimeException> visitor = scanTypeReplacer(null, N.I64);
    Rel plan =
        sb.project(
            input -> sb.fieldReferences(input, 0),
            sb.filter(input -> sb.fieldReference(input, 0), scan("t", R.I64)));

    for (int traversal = 0; traversal < 2; traversal++) {
      Project rewritten =
          assertInstanceOf(
              Project.class,
              plan.accept(visitor, EmptyVisitationContext.INSTANCE)
                  .orElseThrow(() -> new AssertionError("expected the scan to be replaced")));
      assertEquals(N.I64, rewritten.getExpressions().get(0).getType());
      assertEquals(
          N.I64, assertInstanceOf(Filter.class, rewritten.getInput()).getCondition().getType());
    }
  }

  @Test
  void unchangedPlanIsNotCopied() {
    Rel plan =
        sb.project(
            input -> sb.fieldReferences(input, 0),
            sb.filter(input -> sb.fieldReference(input, 0), scan("t", R.I64)));

    // Re-deriving the reference types must not by itself report the plan as changed: for a
    // visitation that replaces nothing, the derived types are the ones already cached.
    assertFalse(
        plan.accept(new RelCopyOnWriteVisitor<RuntimeException>(), EmptyVisitationContext.INSTANCE)
            .isPresent());
  }

  /** Rewrites every i32 literal to its negation, leaving all other literals alone. */
  private static RelCopyOnWriteVisitor<RuntimeException> negateI32LiteralsVisitor() {
    return new RelCopyOnWriteVisitor<>(
        relVisitor ->
            new ExpressionCopyOnWriteVisitor<RuntimeException>(relVisitor) {
              @Override
              public Optional<Expression> visitLiteral(Expression.Literal literal) {
                if (!(literal instanceof Expression.I32Literal)) {
                  return Optional.empty();
                }
                Expression.I32Literal i32Literal = (Expression.I32Literal) literal;
                return Optional.of(
                    Expression.I32Literal.builder()
                        .from(i32Literal)
                        .value(-i32Literal.value())
                        .build());
              }
            });
  }

  /** A window over {@code input} whose one function carries the given bounds. */
  private ConsistentPartitionWindow windowOver(Rel input, WindowBound lower, WindowBound upper) {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    return ConsistentPartitionWindow.builder()
        .input(input)
        .windowFunctions(
            Arrays.asList(
                ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                    .declaration(declaration)
                    .outputType(R.I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(Expression.AggregationInvocation.ALL)
                    .lowerBound(lower)
                    .upperBound(upper)
                    .boundsType(Expression.WindowBoundsType.RANGE)
                    .build()))
        .build();
  }

  @Test
  void consistentPartitionWindowBoundOffsetsAreRewritten() {
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.Preceding.of(sb.i32(5)), WindowBound.Following.of(sb.i32(7)));

    Optional<Rel> rewritten =
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    ConsistentPartitionWindow expected =
        ConsistentPartitionWindow.builder()
            .from(window)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .from(window.getWindowFunctions().get(0))
                        .lowerBound(WindowBound.Preceding.of(sb.i32(-5)))
                        .upperBound(WindowBound.Following.of(sb.i32(-7)))
                        .build()))
            .build();
    assertEquals(Optional.of(expected), rewritten);
  }

  @Test
  void consistentPartitionWindowWithUnchangedBoundsIsNotCopied() {
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.UNBOUNDED, WindowBound.CURRENT_ROW);

    assertEquals(
        Optional.empty(),
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE));
  }

  /** A rewrite that applies only below a window relation comes back with the input replaced. */
  @Test
  void consistentPartitionWindowRewritesItsInput() {
    Rel scan = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I32));
    Rel input = sb.project(in -> Arrays.asList(sb.i32(5)), sb.remap(1), scan);
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.UNBOUNDED, WindowBound.CURRENT_ROW);

    Optional<Rel> rewritten =
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    ConsistentPartitionWindow expected =
        ConsistentPartitionWindow.builder()
            .from(window)
            .input(sb.project(in -> Arrays.asList(sb.i32(-5)), sb.remap(1), scan))
            .build();
    assertEquals(Optional.of(expected), rewritten);
  }

  /**
   * The same guard, one relation over: a rewrite that touches only the exchange's own expression
   * comes back, where the input alone used to decide whether anything changed.
   */
  @Test
  void multiBucketExchangeRewritesItsExpression() {
    Rel scan = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I32));
    MultiBucketExchange exchange =
        MultiBucketExchange.builder()
            .input(scan)
            .expression(sb.i32(5))
            .constrainedToCount(true)
            .partitionCount(1)
            .build();

    Optional<Rel> rewritten =
        exchange.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    assertEquals(
        Optional.of(MultiBucketExchange.builder().from(exchange).expression(sb.i32(-5)).build()),
        rewritten);
  }

  /**
   * Every relation that has inputs hands back a rewrite made below it, so a relation added without
   * visiting its input fails here rather than dropping rewrites silently. Driven by the shared
   * samples, whose own test keeps them exhaustive over the model.
   */
  @Test
  void everyRelationWithInputsPropagatesARewriteBelowIt() {
    // The two the visitor refuses outright rather than descending into.
    List<Class<?>> notVisited = Arrays.asList(Expand.class, ExtensionWrite.class);
    RelCopyOnWriteVisitor<RuntimeException> renameScans =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
            return Optional.of(
                NamedScan.builder()
                    .from(namedScan)
                    .names(Collections.singletonList("renamed"))
                    .build());
          }
        };

    new RelSamples(sb, extensions)
        .samples()
        .forEach(
            (type, rel) -> {
              if (rel.getInputs().isEmpty() || notVisited.contains(type)) {
                return;
              }
              assertTrue(
                  rel.accept(renameScans, EmptyVisitationContext.INSTANCE).isPresent(),
                  type.getSimpleName());
            });
  }

  /**
   * The sweep above exercises only the input branch of each guard: a rewrite that reaches an
   * expression and leaves the input alone has to come back too, which is the second half of this
   * fix. The relations whose samples carry a field reference are pinned by name, so a relation
   * whose visit computes an expression and then decides from its input drops out of the set rather
   * than passing quietly -- {@code MultiBucketExchange} does exactly that on the base.
   *
   * <p>Both reference hooks are overridden, because a reference does not always reach the
   * expression visitor: a scatter exchange's fields and a comparison join key's sides are rewritten
   * through the relation visitor's own {@code visitFieldReference}, which is what puts {@code
   * ScatterExchange}, {@code HashJoin} and {@code MergeJoin} in the pinned set.
   */
  @Test
  void everyRelationCarryingAnExpressionPropagatesARewriteInIt() {
    List<Class<?>> notVisited = Arrays.asList(Expand.class, ExtensionWrite.class);
    RelCopyOnWriteVisitor<RuntimeException> markEveryReference =
        new RelCopyOnWriteVisitor<RuntimeException>(
            relVisitor ->
                new ExpressionCopyOnWriteVisitor<RuntimeException>(relVisitor) {
                  @Override
                  public Optional<Expression> visit(
                      FieldReference reference, EmptyVisitationContext context) {
                    return Optional.of(reference);
                  }
                }) {
          // The positions that hold a reference rather than an arbitrary expression -- a scatter
          // exchange's fields and a comparison join key's sides -- are rewritten through the
          // relation visitor's own hook, so overriding only the expression visitor's one leaves
          // them out of the sweep.
          @Override
          public Optional<FieldReference> visitFieldReference(
              FieldReference reference, EmptyVisitationContext context) {
            return Optional.of(reference);
          }
        };

    List<String> rewritten = new ArrayList<>();
    new RelSamples(sb, extensions)
        .samples()
        .forEach(
            (type, rel) -> {
              if (rel.getInputs().isEmpty() || notVisited.contains(type)) {
                return;
              }
              if (rel.accept(markEveryReference, EmptyVisitationContext.INSTANCE).isPresent()) {
                rewritten.add(type.getSimpleName());
              }
            });

    assertEquals(
        Arrays.asList(
            "Aggregate",
            "ConsistentPartitionWindow",
            "Filter",
            "HashJoin",
            "Join",
            "MergeJoin",
            "MultiBucketExchange",
            "NestedLoopJoin",
            "Project",
            "ScatterExchange",
            "SingleBucketExchange",
            "Sort",
            "TopN"),
        rewritten.stream().sorted().collect(Collectors.toList()));
  }
}
