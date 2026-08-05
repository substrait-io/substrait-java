package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import java.util.Arrays;
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
    RelCopyOnWriteVisitor<RuntimeException> visitor =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
            return Optional.of(
                NamedScan.builder()
                    .from(namedScan)
                    .initialSchema(
                        NamedStruct.of(namedScan.getInitialSchema().names(), R.struct(columnTypes)))
                    .build());
          }
        };
    return rel.accept(visitor, EmptyVisitationContext.INSTANCE)
        .orElseThrow(() -> new AssertionError("expected the visitation to replace the scan"));
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
}
