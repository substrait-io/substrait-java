package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link OuterReferenceConverter}, which translates outer references between the offset-based
 * ({@code steps_out}) and id-based ({@code rel_anchor}/{@code rel_reference}) encodings.
 */
class OuterReferenceConverterTest extends TestBase {

  private final Rel customerTableScan =
      sb.namedScan(
          List.of("customer"),
          List.of("c_custkey", "c_nationkey"),
          List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64));

  private final Rel orderTableScan =
      sb.namedScan(
          List.of("orders"),
          List.of("o_orderkey", "o_custkey"),
          List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64));

  private final Rel nationTableScan =
      sb.namedScan(
          List.of("nation"),
          List.of("n_nationkey", "n_name"),
          List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING));

  /** SELECT o_orderkey, (SELECT c_nationkey FROM customer WHERE c_custkey = orders.o_custkey). */
  private Rel oneStepPlan() {
    return sb.project(
        input ->
            List.of(
                sb.fieldReference(input, 0),
                sb.scalarSubquery(
                    sb.project(
                        input2 -> List.of(sb.fieldReference(input2, 1)),
                        Remap.of(List.of(1)),
                        sb.filter(
                            input2 ->
                                sb.equal(
                                    sb.fieldReference(input2, 0),
                                    FieldReference.newRootStructOuterReference(
                                        1, TypeCreator.REQUIRED.I64, 1)),
                            customerTableScan)),
                    TypeCreator.NULLABLE.I64)),
        Remap.of(List.of(2, 3)),
        orderTableScan);
  }

  /**
   * Nested subquery whose innermost outer reference steps out two boundaries to the orders scan.
   */
  private Rel twoStepPlan() {
    return sb.project(
        input ->
            List.of(
                sb.fieldReference(input, 0),
                sb.scalarSubquery(
                    sb.project(
                        input2 -> List.of(sb.fieldReference(input2, 1)),
                        Remap.of(List.of(2)),
                        sb.filter(
                            input2 ->
                                sb.equal(
                                    sb.fieldReference(input2, 0),
                                    sb.scalarSubquery(
                                        sb.project(
                                            input3 -> List.of(sb.fieldReference(input3, 1)),
                                            Remap.of(List.of(1)),
                                            sb.filter(
                                                input3 ->
                                                    sb.equal(
                                                        sb.fieldReference(input3, 0),
                                                        FieldReference.newRootStructOuterReference(
                                                            1, TypeCreator.REQUIRED.I64, 2)),
                                                customerTableScan)),
                                        TypeCreator.NULLABLE.I64)),
                            nationTableScan)),
                    TypeCreator.NULLABLE.I64)),
        Remap.of(List.of(2, 3)),
        orderTableScan);
  }

  @Test
  void oneStepToIdBasedStampsAnchorAndReference() {
    Rel stepsOut = oneStepPlan();
    Rel idBased = OuterReferenceConverter.toIdBased(stepsOut);

    assertNotEquals(stepsOut, idBased);

    // The outer project's input (the orders scan) is the binding relation and gets anchor 1.
    Project outerProject = (Project) idBased;
    assertEquals(1, outerProject.getInput().getRelAnchor().orElseThrow(AssertionError::new));

    // The correlated reference now uses rel_reference instead of steps_out.
    FieldReference outerRef = outerReferenceOf(outerProject);
    assertEquals(1, outerRef.outerReferenceRelReference().orElseThrow(AssertionError::new));
    assertFalse(outerRef.outerReferenceStepsOut().isPresent());
  }

  @Test
  void oneStepRoundTripsBackToStepsOut() {
    Rel stepsOut = oneStepPlan();
    assertEquals(
        stepsOut, OuterReferenceConverter.toStepsOut(OuterReferenceConverter.toIdBased(stepsOut)));
  }

  @Test
  void twoStepRoundTripsThroughIdBased() {
    Rel stepsOut = twoStepPlan();
    Rel idBased = OuterReferenceConverter.toIdBased(stepsOut);

    assertNotEquals(stepsOut, idBased);
    // The innermost reference steps out two levels to the orders scan, which gets the anchor.
    assertEquals(1, ((Project) idBased).getInput().getRelAnchor().orElseThrow(AssertionError::new));
    assertEquals(stepsOut, OuterReferenceConverter.toStepsOut(idBased));
  }

  @Test
  void sameScopeReferencesShareAnchor() {
    // Two correlated references at the same subquery boundary must resolve to the same anchor.
    Rel stepsOut =
        sb.project(
            input ->
                List.of(
                    sb.fieldReference(input, 0),
                    sb.scalarSubquery(
                        sb.project(
                            input2 -> List.of(sb.fieldReference(input2, 1)),
                            Remap.of(List.of(1)),
                            sb.filter(
                                input2 ->
                                    sb.and(
                                        sb.equal(
                                            sb.fieldReference(input2, 0),
                                            FieldReference.newRootStructOuterReference(
                                                0, TypeCreator.REQUIRED.I64, 1)),
                                        sb.equal(
                                            sb.fieldReference(input2, 1),
                                            FieldReference.newRootStructOuterReference(
                                                1, TypeCreator.REQUIRED.I64, 1))),
                                customerTableScan)),
                        TypeCreator.NULLABLE.I64)),
            Remap.of(List.of(2, 3)),
            orderTableScan);

    Rel idBased = OuterReferenceConverter.toIdBased(stepsOut);
    assertEquals(1, ((Project) idBased).getInput().getRelAnchor().orElseThrow(AssertionError::new));
    assertEquals(stepsOut, OuterReferenceConverter.toStepsOut(idBased));
  }

  @Test
  void stepsOutBeyondDepthIsRejected() {
    Rel invalid =
        sb.project(
            input ->
                List.of(
                    sb.scalarSubquery(
                        sb.project(
                            input2 -> List.of(sb.fieldReference(input2, 1)),
                            Remap.of(List.of(1)),
                            sb.filter(
                                input2 ->
                                    sb.equal(
                                        sb.fieldReference(input2, 0),
                                        // steps_out=5 exceeds the single enclosing boundary
                                        FieldReference.newRootStructOuterReference(
                                            1, TypeCreator.REQUIRED.I64, 5)),
                                customerTableScan)),
                        TypeCreator.NULLABLE.I64)),
            Remap.of(List.of(2)),
            orderTableScan);

    assertThrows(IllegalArgumentException.class, () -> OuterReferenceConverter.toIdBased(invalid));
  }

  @Test
  void unresolvableRelReferenceIsRejected() {
    // An id-based reference to an anchor that is not an enclosing scope cannot become steps_out.
    Rel dangling =
        sb.project(
            input ->
                List.of(
                    sb.scalarSubquery(
                        sb.project(
                            input2 -> List.of(sb.fieldReference(input2, 1)),
                            Remap.of(List.of(1)),
                            sb.filter(
                                input2 ->
                                    sb.equal(
                                        sb.fieldReference(input2, 0),
                                        FieldReference.newRootStructOuterReferenceByRelReference(
                                            1, TypeCreator.REQUIRED.I64, 999)),
                                customerTableScan)),
                        TypeCreator.NULLABLE.I64)),
            Remap.of(List.of(2)),
            orderTableScan);

    assertThrows(
        UnsupportedOperationException.class, () -> OuterReferenceConverter.toStepsOut(dangling));
  }

  @Test
  void multiRootPlanAssignsPlanWideUniqueAnchors() {
    // Two roots, each with a correlated subquery binding to a distinct relation. Anchors must be
    // unique plan-wide (Rel#getRelAnchor()), so the second root's binding relation cannot reuse the
    // first root's anchor.
    Plan stepsOut =
        Plan.builder()
            // from(...) supplies a valid executionBehavior (and the first root); add the second.
            .from(sb.plan(sb.root(oneStepPlanBoundTo(orderTableScan))))
            .addRoots(sb.root(oneStepPlanBoundTo(nationTableScan)))
            .build();

    Plan idBased = OuterReferenceConverter.toIdBased(stepsOut);

    int anchor0 =
        ((Project) idBased.getRoots().get(0).getInput())
            .getInput()
            .getRelAnchor()
            .orElseThrow(AssertionError::new);
    int anchor1 =
        ((Project) idBased.getRoots().get(1).getInput())
            .getInput()
            .getRelAnchor()
            .orElseThrow(AssertionError::new);
    assertNotEquals(anchor0, anchor1);

    assertEquals(stepsOut, OuterReferenceConverter.toStepsOut(idBased));
  }

  @Test
  void correlatedReferenceIntoJoinScopeIsRejected() {
    // A correlated reference whose binding scope is a multi-input (join) relation, reached through
    // a
    // subquery nested in the join condition, is unsupported and must fail rather than silently bind
    // the reference to the wrong (single-input) enclosing relation.
    Rel plan =
        sb.project(
            input ->
                List.of(
                    sb.scalarSubquery(
                        sb.innerJoin(
                            joinInputs ->
                                sb.equal(
                                    sb.fieldReference(joinInputs, 0),
                                    sb.scalarSubquery(
                                        sb.project(
                                            input3 -> List.of(sb.fieldReference(input3, 1)),
                                            Remap.of(List.of(1)),
                                            sb.filter(
                                                input3 ->
                                                    sb.equal(
                                                        sb.fieldReference(input3, 0),
                                                        // steps_out=1 resolves to the enclosing
                                                        // join scope, which is multi-input.
                                                        FieldReference.newRootStructOuterReference(
                                                            0, TypeCreator.REQUIRED.I64, 1)),
                                                customerTableScan)),
                                        TypeCreator.NULLABLE.I64)),
                            nationTableScan,
                            orderTableScan),
                        TypeCreator.NULLABLE.I64)),
            Remap.of(List.of(2)),
            orderTableScan);

    assertThrows(
        UnsupportedOperationException.class, () -> OuterReferenceConverter.toIdBased(plan));
  }

  /** A one-step correlated-subquery plan whose outer reference binds to {@code bindingScan}. */
  private Rel oneStepPlanBoundTo(Rel bindingScan) {
    return sb.project(
        input ->
            List.of(
                sb.fieldReference(input, 0),
                sb.scalarSubquery(
                    sb.project(
                        input2 -> List.of(sb.fieldReference(input2, 1)),
                        Remap.of(List.of(1)),
                        sb.filter(
                            input2 ->
                                sb.equal(
                                    sb.fieldReference(input2, 0),
                                    FieldReference.newRootStructOuterReference(
                                        1, TypeCreator.REQUIRED.I64, 1)),
                            customerTableScan)),
                    TypeCreator.NULLABLE.I64)),
        Remap.of(List.of(2, 3)),
        bindingScan);
  }

  /** Extracts the outer field reference from the one-step plan's scalar-subquery filter. */
  private FieldReference outerReferenceOf(Project outerProject) {
    Expression.ScalarSubquery subquery =
        (Expression.ScalarSubquery) outerProject.getExpressions().get(1);
    Filter filter = (Filter) ((Project) subquery.input()).getInput();
    Expression.ScalarFunctionInvocation equal =
        (Expression.ScalarFunctionInvocation) filter.getCondition();
    return (FieldReference) equal.arguments().get(1);
  }
}
