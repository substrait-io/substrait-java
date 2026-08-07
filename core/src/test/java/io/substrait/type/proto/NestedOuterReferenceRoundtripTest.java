package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for outer references that resolve to an enclosing scope reached through nested
 * relation conversion: a lateral join's left row, or a correlated subquery's outer query. These
 * exercise the schema threading that types an outer reference against the referenced enclosing
 * relation rather than the relation that lexically hosts the reference.
 *
 * <p>The two standalone (non-nested) forms are covered by {@link OuterReferenceRoundtripTest};
 * these cases only round-trip correctly once the enclosing schema is threaded through {@code
 * ProtoRelConverter} / {@code ProtoExpressionConverter}.
 */
class NestedOuterReferenceRoundtripTest extends TestBase {

  final Rel leftTable =
      sb.namedScan(
          Arrays.asList("T1"),
          Arrays.asList("a", "b", "c"),
          Arrays.asList(R.I64, R.FP64, R.STRING));

  final Rel rightTable =
      sb.namedScan(
          Arrays.asList("T2"),
          Arrays.asList("d", "e", "f"),
          Arrays.asList(R.FP64, R.STRING, R.I64));

  final Rel customer =
      sb.namedScan(
          Arrays.asList("customer"),
          Arrays.asList("c_custkey", "c_nationkey"),
          Arrays.asList(R.I64, R.I64));

  final Rel orders =
      sb.namedScan(
          Arrays.asList("orders"),
          Arrays.asList("o_orderkey", "o_custkey"),
          Arrays.asList(R.I64, R.I64));

  final Rel nation =
      sb.namedScan(
          Arrays.asList("nation"),
          Arrays.asList("n_nationkey", "n_name"),
          Arrays.asList(R.I64, R.STRING));

  @Test
  void idBasedOuterReferenceIntoLateralJoinLeftRow() {
    // The right input references field 0 of the current left row via an id-based outer reference to
    // the lateral join's anchor. On the way back from proto the reference must be typed against the
    // LEFT schema (T1), not the right input's own schema (T2).
    int anchor = 1;
    FieldReference outerRef =
        FieldReference.newRootStructOuterReferenceByRelReference(
            0, leftTable.getRecordType(), anchor);

    Rel right = sb.filter(input -> sb.equal(outerRef, sb.fieldReference(input, 2)), rightTable);

    LateralJoin lateralJoin =
        LateralJoin.builder()
            .left(leftTable)
            .right(right)
            .joinType(Join.JoinType.INNER)
            .relAnchor(anchor)
            .build();

    verifyRoundTrip(lateralJoin);
  }

  @Test
  void offsetBasedOuterReferenceIntoEnclosingSubquery() {
    // A correlated scalar subquery whose filter steps out one boundary to the enclosing orders
    // scan; the reference must be typed against orders, not the customer scan hosting it.
    FieldReference outerRef =
        FieldReference.newRootStructOuterReference(1, orders.getRecordType(), 1);

    Rel subquery =
        sb.project(
            input -> List.of(sb.fieldReference(input, 1)),
            Remap.of(List.of(2)),
            sb.filter(input -> sb.equal(sb.fieldReference(input, 0), outerRef), customer));

    Rel plan =
        sb.project(
            input -> List.of(sb.fieldReference(input, 0), sb.scalarSubquery(subquery, R.I64)),
            Remap.of(List.of(2, 3)),
            orders);

    verifyRoundTrip(plan);
  }

  @Test
  void offsetBasedOuterReferenceStepsOutTwoLevels() {
    // The innermost subquery steps out two boundaries, past the intervening nation scope, to the
    // orders scan.
    FieldReference outerRef =
        FieldReference.newRootStructOuterReference(1, orders.getRecordType(), 2);

    Rel innerSubquery =
        sb.project(
            input -> List.of(sb.fieldReference(input, 1)),
            Remap.of(List.of(2)),
            sb.filter(input -> sb.equal(sb.fieldReference(input, 0), outerRef), customer));

    Rel outerSubquery =
        sb.project(
            input -> List.of(sb.fieldReference(input, 1)),
            Remap.of(List.of(2)),
            sb.filter(
                input ->
                    sb.equal(sb.fieldReference(input, 0), sb.scalarSubquery(innerSubquery, R.I64)),
                nation));

    Rel plan =
        sb.project(
            input ->
                List.of(sb.fieldReference(input, 0), sb.scalarSubquery(outerSubquery, R.STRING)),
            Remap.of(List.of(2, 3)),
            orders);

    verifyRoundTrip(plan);
  }

  @Test
  void idBasedOuterReferenceIntoEnclosingSubquery() {
    // The same correlation as the offset-based case, but the enclosing orders scan carries a
    // rel_anchor and the reference resolves to it by id rather than by boundary count.
    int anchor = 1;
    Rel anchoredOrders = orders.withRelAnchor(anchor);

    FieldReference outerRef =
        FieldReference.newRootStructOuterReferenceByRelReference(
            1, anchoredOrders.getRecordType(), anchor);

    Rel subquery =
        sb.project(
            input -> List.of(sb.fieldReference(input, 1)),
            Remap.of(List.of(2)),
            sb.filter(input -> sb.equal(sb.fieldReference(input, 0), outerRef), customer));

    Rel plan =
        sb.project(
            input -> List.of(sb.fieldReference(input, 0), sb.scalarSubquery(subquery, R.I64)),
            Remap.of(List.of(2, 3)),
            anchoredOrders);

    verifyRoundTrip(plan);
  }

  @Test
  void idBasedLateralReferenceFromSubqueryNestedInRight() {
    // The lateral join's left row is referenced from inside a correlated subquery nested in the
    // right input, exercising id-based resolution across a subquery boundary.
    int anchor = 1;
    FieldReference outerRef =
        FieldReference.newRootStructOuterReferenceByRelReference(
            0, leftTable.getRecordType(), anchor);

    Rel subqueryInRight =
        sb.project(
            input -> List.of(sb.fieldReference(input, 1)),
            Remap.of(List.of(2)),
            sb.filter(input -> sb.equal(sb.fieldReference(input, 0), outerRef), customer));

    Rel right =
        sb.filter(
            input ->
                sb.equal(sb.fieldReference(input, 2), sb.scalarSubquery(subqueryInRight, R.I64)),
            rightTable);

    LateralJoin lateralJoin =
        LateralJoin.builder()
            .left(leftTable)
            .right(right)
            .joinType(Join.JoinType.INNER)
            .relAnchor(anchor)
            .build();

    verifyRoundTrip(lateralJoin);
  }
}
