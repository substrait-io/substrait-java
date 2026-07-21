package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.NamedScan;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/relations.md}. Regions marked with {@code // --8<--
 * [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet includes.
 */
class RelationsDocTest extends TestBase {

  private final SubstraitBuilder b = new SubstraitBuilder();
  private final NamedScan scan =
      b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
  private final NamedScan left =
      b.namedScan(List.of("l"), List.of("a", "b"), List.of(R.I32, R.STRING));
  private final NamedScan right =
      b.namedScan(List.of("r"), List.of("a", "b"), List.of(R.I32, R.STRING));

  @Test
  void namedScanDsl() {
    // --8<-- [start:named-scan-dsl]
    // DSL: names, column names, column types
    NamedScan scan = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
    // --8<-- [end:named-scan-dsl]
    assertNotNull(scan);
  }

  @Test
  void namedScanDirect() {
    // --8<-- [start:named-scan-direct]
    // Direct builder
    NamedScan scan =
        NamedScan.builder()
            .addNames("test_table")
            .initialSchema(
                NamedStruct.builder().addNames("only_column").struct(R.struct(R.I32)).build())
            .build();
    // --8<-- [end:named-scan-direct]
    assertNotNull(scan);
  }

  @Test
  void filter() {
    // --8<-- [start:filter]
    Filter filter = b.filter(rel -> b.equal(b.fieldReference(rel, 0), b.i32(10)), scan);
    // --8<-- [end:filter]
    assertNotNull(filter);
  }

  @Test
  void project() {
    // --8<-- [start:project]
    Project project = b.project(rel -> List.of(b.i32(1), b.fieldReference(rel, 0)), scan);
    // --8<-- [end:project]
    assertNotNull(project);
  }

  @Test
  void joinDsl() {
    // --8<-- [start:join-dsl]
    Join join =
        b.innerJoin(
            inputs ->
                b.equal(b.fieldReference(inputs.left(), 0), b.fieldReference(inputs.right(), 0)),
            left,
            right);
    // --8<-- [end:join-dsl]
    assertNotNull(join);
  }

  @Test
  void joinDirect() {
    Rel leftTable = left;
    Rel rightTable = right;
    // --8<-- [start:join-direct]
    // Direct builder, choosing the join type explicitly
    Join join =
        Join.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(ExpressionCreator.bool(false, true))
            .joinType(Join.JoinType.LEFT)
            .build();
    // --8<-- [end:join-direct]
    assertNotNull(join);
  }

  @Test
  void aggregate() {
    // --8<-- [start:aggregate]
    Aggregate aggregate =
        b.aggregate(
            rel -> b.grouping(rel, 1), // GROUP BY column 1
            rel -> List.of(b.count(rel, 0), b.sum(b.fieldReference(rel, 0))),
            scan);
    // --8<-- [end:aggregate]
    assertNotNull(aggregate);
  }

  @Test
  void aggregateFn() {
    // --8<-- [start:aggregate-fn]
    AggregateFunctionInvocation afi =
        b.aggregateFn(
            DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC,
            "count:any",
            R.I64,
            b.fieldReference(scan, 0));

    Aggregate.Measure measure = b.measure(afi);
    // --8<-- [end:aggregate-fn]
    assertNotNull(measure);
  }

  @Test
  void sort() {
    // --8<-- [start:sort]
    Sort sort = b.sort(rel -> b.sortFields(rel, 0), scan);
    // --8<-- [end:sort]
    assertNotNull(sort);
  }

  @Test
  void fetch() {
    // --8<-- [start:fetch]
    Fetch limited = b.limit(10, scan); // first 10 rows
    Fetch skipped = b.offset(5, scan); // skip 5 rows
    Fetch window = b.fetch(0, 10, scan); // offset 0, count 10
    // --8<-- [end:fetch]
    assertNotNull(limited);
    assertNotNull(skipped);
    assertNotNull(window);
  }

  @Test
  void cross() {
    // --8<-- [start:cross]
    Cross cross = b.cross(left, right);
    // --8<-- [end:cross]
    assertNotNull(cross);
  }

  @Test
  void set() {
    Rel input1 = scan;
    Rel input2 = left;
    // --8<-- [start:set]
    Set union = b.set(Set.SetOp.UNION_ALL, input1, input2);
    // --8<-- [end:set]
    assertNotNull(union);
  }

  @Test
  void virtualTableScan() {
    // --8<-- [start:virtual-table]
    VirtualTableScan table =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(List.of("col1"), R.struct(R.I32)))
            .addRows(ExpressionCreator.nestedStruct(false, ExpressionCreator.i32(false, 3)))
            .build();
    // --8<-- [end:virtual-table]
    assertNotNull(table);
  }

  @Test
  void namedWrite() {
    // --8<-- [start:named-write]
    NamedWrite write =
        b.namedWrite(
            List.of("target_table"),
            List.of("c1", "c2"),
            AbstractWriteRel.WriteOp.INSERT,
            AbstractWriteRel.CreateMode.APPEND_IF_EXISTS,
            AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
            scan);
    // --8<-- [end:named-write]
    assertNotNull(write);
  }

  @Test
  void outputRemapping() {
    // --8<-- [start:output-remap]
    Rel.Remap remap = b.remap(0, 1); // keep columns 0 and 1
    Sort sort = b.sort(rel -> b.sortFields(rel, 0), remap, scan);
    // --8<-- [end:output-remap]
    assertNotNull(sort);
  }

  @Test
  void buildingAPlan() {
    Project project =
        b.project(
            rel -> List.of(b.fieldReference(rel, 0), b.fieldReference(rel, 1)),
            b.remap(2, 3),
            scan);
    // --8<-- [start:building-a-plan]
    Plan.Root root = b.root(project, List.of("a", "b"));
    Plan plan = b.plan(root);
    // --8<-- [end:building-a-plan]
    assertNotNull(plan);
  }
}
