package io.substrait.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.extension.SimpleExtension;
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
import io.substrait.relation.Rel.Remap;
import io.substrait.relation.Sort;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SubstraitBuilderTest extends TestBase {

  private SubstraitBuilder builder;

  @BeforeEach
  void newbuilder() {
    this.builder = new SubstraitBuilder();
  }

  @Test
  void basicCreation() {
    assertNotNull(this.builder);
    assertInstanceOf(SimpleExtension.ExtensionCollection.class, this.builder.getExtensions());
  }

  @Nested
  @DisplayName("Literal and Expression Tests")
  class ExpressionTests {

    @Test
    void testLiterals() {
      assertEquals(true, builder.bool(true).value());
      assertEquals(10, builder.i8(10).value());
      assertEquals(100, builder.i16(100).value());
      assertEquals(1000, builder.i32(1000).value());
      assertEquals(10_000L, builder.i64(10_000L).value());
      assertEquals(1.5f, builder.fp32(1.5f).value());
      assertEquals(2.5, builder.fp64(2.5).value());
      assertEquals("foo", builder.str("foo").value());
    }

    @Test
    void testFieldReferences() {
      final NamedScan scan = createSimpleScan();
      final FieldReference ref = builder.fieldReference(scan, 0);
      assertNotNull(ref);

      final List<FieldReference> refs = builder.fieldReferences(scan, 0, 1);
      assertEquals(2, refs.size());
    }

    @Test
    void testCast() {
      final Expression.I32Literal input = builder.i32(1);
      final Expression cast = builder.cast(input, R.I64);
      assertNotNull(cast);
      assertTrue(cast instanceof Expression.Cast);
    }
  }

  @Nested
  @DisplayName("Relation Building Tests")
  class RelationTests {

    @Test
    void testNamedScan() {
      final NamedScan scan =
          builder.namedScan(List.of("table"), List.of("c1", "c2"), List.of(R.I32, R.STRING));
      assertNotNull(scan);
      assertEquals(List.of("table"), scan.getNames());
    }

    @Test
    void testProject() {
      final NamedScan scan = createSimpleScan();
      final Project project =
          builder.project(rel -> List.of(builder.i32(1), builder.fieldReference(rel, 0)), scan);
      assertNotNull(project);
      assertEquals(scan, project.getInput());
    }

    @Test
    void testFilter() {
      final NamedScan scan = createSimpleScan();
      final Filter filter =
          builder.filter(
              rel -> builder.equal(builder.fieldReference(rel, 0), builder.i32(10)), scan);
      assertNotNull(filter);
      assertNotNull(filter.getCondition());
    }

    @Test
    void testInnerJoin() {
      final NamedScan left = createSimpleScan();
      final NamedScan right = createSimpleScan();
      final Join join =
          builder.innerJoin(
              inputs ->
                  builder.equal(
                      builder.fieldReference(inputs.left(), 0),
                      builder.fieldReference(inputs.right(), 0)),
              left,
              right);
      assertNotNull(join);
      assertEquals(Join.JoinType.INNER, join.getJoinType());
    }

    @Test
    void testFetchAndLimit() {
      final NamedScan scan = createSimpleScan();
      Fetch limit = builder.limit(10, scan);
      assertEquals(10, limit.getCount().getAsLong());
      assertEquals(0, limit.getOffset());
      limit = builder.limit(10, Remap.of(Arrays.asList(new Integer[] {0, 1})), scan);
      assertNotNull(limit);

      Fetch offset = builder.offset(5, scan);
      assertEquals(5, offset.getOffset());

      offset = builder.offset(5, Remap.of(Arrays.asList(new Integer[] {0, 1})), scan);
      assertEquals(5, offset.getOffset());
    }

    @Test
    void testSort() {
      final NamedScan scan = createSimpleScan();
      Sort sort = builder.sort(rel -> builder.sortFields(rel, 0), scan);
      assertNotNull(sort);
      sort =
          builder.sort(
              rel -> builder.sortFields(rel, 0),
              Remap.of(Arrays.asList(new Integer[] {0, 1})),
              scan);
      assertNotNull(sort);
    }

    @Test
    void testCross() {
      final NamedScan left = createSimpleScan();
      final NamedScan right = createSimpleScan();

      Cross cross = builder.cross(left, right);
      assertNotNull(cross);

      cross = builder.cross(left, right, Remap.of(Arrays.asList(new Integer[] {0, 1})));
      assertNotNull(cross);
    }

    @Test
    void testFetch() {
      final NamedScan left = createSimpleScan();
      Fetch fetch = builder.fetch(0, 1, left);
      assertNotNull(fetch);

      fetch = builder.fetch(0, 1, Remap.of(Arrays.asList(new Integer[] {0, 1})), left);
      assertNotNull(fetch);
    }
  }

  @Nested
  @DisplayName("Aggregate and Scalar Function Tests")
  class FunctionTests {

    @Test
    void testAggregateMeasures() {
      final NamedScan scan = createSimpleScan();
      final Aggregate.Measure count = builder.count(scan, 0);
      final Aggregate.Measure sum = builder.sum(builder.fieldReference(scan, 0));

      assertNotNull(count);
      assertNotNull(sum);

      final Aggregate.Grouping grouping = builder.grouping(scan, 1);
      assertNotNull(grouping);
      assertEquals(1, grouping.getExpressions().size());

      final AggregateFunctionInvocation afi1 =
          builder.aggregateFn(
              "extension:io.substrait:functions_aggregate_generic",
              "count:any",
              Type.I32.builder().nullable(false).build(),
              builder.fieldReference(scan, 0));
      assertNotNull(afi1);
    }

    @Test
    void testArithmeticFunctions() {
      final Expression left = builder.i32(10);
      final Expression right = builder.i32(20);

      assertNotNull(builder.add(left, right));
      assertNotNull(builder.subtract(left, right));
      assertNotNull(builder.multiply(left, right));
      assertNotNull(builder.divide(left, right));
      assertNotNull(builder.negate(left));
    }

    @Test
    void testBooleanLogic() {
      final Expression b1 = builder.bool(true);
      final Expression b2 = builder.bool(false);

      assertNotNull(builder.and(b1, b2));
      assertNotNull(builder.or(b1, b2));
      assertNotNull(builder.not(b1));
      assertNotNull(builder.isNull(b1));
    }
  }

  @Nested
  @DisplayName("Write and Plan Tests")
  class PlanTests {

    @Test
    void testNamedWrite() {
      final NamedScan scan = createSimpleScan();
      final NamedWrite write =
          builder.namedWrite(
              List.of("target_table"),
              List.of("c1", "c2"),
              AbstractWriteRel.WriteOp.INSERT,
              AbstractWriteRel.CreateMode.APPEND_IF_EXISTS,
              AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
              scan);
      assertNotNull(write);
    }

    @Test
    void testPlanCreation() {
      final NamedScan scan = createSimpleScan();
      final Plan.Root root = builder.root(scan, List.of("out1", "out2"));
      final Plan plan = builder.plan(root);

      assertNotNull(plan);
      assertEquals(1, plan.getRoots().size());
    }
  }

  // Helper method to create a base relation for testing
  private NamedScan createSimpleScan() {
    return builder.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
  }
}
