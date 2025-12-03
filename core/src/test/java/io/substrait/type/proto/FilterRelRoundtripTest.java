package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Filter;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class FilterRelRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "amount", "name", "status"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.BOOLEAN));

  @Test
  void simpleEqualityFilter() {
    // Filter: WHERE id = 100
    final Expression condition = b.equal(b.fieldReference(baseTable, 0), b.i32(100));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void stringComparisonFilter() {
    // Filter: WHERE name = 'John'
    final Expression condition = b.equal(b.fieldReference(baseTable, 2), b.str("John"));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void andConditionFilter() {
    // Filter: WHERE id = 10 AND amount = 100.0
    final Expression condition =
        b.and(
            b.equal(b.fieldReference(baseTable, 0), b.i32(10)),
            b.equal(b.fieldReference(baseTable, 1), b.fp64(100.0)));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void orConditionFilter() {
    // Filter: WHERE id = 5 OR id = 95
    final Expression condition =
        b.or(
            b.equal(b.fieldReference(baseTable, 0), b.i32(5)),
            b.equal(b.fieldReference(baseTable, 0), b.i32(95)));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void complexBooleanFilter() {
    // Filter: WHERE (id = 10 AND amount = 100) OR status = true
    final Expression andCondition =
        b.and(
            b.equal(b.fieldReference(baseTable, 0), b.i32(10)),
            b.equal(b.fieldReference(baseTable, 1), b.fp64(100.0)));

    final Expression condition =
        b.or(andCondition, b.equal(b.fieldReference(baseTable, 3), b.bool(true)));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void multipleFieldComparison() {
    // Filter: WHERE id = amount (comparing two fields)
    final Expression condition =
        b.equal(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 1));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void nestedFilters() {
    // Apply filter on top of another filter
    final Expression firstCondition = b.equal(b.fieldReference(baseTable, 0), b.i32(10));
    final Rel firstFilter = Filter.builder().input(baseTable).condition(firstCondition).build();

    final Expression secondCondition = b.equal(b.fieldReference(firstFilter, 1), b.fp64(100.0));
    final Rel secondFilter = Filter.builder().input(firstFilter).condition(secondCondition).build();

    verifyRoundTrip(secondFilter);
  }

  @Test
  void filterWithArithmeticExpression() {
    // Filter: WHERE amount * 2 = 100
    final Expression multiply = b.multiply(b.fieldReference(baseTable, 1), b.fp64(2.0));
    final Expression condition = b.equal(multiply, b.fp64(100.0));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithBooleanField() {
    // Filter: WHERE status (direct boolean field)
    final Expression condition = b.fieldReference(baseTable, 3);

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithAddition() {
    // Filter: WHERE id + id = id (field with itself)
    final Expression add = b.add(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));
    final Expression condition = b.equal(add, b.fieldReference(baseTable, 0));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }
}
