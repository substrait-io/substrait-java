package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Filter;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class FilterRelRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "amount", "name", "status"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.BOOLEAN));

  @Test
  void simpleEqualityFilter() {
    // Filter: WHERE id = 100
    Expression condition = b.equal(b.fieldReference(baseTable, 0), b.i32(100));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void stringComparisonFilter() {
    // Filter: WHERE name = 'John'
    Expression condition = b.equal(b.fieldReference(baseTable, 2), b.str("John"));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void andConditionFilter() {
    // Filter: WHERE id = 10 AND amount = 100.0
    Expression condition =
        b.and(
            b.equal(b.fieldReference(baseTable, 0), b.i32(10)),
            b.equal(b.fieldReference(baseTable, 1), b.fp64(100.0)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void orConditionFilter() {
    // Filter: WHERE id = 5 OR id = 95
    Expression condition =
        b.or(
            b.equal(b.fieldReference(baseTable, 0), b.i32(5)),
            b.equal(b.fieldReference(baseTable, 0), b.i32(95)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void complexBooleanFilter() {
    // Filter: WHERE (id = 10 AND amount = 100) OR status = true
    Expression andCondition =
        b.and(
            b.equal(b.fieldReference(baseTable, 0), b.i32(10)),
            b.equal(b.fieldReference(baseTable, 1), b.fp64(100.0)));

    Expression condition =
        b.or(andCondition, b.equal(b.fieldReference(baseTable, 3), b.bool(true)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void multipleFieldComparison() {
    // Filter: WHERE id = amount (comparing two fields)
    Expression condition = b.equal(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 1));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void nestedFilters() {
    // Apply filter on top of another filter
    Expression firstCondition = b.equal(b.fieldReference(baseTable, 0), b.i32(10));
    Rel firstFilter = Filter.builder().input(baseTable).condition(firstCondition).build();

    Expression secondCondition = b.equal(b.fieldReference(firstFilter, 1), b.fp64(100.0));
    Rel secondFilter = Filter.builder().input(firstFilter).condition(secondCondition).build();

    verifyRoundTrip(secondFilter);
  }

  @Test
  void filterWithArithmeticExpression() {
    // Filter: WHERE amount * 2 = 100
    Expression multiply = b.multiply(b.fieldReference(baseTable, 1), b.fp64(2.0));
    Expression condition = b.equal(multiply, b.fp64(100.0));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithBooleanField() {
    // Filter: WHERE status (direct boolean field)
    Expression condition = b.fieldReference(baseTable, 3);

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithAddition() {
    // Filter: WHERE id + id = id (field with itself)
    Expression add = b.add(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));
    Expression condition = b.equal(add, b.fieldReference(baseTable, 0));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }
}
