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
      sb.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "amount", "name", "status"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.BOOLEAN));

  @Test
  void simpleEqualityFilter() {
    // Filter: WHERE id = 100
    Expression condition = sb.equal(sb.fieldReference(baseTable, 0), sb.i32(100));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void stringComparisonFilter() {
    // Filter: WHERE name = 'John'
    Expression condition = sb.equal(sb.fieldReference(baseTable, 2), sb.str("John"));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void andConditionFilter() {
    // Filter: WHERE id = 10 AND amount = 100.0
    Expression condition =
        sb.and(
            sb.equal(sb.fieldReference(baseTable, 0), sb.i32(10)),
            sb.equal(sb.fieldReference(baseTable, 1), sb.fp64(100.0)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void orConditionFilter() {
    // Filter: WHERE id = 5 OR id = 95
    Expression condition =
        sb.or(
            sb.equal(sb.fieldReference(baseTable, 0), sb.i32(5)),
            sb.equal(sb.fieldReference(baseTable, 0), sb.i32(95)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void complexBooleanFilter() {
    // Filter: WHERE (id = 10 AND amount = 100) OR status = true
    Expression andCondition =
        sb.and(
            sb.equal(sb.fieldReference(baseTable, 0), sb.i32(10)),
            sb.equal(sb.fieldReference(baseTable, 1), sb.fp64(100.0)));

    Expression condition =
        sb.or(andCondition, sb.equal(sb.fieldReference(baseTable, 3), sb.bool(true)));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void multipleFieldComparison() {
    // Filter: WHERE id = amount (comparing two fields)
    Expression condition =
        sb.equal(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 1));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void nestedFilters() {
    // Apply filter on top of another filter
    Expression firstCondition = sb.equal(sb.fieldReference(baseTable, 0), sb.i32(10));
    Rel firstFilter = Filter.builder().input(baseTable).condition(firstCondition).build();

    Expression secondCondition = sb.equal(sb.fieldReference(firstFilter, 1), sb.fp64(100.0));
    Rel secondFilter = Filter.builder().input(firstFilter).condition(secondCondition).build();

    verifyRoundTrip(secondFilter);
  }

  @Test
  void filterWithArithmeticExpression() {
    // Filter: WHERE amount * 2 = 100
    Expression multiply = sb.multiply(sb.fieldReference(baseTable, 1), sb.fp64(2.0));
    Expression condition = sb.equal(multiply, sb.fp64(100.0));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithBooleanField() {
    // Filter: WHERE status (direct boolean field)
    Expression condition = sb.fieldReference(baseTable, 3);

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void filterWithAddition() {
    // Filter: WHERE id + id = id (field with itself)
    Expression add = sb.add(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 0));
    Expression condition = sb.equal(add, sb.fieldReference(baseTable, 0));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }
}
