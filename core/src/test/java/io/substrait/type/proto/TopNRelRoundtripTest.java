package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.TopN;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TopNRelRoundtripTest extends TestBase {

  final Rel baseTable =
      sb.namedScan(
          Collections.singletonList("topn_test_table"),
          Arrays.asList("id", "amount", "name", "category"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.STRING));

  private Expression.SortField sortField(int field, Expression.SortDirection direction) {
    return Expression.SortField.builder()
        .expr(sb.fieldReference(baseTable, field))
        .direction(direction)
        .build();
  }

  @Test
  void rowsOnlyWithCount() {
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(0, Expression.SortDirection.ASC_NULLS_FIRST))
            .count(sb.i64(10))
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void offsetAndCount() {
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(1, Expression.SortDirection.DESC_NULLS_LAST))
            .offset(sb.i64(5))
            .count(sb.i64(20))
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void countAbsentReturnsAll() {
    // No count set: signals ALL. Offset present.
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(0, Expression.SortDirection.ASC_NULLS_FIRST))
            .offset(sb.i64(3))
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void withTiesMode() {
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(1, Expression.SortDirection.DESC_NULLS_FIRST))
            .count(sb.i64(15))
            .mode(TopN.Mode.WITH_TIES)
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void defaultModeIsRowsOnly() {
    // No mode set: defaults to ROWS_ONLY and must round-trip.
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(2, Expression.SortDirection.ASC_NULLS_LAST))
            .count(sb.i64(1))
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void multipleSortFields() {
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(
                sortField(3, Expression.SortDirection.ASC_NULLS_FIRST),
                sortField(1, Expression.SortDirection.DESC_NULLS_LAST))
            .offset(sb.i64(2))
            .count(sb.i64(8))
            .mode(TopN.Mode.WITH_TIES)
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void viaBuilderHelper() {
    Rel topN =
        sb.topN(
            input -> Arrays.asList(sortField(0, Expression.SortDirection.ASC_NULLS_FIRST)),
            0,
            5,
            baseTable);

    verifyRoundTrip(topN);
  }

  @Test
  void nestedUnderTopN() {
    Rel inner =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(0, Expression.SortDirection.ASC_NULLS_FIRST))
            .count(sb.i64(100))
            .build();

    Rel outer =
        TopN.builder()
            .input(inner)
            .addSortFields(
                Expression.SortField.builder()
                    .expr(sb.fieldReference(inner, 1))
                    .direction(Expression.SortDirection.DESC_NULLS_LAST)
                    .build())
            .count(sb.i64(10))
            .build();

    verifyRoundTrip(outer);
  }

  @Test
  void unspecifiedProtoModeNormalizesToRowsOnly() {
    // A producer may leave the mode unset (FETCH_MODE_UNSPECIFIED); the spec defines its default
    // as ROWS_ONLY, so proto -> POJO conversion surfaces ROWS_ONLY rather than UNSPECIFIED.
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .addSortFields(sortField(0, Expression.SortDirection.ASC_NULLS_FIRST))
            .count(sb.i64(10))
            .build();

    io.substrait.proto.Rel proto = relProtoConverter.toProto(topN);
    io.substrait.proto.Rel unsetMode =
        proto.toBuilder().setTopN(proto.getTopN().toBuilder().clearMode()).build();

    Rel converted = protoRelConverter.from(unsetMode);
    assertInstanceOf(TopN.class, converted);
    assertEquals(TopN.Mode.ROWS_ONLY, ((TopN) converted).getMode());
  }
}
