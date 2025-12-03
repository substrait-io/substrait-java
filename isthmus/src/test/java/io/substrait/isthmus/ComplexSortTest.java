package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class ComplexSortTest extends PlanTestBase {

  final TypeCreator R = TypeCreator.of(false);
  SubstraitBuilder b = new SubstraitBuilder(extensions);

  final SubstraitToCalcite substraitToCalcite =
      new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory);

  /**
   * A {@link RelWriterImpl} that annotates each {@link RelNode} with its {@link RelCollation} trait
   * information. A {@link RelNode} is only annotated if its {@link RelCollation} is not empty.
   */
  public static class CollationRelWriter extends RelWriterImpl {
    public CollationRelWriter(final StringWriter sw) {
      super(new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
    }

    @Override
    protected void explain_(final RelNode rel, final List<Pair<String, @Nullable Object>> values) {
      final RelCollation collation = rel.getTraitSet().getCollation();
      if (!collation.isDefault()) {
        final StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        s.append("Collation: ").append(collation.toString());
        pw.println(s);
      }
      super.explain_(rel, values);
    }
  }

  @Test
  void handleInputReferenceSort() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a FROM example ORDER BY a

    final Rel rel =
        b.project(
            input -> b.fieldReferences(input, 0),
            b.remap(1),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.fieldReference(input, 0), Expression.SortDirection.ASC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING))));

    final String expected =
        "Collation: [0]\n"
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalTableScan(table=[[example]])\n";

    final RelNode relReturned = substraitToCalcite.convert(rel);
    final StringWriter sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastExpressionSort() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a FROM example ORDER BY a::INT

    final Rel rel =
        b.project(
            input -> b.fieldReferences(input, 0),
            b.remap(1),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.cast(b.fieldReference(input, 0), R.I32),
                            Expression.SortDirection.ASC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING))));

    final String expected =
        "LogicalProject(a0=[$0])\n"
            + "  Collation: [1]\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC])\n"
            + "    LogicalProject(a=[$0], a0=[CAST($0):INTEGER NOT NULL])\n"
            + "      LogicalTableScan(table=[[example]])\n";

    final RelNode relReturned = substraitToCalcite.convert(rel);
    final StringWriter sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastProjectAndSortWithSortDirection() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a::INT FROM example ORDER BY a::INT DESC NULLS LAST

    final Rel rel =
        b.project(
            input -> List.of(b.cast(b.fieldReference(input, 0), R.I32)),
            b.remap(1),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.cast(b.fieldReference(input, 0), R.I32),
                            Expression.SortDirection.DESC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING))));

    final String expected =
        "LogicalProject(a0=[CAST($0):INTEGER NOT NULL])\n"
            + "  Collation: [1 DESC-nulls-last]\n"
            + "  LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "    LogicalProject(a=[$0], a0=[CAST($0):INTEGER NOT NULL])\n"
            + "      LogicalTableScan(table=[[example]])\n";

    final RelNode relReturned = substraitToCalcite.convert(rel);
    final StringWriter sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastSortToOriginalType() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a FROM example ORDER BY a::VARCHAR

    final Rel rel =
        b.project(
            input -> List.of(b.fieldReference(input, 0)),
            b.remap(1),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.cast(b.fieldReference(input, 0), R.STRING),
                            Expression.SortDirection.DESC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING))));

    final String expected =
        "LogicalProject(a0=[$0])\n"
            + "  Collation: [1 DESC-nulls-last]\n"
            + "  LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "    LogicalProject(a=[$0], a0=[$0])\n"
            + "      LogicalTableScan(table=[[example]])\n";

    final RelNode relReturned = substraitToCalcite.convert(rel);
    final StringWriter sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleComplex2ExpressionSort() {
    // CREATE TABLE example (a VARCHAR, b INT)
    // SELECT b, a FROM example ORDER BY a::INT DESC, -b + 42 ASC NULLS LAST

    final Rel rel =
        b.project(
            input -> List.of(b.fieldReference(input, 0), b.fieldReference(input, 1)),
            b.remap(2, 3),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.cast(b.fieldReference(input, 0), R.I32),
                            Expression.SortDirection.DESC_NULLS_FIRST),
                        b.sortField(
                            b.add(b.negate(b.fieldReference(input, 1)), b.i32(42)),
                            Expression.SortDirection.ASC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.STRING, R.I32))));

    final String expected =
        "LogicalProject(a0=[$0], b0=[$1])\n"
            + "  Collation: [2 DESC, 3]\n"
            + "  LogicalSort(sort0=[$2], sort1=[$3], dir0=[DESC], dir1=[ASC])\n"
            + "    LogicalProject(a=[$0], b=[$1], a0=[CAST($0):INTEGER NOT NULL], $f3=[+(-($1), 42)])\n"
            + "      LogicalTableScan(table=[[example]])\n";

    final RelNode relReturned = substraitToCalcite.convert(rel);
    final StringWriter sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }
}
