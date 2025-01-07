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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

public class ComplexSortTest extends PlanTestBase {

  final TypeCreator R = TypeCreator.of(false);
  SubstraitBuilder b = new SubstraitBuilder(extensions);

  final SubstraitToCalcite substraitToCalcite =
      new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory);

  public static class CollationRelWriter extends RelWriterImpl {
    public CollationRelWriter(StringWriter sw) {
      super(new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
      var collation = rel.getTraitSet().getCollation();
      if (!collation.isDefault()) {
        StringBuilder s = new StringBuilder();
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

    Rel rel =
        b.project(
            input -> b.fieldReferences(input, 0),
            b.remap(1),
            b.sort(
                input ->
                    List.of(
                        b.sortField(
                            b.fieldReference(input, 0), Expression.SortDirection.ASC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING))));

    String expected =
        """
        Collation: [0]
        LogicalSort(sort0=[$0], dir0=[ASC])
          LogicalTableScan(table=[[example]])
        """;

    RelNode relReturned = substraitToCalcite.convert(rel);
    var sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastExpressionSort() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a FROM example ORDER BY a::INT

    Rel rel =
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

    String expected =
        """
         LogicalProject(a0=[$0])
           Collation: [1]
           LogicalSort(sort0=[$1], dir0=[ASC])
             LogicalProject(a=[$0], a0=[CAST($0):INTEGER NOT NULL])
               LogicalTableScan(table=[[example]])
         """;

    RelNode relReturned = substraitToCalcite.convert(rel);
    var sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastProjectAndSortWithSortDirection() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a::INT FROM example ORDER BY a::INT DESC NULLS LAST

    Rel rel =
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

    String expected =
        """
        LogicalProject(a0=[CAST($0):INTEGER NOT NULL])
          Collation: [1 DESC-nulls-last]
          LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])
            LogicalProject(a=[$0], a0=[CAST($0):INTEGER NOT NULL])
              LogicalTableScan(table=[[example]])
         """;

    RelNode relReturned = substraitToCalcite.convert(rel);
    var sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleCastSortToOriginalType() {
    // CREATE TABLE example (a VARCHAR)
    // SELECT a FROM example ORDER BY a::VARCHAR

    Rel rel =
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

    String expected =
        """
        LogicalProject(a0=[$0])
          Collation: [1 DESC-nulls-last]
          LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])
            LogicalProject(a=[$0], a0=[$0])
              LogicalTableScan(table=[[example]])
        """;

    RelNode relReturned = substraitToCalcite.convert(rel);
    var sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }

  @Test
  void handleComplex2ExpressionSort() {
    // CREATE TABLE example (a VARCHAR, b INT)
    // SELECT b, a FROM example ORDER BY a::INT > 0 DESC, -b + 42 ASC NULLS LAST

    Rel rel =
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
                            b.add(b.negate(b.cast(b.fieldReference(input, 0), R.I32)), b.i32(42)),
                            Expression.SortDirection.ASC_NULLS_LAST)),
                b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.STRING, R.I64))));

    String expected =
        """
        LogicalProject(a0=[$0], b0=[$1])
          Collation: [2 DESC, 3]
          LogicalSort(sort0=[$2], sort1=[$3], dir0=[DESC], dir1=[ASC])
            LogicalProject(a=[$0], b=[$1], a0=[CAST($0):INTEGER NOT NULL], $f3=[+(-(CAST($0):INTEGER NOT NULL), 42)])
              LogicalTableScan(table=[[example]])
        """;

    RelNode relReturned = substraitToCalcite.convert(rel);
    var sw = new StringWriter();
    relReturned.explain(new CollationRelWriter(sw));
    assertEquals(expected, sw.toString());
  }
}
