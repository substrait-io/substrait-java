package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Cross;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.utils.StringHolder;
import io.substrait.relation.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verify that the various extension types in {@link io.substrait.relation.Extension} roundtrip
 * correctly.
 */
public class ExtensionRoundtripTest extends TestBase {

  final ProtoRelConverter protoRelConverter =
      new StringHolderHandlingProtoRelConverter(functionCollector, defaultExtensionCollection);

  final Rel commonTable =
      b.namedScan(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

  final AdvancedExtension commonExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("COMMON ENHANCEMENT"))
          .addOptimizations(new StringHolder("COMMON OPTIMIZATION"))
          .build();

  final StringHolder detail = new StringHolder("DETAIL");

  final AdvancedExtension relExtension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("REL ENHANCEMENT"))
          .addOptimizations(new StringHolder("REL OPTIMIZATION"))
          .build();

  @Override
  protected void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void virtualTable() {
    Rel rel =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct()))
            .addRows(Expression.StructLiteral.builder().fields(Collections.emptyList()).build())
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void localFiles() {
    Rel rel =
        LocalFiles.builder()
            .initialSchema(
                NamedStruct.of(
                    Collections.emptyList(), Type.Struct.builder().nullable(false).build()))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void namedScan() {
    Rel rel =
        NamedScan.builder()
            .from(
                b.namedScan(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionTable() {
    Rel rel = ExtensionTable.from(detail).build();
    verifyRoundTrip(rel);
  }

  @Test
  void filter() {
    Rel rel =
        Filter.builder()
            .from(b.filter(__ -> b.bool(true), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void fetch() {
    Rel rel =
        Fetch.builder()
            .from(b.fetch(1, 2, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void aggregate() {
    Rel rel =
        Aggregate.builder()
            .from(b.aggregate(b::grouping, __ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void sort() {
    Rel rel =
        Sort.builder()
            .from(b.sort(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void join() {
    Rel rel =
        Join.builder()
            .from(b.innerJoin(__ -> b.bool(true), commonTable, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void hashJoin() {
    // with empty keys
    List<Integer> leftEmptyKeys = Collections.emptyList();
    List<Integer> rightEmptyKeys = Collections.emptyList();
    Rel relWithoutKeys =
        HashJoin.builder()
            .from(
                b.hashJoin(
                    leftEmptyKeys,
                    rightEmptyKeys,
                    HashJoin.JoinType.INNER,
                    commonTable,
                    commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void mergeJoin() {
    // with empty keys
    List<Integer> leftEmptyKeys = Collections.emptyList();
    List<Integer> rightEmptyKeys = Collections.emptyList();
    Rel relWithoutKeys =
        MergeJoin.builder()
            .from(
                b.mergeJoin(
                    leftEmptyKeys,
                    rightEmptyKeys,
                    MergeJoin.JoinType.INNER,
                    commonTable,
                    commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void nestedLoopJoin() {
    Rel rel =
        NestedLoopJoin.builder()
            .from(
                b.nestedLoopJoin(
                    __ -> b.bool(true), NestedLoopJoin.JoinType.INNER, commonTable, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void project() {
    Rel rel =
        Project.builder()
            .from(b.project(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void expand() {
    Rel rel =
        Expand.builder()
            .from(b.expand(__ -> Collections.emptyList(), commonTable))
            .commonExtension(commonExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void set() {
    Rel rel =
        Set.builder()
            .from(b.set(Set.SetOp.UNION_ALL, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionSingleRel() {
    Rel rel = ExtensionSingle.from(detail, commonTable).commonExtension(commonExtension).build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionMultiRel() {
    Rel rel =
        ExtensionMulti.from(detail, commonTable, commonTable)
            .commonExtension(commonExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void extensionLeafRel() {
    Rel rel = ExtensionLeaf.from(detail).commonExtension(commonExtension).build();
    verifyRoundTrip(rel);
  }

  @Test
  void cross() {
    Rel rel =
        Cross.builder()
            .from(b.cross(commonTable, commonTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();
    verifyRoundTrip(rel);
  }

  @Nested
  class ExtensionThroughExpression {
    // There are some expression that can contains relations.
    // Check that custom extensions in these relations can be handled.

    Rel baseTable =
        b.namedScan(
            Stream.of("test_table").collect(Collectors.toList()),
            Stream.of("test_column").collect(Collectors.toList()),
            Stream.of(TypeCreator.REQUIRED.I64).collect(Collectors.toList()));
    Rel relWithEnhancement =
        Project.builder()
            .from(b.project(input -> Collections.emptyList(), baseTable))
            .commonExtension(commonExtension)
            .extension(relExtension)
            .build();

    @Test
    void scalarSubquery() {
      var rel =
          b.project(
              input ->
                  Stream.of(
                          Expression.ScalarSubquery.builder()
                              .input(relWithEnhancement)
                              .type(TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.I64))
                              .build())
                      .collect(Collectors.toList()),
              commonTable);

      verifyRoundTrip(rel);
    }

    @Test
    void inPredicate() {
      var rel =
          b.project(
              input ->
                  Stream.of(
                          Expression.InPredicate.builder()
                              .needles(Collections.emptyList())
                              .haystack(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }

    @Test
    void setPredicate() {
      var rel =
          b.project(
              input ->
                  Stream.of(
                          Expression.SetPredicate.builder()
                              .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
                              .tuples(relWithEnhancement)
                              .build())
                      .collect(Collectors.toList()),
              commonTable);
      verifyRoundTrip(rel);
    }
  }
}
