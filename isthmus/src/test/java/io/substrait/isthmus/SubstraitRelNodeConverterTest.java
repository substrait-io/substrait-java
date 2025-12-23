package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.Rel;
import io.substrait.relation.Set.SetOp;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SubstraitRelNodeConverterTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  // Define a shared table (i.e. a NamedScan) for use in tests.
  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final List<Type> commonTableTypeTwice =
      Stream.concat(commonTableType.stream(), commonTableType.stream())
          .collect(Collectors.toList());
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  @Nested
  class Aggregate {
    @Test
    void direct() {
      Plan.Root root =
          b.root(
              b.aggregate(
                  input -> b.grouping(input, 0, 2),
                  input -> List.of(b.count(input, 0)),
                  commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.I64);
    }

    @Test
    void emit() {
      Plan.Root root =
          b.root(
              b.aggregate(
                  input -> b.grouping(input, 0, 2),
                  input -> List.of(b.count(input, 0)),
                  b.remap(1, 2),
                  commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, R.I64);
    }
  }

  @Nested
  class Cross {
    @Test
    void direct() {
      Plan.Root root = b.root(b.cross(commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    void emit() {
      Plan.Root root = b.root(b.cross(commonTable, commonTable, b.remap(0, 1, 4, 6)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32, R.I32, N.STRING);
    }
  }

  @Nested
  class Fetch {
    @Test
    void direct() {
      Plan.Root root = b.root(b.fetch(20, 40, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = b.root(b.fetch(20, 40, b.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Filter {
    @Test
    void direct() {
      Plan.Root root = b.root(b.filter(input -> b.bool(true), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = b.root(b.filter(input -> b.bool(true), b.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Join {
    @Test
    void direct() {
      Plan.Root root = b.root(b.innerJoin(input -> b.bool(true), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    void emit() {
      Plan.Root root =
          b.root(b.innerJoin(input -> b.bool(true), b.remap(0, 6), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }

    @Test
    void leftJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.LEFT, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.STRING, R.FP64, N.STRING);
    }

    @Test
    void rightJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.RIGHT, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, R.STRING);
    }

    @Test
    void outerJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.OUTER, joinTable, joinTable)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, N.STRING);
    }
  }

  @Nested
  class NamedScan {
    @Test
    void direct() {
      Plan.Root root =
          b.root(b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32);
    }

    @Test
    void emit() {
      Plan.Root root =
          b.root(
              b.namedScan(
                  List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32), b.remap(1)));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.FP32);
    }
  }

  @Nested
  class Project {
    @Test
    void direct() {
      Plan.Root root = b.root(b.project(input -> b.fieldReferences(input, 1, 0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(
          relNode.getRowType(), R.I32, R.FP32, N.STRING, N.BOOLEAN, R.FP32, R.I32, N.STRING);
    }

    @Test
    void emit() {
      Plan.Root root =
          b.root(
              b.project(
                  input -> b.fieldReferences(input, 1, 0, 2), b.remap(0, 2, 4, 6), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.FP32, N.STRING);
    }
  }

  @Nested
  class Set {
    @Test
    void direct() {
      Plan.Root root = b.root(b.set(SetOp.UNION_ALL, commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root = b.root(b.set(SetOp.UNION_ALL, b.remap(0, 2), commonTable, commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Sort {
    @Test
    void direct() {
      Plan.Root root = b.root(b.sort(input -> b.sortFields(input, 0, 1, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    void emit() {
      Plan.Root root =
          b.root(b.sort(input -> b.sortFields(input, 0, 1, 2), b.remap(0, 2), commonTable));

      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class EmptyScan {

    @Test
    void direct() {
      Rel emptyScan =
          io.substrait.relation.EmptyScan.builder()
              .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct(R.I32, N.STRING)))
              .build();

      Plan.Root root = b.root(emptyScan);
      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), List.of(R.I32, N.STRING));
    }

    @Test
    void emit() {
      Rel emptyScanWithRemap =
          io.substrait.relation.EmptyScan.builder()
              .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct(R.I32, N.STRING)))
              .remap(Rel.Remap.of(List.of(0)))
              .build();

      Plan.Root root = b.root(emptyScanWithRemap);
      RelNode relNode = substraitToCalcite.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32);
    }
  }
}
