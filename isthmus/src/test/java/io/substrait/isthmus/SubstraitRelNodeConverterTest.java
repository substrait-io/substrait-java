package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.relation.Join.JoinType;
import io.substrait.relation.Rel;
import io.substrait.relation.Set.SetOp;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SubstraitRelNodeConverterTest extends PlanTestBase {

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

  final SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);

  void assertRowMatch(RelDataType actual, Type... expected) {
    assertRowMatch(actual, Arrays.asList(expected));
  }

  void assertRowMatch(RelDataType actual, List<Type> expected) {
    Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertInstanceOf(Type.Struct.class, type);
    Type.Struct struct = (Type.Struct) type;
    assertEquals(expected, struct.fields());
  }

  @Nested
  class Aggregate {
    @Test
    public void direct() {
      Plan.Root root =
          b.root(
              b.aggregate(
                  input -> b.grouping(input, 0, 2),
                  input -> List.of(b.count(input, 0)),
                  commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.I64);
    }

    @Test
    public void emit() {
      Plan.Root root =
          b.root(
              b.aggregate(
                  input -> b.grouping(input, 0, 2),
                  input -> List.of(b.count(input, 0)),
                  b.remap(1, 2),
                  commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, R.I64);
    }
  }

  @Nested
  class Cross {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.cross(commonTable, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    public void emit() {
      Plan.Root root = b.root(b.cross(commonTable, commonTable, b.remap(0, 1, 4, 6)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32, R.I32, N.STRING);
    }
  }

  @Nested
  class Fetch {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.fetch(20, 40, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    public void emit() {
      Plan.Root root = b.root(b.fetch(20, 40, b.remap(0, 2), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Filter {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.filter(input -> b.bool(true), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    public void emit() {
      Plan.Root root = b.root(b.filter(input -> b.bool(true), b.remap(0, 2), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Join {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.innerJoin(input -> b.bool(true), commonTable, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableTypeTwice);
    }

    @Test
    public void emit() {
      Plan.Root root =
          b.root(b.innerJoin(input -> b.bool(true), b.remap(0, 6), commonTable, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }

    @Test
    public void leftJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.LEFT, joinTable, joinTable)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.STRING, R.FP64, N.STRING);
    }

    @Test
    public void rightJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.RIGHT, joinTable, joinTable)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, R.STRING);
    }

    @Test
    public void outerJoin() {
      final List<Type> joinTableType = List.of(R.STRING, R.FP64, R.BINARY);
      final Rel joinTable = b.namedScan(List.of("join"), List.of("a", "b", "c"), joinTableType);

      Plan.Root root =
          b.root(
              b.project(
                  r -> b.fieldReferences(r, 0, 1, 3),
                  b.remap(6, 7, 8),
                  b.join(ji -> b.bool(true), JoinType.OUTER, joinTable, joinTable)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), N.STRING, N.FP64, N.STRING);
    }
  }

  @Nested
  class NamedScan {
    @Test
    public void direct() {
      Plan.Root root =
          b.root(b.namedScan(List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, R.FP32);
    }

    @Test
    public void emit() {
      Plan.Root root =
          b.root(
              b.namedScan(
                  List.of("example"), List.of("a", "b"), List.of(R.I32, R.FP32), b.remap(1)));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.FP32);
    }
  }

  @Nested
  class Project {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.project(input -> b.fieldReferences(input, 1, 0, 2), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(
          relNode.getRowType(), R.I32, R.FP32, N.STRING, N.BOOLEAN, R.FP32, R.I32, N.STRING);
    }

    @Test
    public void emit() {
      Plan.Root root =
          b.root(
              b.project(
                  input -> b.fieldReferences(input, 1, 0, 2), b.remap(0, 2, 4, 6), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING, R.FP32, N.STRING);
    }
  }

  @Nested
  class Set {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.set(SetOp.UNION_ALL, commonTable, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    public void emit() {
      Plan.Root root = b.root(b.set(SetOp.UNION_ALL, b.remap(0, 2), commonTable, commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }

  @Nested
  class Sort {
    @Test
    public void direct() {
      Plan.Root root = b.root(b.sort(input -> b.sortFields(input, 0, 1, 2), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), commonTableType);
    }

    @Test
    public void emit() {
      Plan.Root root =
          b.root(b.sort(input -> b.sortFields(input, 0, 1, 2), b.remap(0, 2), commonTable));

      var relNode = converter.convert(root.getInput());
      assertRowMatch(relNode.getRowType(), R.I32, N.STRING);
    }
  }
}
