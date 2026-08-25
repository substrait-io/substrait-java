package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.hint.Hint;
import io.substrait.relation.Filter;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

/**
 * Tests that the alternative output field names a relation carries in its hint are applied when the
 * relation is converted to Calcite.
 */
class OutputNamesTest extends PlanTestBase {

  private final Rel scan = sb.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I64, N.STRING));

  private Project projectWithHint(Optional<Hint> hint) {
    ImmutableProject.Builder builder =
        Project.builder()
            .input(scan)
            .remap(Rel.Remap.offset(2, 1))
            .addExpressions(sb.add(sb.fieldReference(scan, 0), sb.i64(1)));
    hint.ifPresent(builder::hint);
    return builder.build();
  }

  @Test
  void namesAProjection() {
    RelNode node =
        substraitToCalcite.convert(
            projectWithHint(Optional.of(Hint.builder().addOutputNames("total").build())));

    assertEquals(List.of("total"), node.getRowType().getFieldNames());
    // The names ride along on the projection the conversion already produced, rather than on one
    // added to carry them.
    assertEquals(1, countProjections(node));
  }

  @Test
  void namesTheOutputOfARelationWithAnEmitMapping() {
    // The mapping drops the first column, so the name has to land on the second one.
    Rel filter =
        Filter.builder()
            .input(scan)
            .condition(sb.equal(sb.fieldReference(scan, 0), sb.i64(1)))
            .remap(Rel.Remap.of(List.of(1)))
            .hint(Hint.builder().addOutputNames("label").build())
            .build();

    RelNode node = substraitToCalcite.convert(filter);

    assertEquals(List.of("label"), node.getRowType().getFieldNames());
  }

  @Test
  void namesTopLevelFieldsOfANestedRow() {
    // output_names is written depth first, like the names of a RelRoot, so a struct column is
    // named along with the fields inside it. Only the column itself can be renamed here: the names
    // inside it belong to the type of the expression producing it.
    Type.Struct inner = TypeCreator.REQUIRED.struct(R.I64, N.STRING);
    Rel structScan = sb.namedScan(List.of("t"), List.of("s", "x", "y"), List.of(inner));

    RelNode plain = substraitToCalcite.convert(structColumnProject(structScan, Optional.empty()));
    RelNode named =
        substraitToCalcite.convert(
            structColumnProject(
                structScan,
                Optional.of(Hint.builder().addOutputNames("renamed", "first", "second").build())));

    assertEquals(List.of("renamed"), named.getRowType().getFieldNames());
    assertEquals(
        plain.getRowType().getFieldList().get(0).getType().getFieldNames(),
        named.getRowType().getFieldList().get(0).getType().getFieldNames(),
        "the names inside the struct are left as the projection had them");
  }

  private Project structColumnProject(Rel structScan, Optional<Hint> hint) {
    ImmutableProject.Builder builder =
        Project.builder()
            .input(structScan)
            .remap(Rel.Remap.offset(1, 1))
            .addExpressions(sb.fieldReference(structScan, 0));
    hint.ifPresent(builder::hint);
    return builder.build();
  }

  @Test
  void keepsCalciteNamesWithoutAHint() {
    RelNode node = substraitToCalcite.convert(projectWithHint(Optional.empty()));

    assertEquals(List.of("$f2"), node.getRowType().getFieldNames());
  }

  @Test
  void dropsNamesThatDoNotFitTheRelation() {
    RelNode node =
        substraitToCalcite.convert(
            projectWithHint(Optional.of(Hint.builder().addOutputNames("x", "y", "z").build())));

    assertEquals(List.of("$f2"), node.getRowType().getFieldNames());
  }

  @Test
  void leavesARelationThatIsNotAProjectionAlone() {
    Rel filter =
        Filter.builder()
            .input(scan)
            .condition(sb.equal(sb.fieldReference(scan, 0), sb.i64(1)))
            .hint(Hint.builder().addOutputNames("k", "v").build())
            .build();

    RelNode node = substraitToCalcite.convert(filter);

    // No projection is added to carry the names, so the plan keeps the shape it had.
    assertEquals(List.of("a", "b"), node.getRowType().getFieldNames());
    assertEquals(0, countProjections(node));
  }

  @Test
  void dropsNamesThatRepeat() {
    // Calcite requires the field names of a projection to be distinct.
    Rel project =
        Project.builder()
            .input(scan)
            .remap(Rel.Remap.offset(2, 2))
            .addExpressions(
                sb.add(sb.fieldReference(scan, 0), sb.i64(1)),
                sb.add(sb.fieldReference(scan, 0), sb.i64(2)))
            .hint(Hint.builder().addOutputNames("same", "same").build())
            .build();

    RelNode node = substraitToCalcite.convert(project);

    assertEquals(List.of("$f2", "$f3"), node.getRowType().getFieldNames());
  }

  @Test
  void namesAnAggregateOverSeveralGroupingSets() {
    // The conversion rewrites the emit mapping of an aggregate over more than one grouping set, so
    // the node the names land on is not the one the relation's own mapping describes.
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 0), sb.grouping(input, 1)),
            input -> List.of(sb.count(input, 0)),
            Optional.empty(),
            scan);
    Rel named =
        aggregate.withHint(
            Optional.of(Hint.builder().addOutputNames("k1", "k2", "n", "g").build()));

    RelNode plain = substraitToCalcite.convert(aggregate);
    RelNode node = substraitToCalcite.convert(named);

    assertEquals(List.of("a", "b", "$f2", "$f3"), plain.getRowType().getFieldNames());
    assertEquals(List.of("k1", "k2", "n", "g"), node.getRowType().getFieldNames());
  }

  private static int countProjections(RelNode node) {
    int count = node instanceof org.apache.calcite.rel.core.Project ? 1 : 0;
    for (RelNode input : node.getInputs()) {
      count += countProjections(input);
    }
    return count;
  }
}
