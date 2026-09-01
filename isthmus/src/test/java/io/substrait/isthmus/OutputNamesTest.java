package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.hint.Hint;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
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

    // Calcite's own name for an expression that has none. The tests that drop a name list compare
    // against this conversion rather than repeating it.
    assertEquals(List.of("$f2"), node.getRowType().getFieldNames());
  }

  @Test
  void dropsNamesThatDoNotFitTheRelation() {
    RelNode plain = substraitToCalcite.convert(projectWithHint(Optional.empty()));
    RelNode node =
        substraitToCalcite.convert(
            projectWithHint(Optional.of(Hint.builder().addOutputNames("x", "y", "z").build())));

    assertEquals(plain.getRowType().getFieldNames(), node.getRowType().getFieldNames());
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
    Rel project = twoColumnProject();
    RelNode named =
        substraitToCalcite.convert(
            project.withHint(Optional.of(Hint.builder().addOutputNames("same", "same").build())));

    assertEquals(
        substraitToCalcite.convert(project).getRowType().getFieldNames(),
        named.getRowType().getFieldNames());
  }

  @Test
  void dropsNamesThatAreEmpty() {
    // Calcite reads an empty field name as the star identifier, so a projection carrying one
    // renders as SELECT ... AS *, which does not parse. The spec does not say whether a name may
    // be empty; dropping the list is the reading that cannot produce such a plan.
    Rel project = twoColumnProject();
    RelNode plain = substraitToCalcite.convert(project);
    RelNode named =
        substraitToCalcite.convert(
            project.withHint(Optional.of(Hint.builder().addOutputNames("", "x").build())));

    assertEquals(plain.getRowType().getFieldNames(), named.getRowType().getFieldNames());
    assertEquals(
        SubstraitSqlDialect.toSql(plain).getSql(), SubstraitSqlDialect.toSql(named).getSql());
  }

  private Rel twoColumnProject() {
    return Project.builder()
        .input(scan)
        .remap(Rel.Remap.offset(2, 2))
        .addExpressions(
            sb.add(sb.fieldReference(scan, 0), sb.i64(1)),
            sb.add(sb.fieldReference(scan, 0), sb.i64(2)))
        .build();
  }

  @Test
  void leavesAnAggregateThatEmitsDirectlyAlone() {
    // The conversion of an aggregate over several grouping sets ends in a projection that carries
    // the grouping-set index. Its other columns are the relation's own, in the declared order, but
    // that one comes back as Calcite's folded GROUP_ID literal -- a BIGINT where the relation
    // declares an i32 -- so the names are dropped rather than pinned onto a column whose type the
    // plan does not describe.
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 0), sb.grouping(input, 1)),
            input -> List.of(sb.count(input, 0)),
            Optional.empty(),
            scan);

    RelNode plain = substraitToCalcite.convert(aggregate);
    RelNode node =
        substraitToCalcite.convert(
            aggregate.withHint(
                Optional.of(Hint.builder().addOutputNames("k1", "k2", "n", "g").build())));

    assertEquals(plain.getRowType().getFieldNames(), node.getRowType().getFieldNames());
  }

  @Test
  void namesTheOutputOfAJoinWithAnEmitMapping() {
    Rel join =
        sb.innerJoin(
            input -> sb.equal(sb.fieldReference(input, 0), sb.fieldReference(input, 2)),
            Rel.Remap.of(List.of(2, 0)),
            scan,
            scan);
    Rel named = join.withHint(Optional.of(Hint.builder().addOutputNames("right", "left").build()));

    assertEquals(
        List.of("right", "left"), substraitToCalcite.convert(named).getRowType().getFieldNames());
  }

  @Test
  void leavesARelationWithAnIdentityEmitMappingAlone() {
    // An identity mapping needs no projection, so there is no node to hang the names on.
    Rel filter =
        Filter.builder()
            .input(scan)
            .condition(sb.equal(sb.fieldReference(scan, 0), sb.i64(1)))
            .remap(Rel.Remap.of(List.of(0, 1)))
            .hint(Hint.builder().addOutputNames("k", "v").build())
            .build();

    RelNode node = substraitToCalcite.convert(filter);

    assertEquals(List.of("a", "b"), node.getRowType().getFieldNames());
    assertEquals(0, countProjections(node));
  }

  @Test
  void leavesTheNamesOfAnotherRelationAlone() {
    // The always-true condition means Calcite builds no filter at all, so the node on top is the
    // projection of the inner relation, with the names that relation asked for.
    Rel filter =
        Filter.builder()
            .input(hintedInnerProject())
            .condition(sb.bool(true))
            .hint(Hint.builder().addOutputNames("outer").build())
            .build();

    assertEquals(List.of("inner"), substraitToCalcite.convert(filter).getRowType().getFieldNames());
  }

  @Test
  void namesAProjectionWithoutAnEmitMapping() {
    // A projection is the one relation whose conversion produces a Calcite projection of its own,
    // with no emit mapping needed to ask for one.
    Rel project =
        Project.builder()
            .input(scan)
            .addExpressions(sb.add(sb.fieldReference(scan, 0), sb.i64(1)))
            .hint(Hint.builder().addOutputNames("a", "b", "total").build())
            .build();

    RelNode node = substraitToCalcite.convert(project);

    assertEquals(List.of("a", "b", "total"), node.getRowType().getFieldNames());
    assertEquals(1, countProjections(node));
  }

  @Test
  void leavesTheNamesOfAnotherRelationAloneWhenItsOwnOperatorIsElided() {
    // Same as above, with an emit mapping that changes nothing either: Calcite builds no filter
    // and no projection, so the node on top is still the inner relation's, hint and all.
    Rel filter =
        Filter.builder()
            .input(hintedInnerProject())
            .condition(sb.bool(true))
            .remap(Rel.Remap.of(List.of(0)))
            .hint(Hint.builder().addOutputNames("outer").build())
            .build();

    assertEquals(List.of("inner"), substraitToCalcite.convert(filter).getRowType().getFieldNames());
  }

  @Test
  void namesAnAggregateWhoseGroupingColumnsCalciteOrdersDifferently() {
    // The grouping sets first mention field 1 and then field 0, so the relation declares its
    // grouping columns as (b, a) where the aggregate underneath emits (a, b). The emit mapping the
    // conversion adds puts them back in the declared order, which is what lets the names be bound
    // by position at all. The mapping drops the grouping-set index, so every remaining column is
    // one the relation declares.
    Rel scan3 =
        sb.namedScan(List.of("t3"), List.of("a", "b", "c"), List.of(R.I64, N.STRING, R.FP64));
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 1), sb.grouping(input, 0)),
            input -> List.of(sb.count(input, 0)),
            Optional.of(Rel.Remap.of(List.of(0, 1, 2))),
            scan3);

    RelNode named =
        substraitToCalcite.convert(
            aggregate.withHint(
                Optional.of(Hint.builder().addOutputNames("k_b", "k_a", "n").build())));

    assertEquals(List.of("k_b", "k_a", "n"), named.getRowType().getFieldNames());
  }

  @Test
  void dropsNamesWhereTheColumnsAreNotTheRelationsColumns() {
    // Same aggregate with the grouping-set index emitted: the relation types it i32 where the
    // GROUP_ID call the conversion appends is i64, so the fourth column is not the fourth column
    // the relation declares and the names would land on a column the plan does not name.
    Rel scan3 =
        sb.namedScan(List.of("t3"), List.of("a", "b", "c"), List.of(R.I64, N.STRING, R.FP64));
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 1), sb.grouping(input, 0)),
            input -> List.of(sb.count(input, 0)),
            Optional.of(Rel.Remap.of(List.of(0, 1, 2, 3))),
            scan3);

    RelNode plain = substraitToCalcite.convert(aggregate);
    RelNode named =
        substraitToCalcite.convert(
            aggregate.withHint(
                Optional.of(Hint.builder().addOutputNames("k_b", "k_a", "n", "gs").build())));

    assertEquals(plain.getRowType().getFieldNames(), named.getRowType().getFieldNames());
  }

  private Rel hintedInnerProject() {
    return Project.builder()
        .input(scan)
        .remap(Rel.Remap.offset(2, 1))
        .addExpressions(sb.add(sb.fieldReference(scan, 0), sb.i64(1)))
        .hint(Hint.builder().addOutputNames("inner").build())
        .build();
  }

  private static int countProjections(RelNode node) {
    int count = node instanceof org.apache.calcite.rel.core.Project ? 1 : 0;
    for (RelNode input : node.getInputs()) {
      count += countProjections(input);
    }
    return count;
  }
}
