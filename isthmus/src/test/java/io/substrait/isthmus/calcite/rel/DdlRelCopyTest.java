package io.substrait.isthmus.calcite.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.isthmus.PlanTestBase;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * The DDL relations are single-input, their {@code copy()} rejects any other input count, and the
 * schema they declare is carried rather than derived.
 */
class DdlRelCopyTest extends PlanTestBase {

  private final RelNode input = builder.values(new String[] {"a"}, 1).build();
  private final RelNode otherInput = builder.values(new String[] {"b"}, 2).build();

  private RelDataType declaredSchema() {
    return typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.BIGINT)), List.of("declared"));
  }

  @Test
  void createTableCopiesItsSingleInput() {
    CreateTable createTable = new CreateTable(List.of("FOO"), input);

    assertEquals(input, createTable.copy(createTable.getTraitSet(), List.of(input)).getInput(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> createTable.copy(createTable.getTraitSet(), List.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> createTable.copy(createTable.getTraitSet(), List.of(input, input)));
  }

  @Test
  void createViewCopiesItsSingleInput() {
    CreateView createView = new CreateView(List.of("FOO"), input);

    assertEquals(input, createView.copy(createView.getTraitSet(), List.of(input)).getInput(0));
    assertThrows(
        IllegalArgumentException.class, () -> createView.copy(createView.getTraitSet(), List.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> createView.copy(createView.getTraitSet(), List.of(input, input)));
  }

  /**
   * A planner rewrites what fills the object, which is what {@code copy} is for. The schema the
   * statement declared is not derived from that input, so rewriting it leaves the schema alone.
   */
  @Test
  void aDeclaredSchemaSurvivesACopyOntoAnotherInput() {
    CreateTable createTable = new CreateTable(List.of("FOO"), declaredSchema(), input);
    CreateView createView = new CreateView(List.of("FOO"), declaredSchema(), input);

    RelNode copiedTable = createTable.copy(createTable.getTraitSet(), List.of(otherInput));
    RelNode copiedView = createView.copy(createView.getTraitSet(), List.of(otherInput));

    assertEquals(otherInput, copiedTable.getInput(0));
    assertEquals(declaredSchema(), ((CreateTable) copiedTable).getTableSchema());
    assertEquals(otherInput, copiedView.getInput(0));
    assertEquals(declaredSchema(), ((CreateView) copiedView).getViewSchema());
  }

  /**
   * The declared schema names the object's columns and the input fills them, so it has to be a
   * struct of as many leaf fields as the input produces. The names and the types are the
   * statement's own -- a CTAS may declare both -- so neither is checked.
   */
  @Test
  void aSchemaTheInputCannotFillIsRejected() {
    RelDataType notAStruct = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RelDataType twoColumns =
        typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                typeFactory.createSqlType(SqlTypeName.BIGINT)),
            List.of("one", "two"));

    assertThrows(
        IllegalArgumentException.class, () -> new CreateTable(List.of("FOO"), notAStruct, input));
    assertThrows(
        IllegalArgumentException.class, () -> new CreateTable(List.of("FOO"), twoColumns, input));
    assertThrows(
        IllegalArgumentException.class, () -> new CreateView(List.of("FOO"), notAStruct, input));
    assertThrows(
        IllegalArgumentException.class, () -> new CreateView(List.of("FOO"), twoColumns, input));
  }
}
