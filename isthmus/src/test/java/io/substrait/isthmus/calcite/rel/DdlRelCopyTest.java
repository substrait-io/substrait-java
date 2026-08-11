package io.substrait.isthmus.calcite.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.isthmus.PlanTestBase;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

/** The DDL relations are single-input, and their {@code copy()} rejects any other input count. */
class DdlRelCopyTest extends PlanTestBase {

  private final RelNode input = builder.values(new String[] {"a"}, 1).build();

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
}
