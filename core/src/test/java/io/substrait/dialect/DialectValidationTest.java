package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies the build-time guards that keep dialect entries serializable and schema-valid. */
class DialectValidationTest {

  @Test
  void executionContextVariableRejectsMetadata() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SupportedExpression.builder()
                .expression(ExpressionKind.EXECUTION_CONTEXT_VARIABLE)
                .metadata(Map.of("k", "v"))
                .build());
  }

  @Test
  void writeAndDdlWriteTypesAreMutuallyExclusive() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SupportedRelation.builder()
                .relation(RelationKind.WRITE)
                .addWriteTypes(WriteType.NAMED_TABLE)
                .addDdlWriteTypes(DdlWriteType.NAMED_OBJECT)
                .build());
  }
}
