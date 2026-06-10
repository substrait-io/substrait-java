package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.dialect.Dialect.DialectDocument;
import io.substrait.dialect.Dialect.JoinType;
import io.substrait.dialect.Dialect.RelationKind;
import io.substrait.dialect.Dialect.SupportedRelation;
import org.junit.jupiter.api.Test;

/**
 * Verifies that configuration-free union entries collapse to a bare enum string in YAML, while
 * configured entries serialize as mappings, and both parse back to equal objects.
 */
class DialectBareStringCollapseTest {

  @Test
  void configFreeRelationSerializesAsBareString() {
    DialectDocument dialect =
        DialectDocument.builder()
            .addSupportedRelations(SupportedRelation.of(RelationKind.FILTER))
            .build();

    String yaml = Dialect.toYaml(dialect);

    // The list item is the scalar "FILTER", not a "- relation: FILTER" mapping.
    assertTrue(yaml.contains("- \"FILTER\"") || yaml.contains("- FILTER"), yaml);
    assertFalse(yaml.contains("relation:"), yaml);

    assertEquals(dialect, Dialect.load(yaml));
  }

  @Test
  void configuredRelationSerializesAsObject() {
    DialectDocument dialect =
        DialectDocument.builder()
            .addSupportedRelations(
                SupportedRelation.builder()
                    .relation(RelationKind.JOIN)
                    .addJoinTypes(JoinType.INNER)
                    .build())
            .build();

    String yaml = Dialect.toYaml(dialect);

    assertTrue(yaml.contains("relation: \"JOIN\"") || yaml.contains("relation: JOIN"), yaml);
    assertTrue(yaml.contains("join_types"), yaml);

    assertEquals(dialect, Dialect.load(yaml));
  }

  @Test
  void extensionRelationIsNeverBare() {
    SupportedRelation extension =
        SupportedRelation.builder().relation(RelationKind.EXTENSION_LEAF).build();
    assertFalse(extension.isBare());

    String yaml =
        Dialect.toYaml(DialectDocument.builder().addSupportedRelations(extension).build());
    assertTrue(
        yaml.contains("relation: \"EXTENSION_LEAF\"") || yaml.contains("relation: EXTENSION_LEAF"),
        yaml);
  }
}
