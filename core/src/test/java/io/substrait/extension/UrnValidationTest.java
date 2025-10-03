package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class UrnValidationTest {

  @Test
  public void testMissingUrnThrowsException() {
    String yamlWithoutUrn = "%YAML 1.2\n" + "---\n" + "scalar_functions:\n" + "  - name: test\n";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> SimpleExtension.load("some/uri", yamlWithoutUrn));
    assertTrue(exception.getMessage().contains("Extension YAML file must contain a 'urn' field"));
  }

  @Test
  public void testInvalidUrnFormatThrowsException() {
    String yamlWithInvalidUrn =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: invalid:format\n"
            + "scalar_functions:\n"
            + "  - name: test\n";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SimpleExtension.load("some/uri", yamlWithInvalidUrn));
    assertTrue(
        exception.getMessage().contains("URN must follow format 'extension:<namespace>:<name>'"));
  }

  @Test
  public void testValidUrnWorks() {
    String yamlWithValidUrn =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:test:valid\n"
            + "scalar_functions:\n"
            + "  - name: test\n";
    assertDoesNotThrow(() -> SimpleExtension.load("some/uri", yamlWithValidUrn));
  }

  @Test
  public void testUriUrnMapIsPopulated() {
    String yamlWithValidUrn =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:test:valid\n"
            + "scalar_functions:\n"
            + "  - name: test\n";
    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load("test://uri", yamlWithValidUrn);
    assertEquals("extension:test:valid", collection.getUrnFromUri("test://uri"));
  }
}
