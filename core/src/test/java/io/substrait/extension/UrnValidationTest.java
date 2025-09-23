package io.substrait.extension;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UrnValidationTest {

  @Test
  public void testMissingUrnThrowsException() {
    String yamlWithoutUrn = "%YAML 1.2\n" + "---\n" + "scalar_functions:\n" + "  - name: test\n";
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(yamlWithoutUrn));
    assertTrue(exception.getMessage().contains("Extension YAML file must contain a 'urn' field"));
  }

  @Test
  public void testInvalidUrnFormatThrowsException() {
    String yamlWithInvalidUrn = "%YAML 1.2\n" + "---\n" + "urn: invalid:format\n" + "scalar_functions:\n" + "  - name: test\n";
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(yamlWithInvalidUrn));
    assertTrue(exception.getMessage().contains("URN must follow format 'extension:<namespace>:<name>'"));
  }

  @Test
  public void testValidUrnWorks() {
    String yamlWithValidUrn = "%YAML 1.2\n" + "---\n" + "urn: extension:test:valid\n" + "scalar_functions:\n" + "  - name: test\n";
    assertDoesNotThrow(() -> SimpleExtension.load(yamlWithValidUrn));
  }
}
