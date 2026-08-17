package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies the failure modes of the {@link SimpleExtension} {@code load} overloads. */
class SimpleExtensionLoadTest {

  private static final String MISSING_RESOURCE = "/substrait/extensions/does_not_exist.yaml";
  private static final String PRESENT_RESOURCE = "/substrait/extensions/functions_boolean.yaml";

  @Test
  void loadEmptyResourcePathsThrows() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SimpleExtension.load(Collections.emptyList()));
    assertTrue(e.getMessage().contains("Require at least one resource path"), e.getMessage());
  }

  @Test
  void loadMissingResourceNamesThePath() {
    List<String> paths = Collections.singletonList(MISSING_RESOURCE);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(paths));
    assertTrue(e.getMessage().contains(MISSING_RESOURCE), e.getMessage());
  }

  @Test
  void loadMissingResourceAmongPresentOnesNamesTheMissingPath() {
    List<String> paths = Arrays.asList(PRESENT_RESOURCE, MISSING_RESOURCE);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(paths));
    assertTrue(e.getMessage().contains(MISSING_RESOURCE), e.getMessage());
  }

  @Test
  void loadNullStreamThrowsWithAMessage() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SimpleExtension.load((InputStream) null));
    assertNotNull(e.getMessage());
  }

  @Test
  void loadPresentResourceSucceeds() {
    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load(Collections.singletonList(PRESENT_RESOURCE));
    assertTrue(collection.scalarFunctions().size() > 0);
  }
}
