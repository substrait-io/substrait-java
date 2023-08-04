package io.substrait;

import io.substrait.extension.SimpleExtension;
import java.io.IOException;

public abstract class TestBase {

  protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection;

  static {
    try {
      defaultExtensionCollection = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
