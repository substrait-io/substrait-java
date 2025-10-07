package io.substrait.utils;

import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.RelProtoConverter;

public class StringHolderHandlingRelProtoConverter extends RelProtoConverter {

  public StringHolderHandlingRelProtoConverter(final ExtensionCollector extensionCollector) {
    super(extensionCollector, new StringHolderHandlingExtensionProtoConverter());
  }
}
