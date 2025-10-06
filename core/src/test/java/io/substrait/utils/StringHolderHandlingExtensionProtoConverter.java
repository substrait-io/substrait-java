package io.substrait.utils;

import com.google.protobuf.Any;
import io.substrait.extension.AdvancedExtension.Enhancement;
import io.substrait.extension.AdvancedExtension.Optimization;
import io.substrait.extension.ExtensionProtoConverter;

public class StringHolderHandlingExtensionProtoConverter extends ExtensionProtoConverter {
  @Override
  protected Any toProto(final Optimization optimization) {
    if (optimization instanceof StringHolder) {
      return ((StringHolder) optimization).toProto(null);
    }

    return null;
  }

  @Override
  protected Any toProto(final Enhancement enhancement) {
    if (enhancement instanceof StringHolder) {
      return ((StringHolder) enhancement).toProto(null);
    }

    return null;
  }
}
