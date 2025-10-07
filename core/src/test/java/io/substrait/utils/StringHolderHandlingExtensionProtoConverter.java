package io.substrait.utils;

import com.google.protobuf.Any;
import io.substrait.extension.ExtensionProtoConverter;

public class StringHolderHandlingExtensionProtoConverter
    extends ExtensionProtoConverter<StringHolder, StringHolder> {
  @Override
  protected Any toProto(final StringHolder holder) {
    return holder.toProto(null);
  }
}
