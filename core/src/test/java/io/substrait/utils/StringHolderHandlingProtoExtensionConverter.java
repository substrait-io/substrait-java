package io.substrait.utils;

import com.google.protobuf.Any;
import io.substrait.extension.AdvancedExtension.Enhancement;
import io.substrait.extension.AdvancedExtension.Optimization;
import io.substrait.extension.ProtoExtensionConverter;

public class StringHolderHandlingProtoExtensionConverter extends ProtoExtensionConverter {
  @Override
  protected Enhancement enhancementFromAdvancedExtension(final Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Optimization optimizationFromAdvancedExtension(final Any any) {
    return StringHolder.fromProto(any);
  }
}
