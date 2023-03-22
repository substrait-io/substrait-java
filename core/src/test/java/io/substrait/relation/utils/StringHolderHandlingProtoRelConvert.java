package io.substrait.relation.utils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.substrait.expression.FunctionLookup;
import io.substrait.function.SimpleExtension;
import io.substrait.relation.Extension;
import io.substrait.relation.ProtoRelConverter;

/**
 * Extends {@link ProtoRelConverter} with conversions from {@link com.google.protobuf.Any} to {@link
 * StringHolder}
 *
 * <p>Used to verify serde of {@link com.google.protobuf.Any} fields in the spec
 */
public class StringHolderHandlingProtoRelConvert extends ProtoRelConverter {
  public StringHolderHandlingProtoRelConvert(
      FunctionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    super(lookup, extensions);
  }

  @Override
  protected Extension.Enhancement enhancementFromAdvancedExtension(Any any) {
    try {
      return new StringHolder(any.unpack(StringValue.class).getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Extension.Optimization optimizationFromAdvancedExtension(Any any) {
    try {
      return new StringHolder(any.unpack(StringValue.class).getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
