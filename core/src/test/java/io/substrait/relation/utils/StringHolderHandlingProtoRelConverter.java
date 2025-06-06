package io.substrait.relation.utils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Extension;
import io.substrait.relation.ProtoRelConverter;

/**
 * Extends {@link ProtoRelConverter} with conversions from {@link com.google.protobuf.Any} to {@link
 * StringHolder}
 *
 * <p>Used to verify serde of {@link com.google.protobuf.Any} fields in the spec
 */
public class StringHolderHandlingProtoRelConverter extends ProtoRelConverter {
  public StringHolderHandlingProtoRelConverter(
      ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    super(lookup, extensions);
  }

  StringHolder asStringHolder(Any any) {
    try {
      return new StringHolder(any.unpack(StringValue.class).getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Extension.Enhancement enhancementFromAdvancedExtension(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.Optimization optimizationFromAdvancedExtension(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.LeafRelDetail detailFromExtensionLeafRel(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.SingleRelDetail detailFromExtensionSingleRel(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.MultiRelDetail detailFromExtensionMultiRel(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.ExtensionTableDetail detailFromExtensionTable(Any any) {
    return asStringHolder(any);
  }

  @Override
  protected Extension.WriteExtensionObject detailFromWriteExtensionObject(Any any) {
    return asStringHolder(any);
  }
}
