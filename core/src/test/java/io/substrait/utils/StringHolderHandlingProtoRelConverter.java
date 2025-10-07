package io.substrait.utils;

import com.google.protobuf.Any;
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
    super(lookup, extensions, new StringHolderHandlingProtoExtensionConverter());
  }

  @Override
  protected Extension.LeafRelDetail detailFromExtensionLeafRel(Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Extension.SingleRelDetail detailFromExtensionSingleRel(Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Extension.MultiRelDetail detailFromExtensionMultiRel(Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Extension.ExtensionTableDetail detailFromExtensionTable(Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Extension.WriteExtensionObject detailFromWriteExtensionObject(Any any) {
    return StringHolder.fromProto(any);
  }

  @Override
  protected Extension.DdlExtensionObject detailFromDdlExtensionObject(Any any) {
    return StringHolder.fromProto(any);
  }
}
