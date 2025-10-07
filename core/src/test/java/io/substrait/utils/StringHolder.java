package io.substrait.utils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * For use in {@link io.substrait.relation.ProtoRelConverterTest} and {@link
 * io.substrait.type.proto.ExtensionRoundtripTest}
 *
 * <p>Used to verify serde of {@link com.google.protobuf.Any} fields in the spec.
 */
public class StringHolder
    implements AdvancedExtension.Enhancement,
        AdvancedExtension.Optimization,
        Extension.LeafRelDetail,
        Extension.SingleRelDetail,
        Extension.MultiRelDetail,
        Extension.ExtensionTableDetail,
        Extension.WriteExtensionObject,
        Extension.DdlExtensionObject {

  private static final String PROTO_TYPE_URL = "type.googleapis.com/google.protobuf.StringValue";

  private final String value;

  public StringHolder(String value) {
    this.value = value;
  }

  public static StringHolder fromProto(final Any any) {
    try {
      if (PROTO_TYPE_URL.equals(any.getTypeUrl())) {
        return new StringHolder(any.unpack(StringValue.class).getValue());
      }
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }

    throw new IllegalArgumentException(
        String.format("Missing handler for protobuf with type URL: %s", any.getTypeUrl()));
  }

  @Override
  public Any toProto(RelProtoConverter relProtoConverter) {
    return com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of(this.value));
  }

  @Override
  public Type.Struct deriveRecordType() {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public Type.Struct deriveRecordType(Rel input) {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public Type.Struct deriveRecordType(List<Rel> inputs) {
    return TypeCreator.NULLABLE.struct();
  }

  @Override
  public NamedStruct deriveSchema() {
    return NamedStruct.of(Collections.emptyList(), Type.Struct.builder().nullable(true).build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringHolder that = (StringHolder) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
