package io.substrait.relation.utils;

import com.google.protobuf.Any;
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
    implements Extension.Enhancement,
        Extension.Optimization,
        Extension.LeafRelDetail,
        Extension.SingleRelDetail,
        Extension.MultiRelDetail,
        Extension.ExtensionTableDetail,
        Extension.WriteExtensionObject {

  private final String value;

  public StringHolder(String value) {
    this.value = value;
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
