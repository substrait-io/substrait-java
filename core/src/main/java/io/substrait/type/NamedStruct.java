package io.substrait.type;

import io.substrait.type.proto.ProtoTypeConverter;
import io.substrait.type.proto.TypeProtoConverter;
import java.util.List;
import org.immutables.value.Value;

/**
 * A struct type paired with the flattened list of field names that label its (possibly nested)
 * fields.
 */
@Value.Immutable
public interface NamedStruct {
  /**
   * Returns the struct type describing the field types.
   *
   * @return the struct type
   */
  Type.Struct struct();

  /**
   * Returns the flattened field names, in depth-first order over the struct's fields.
   *
   * @return the field names
   */
  List<String> names();

  /**
   * Creates a builder for {@link NamedStruct}.
   *
   * @return a new builder
   */
  static ImmutableNamedStruct.Builder builder() {
    return ImmutableNamedStruct.builder();
  }

  /**
   * Creates a {@link NamedStruct} from the given names and struct type.
   *
   * @param names the flattened field names
   * @param type the struct type
   * @return the named struct
   */
  static NamedStruct of(Iterable<String> names, Type.Struct type) {
    return ImmutableNamedStruct.builder().addAllNames(names).struct(type).build();
  }

  /**
   * Converts this named struct to its protobuf representation.
   *
   * @param typeProtoConverter converter used to render the struct type as proto
   * @return the protobuf {@link io.substrait.proto.NamedStruct}
   */
  default io.substrait.proto.NamedStruct toProto(TypeProtoConverter typeProtoConverter) {
    io.substrait.proto.Type type = struct().accept(typeProtoConverter);
    return io.substrait.proto.NamedStruct.newBuilder()
        .setStruct(type.getStruct())
        .addAllNames(names())
        .build();
  }

  /**
   * Creates a {@link NamedStruct} from its protobuf representation.
   *
   * @param namedStruct the protobuf named struct to convert
   * @param protoTypeConverter converter used to convert the proto field types
   * @return the converted named struct
   */
  static io.substrait.type.NamedStruct fromProto(
      io.substrait.proto.NamedStruct namedStruct, ProtoTypeConverter protoTypeConverter) {
    io.substrait.proto.Type.Struct struct = namedStruct.getStruct();
    return ImmutableNamedStruct.builder()
        .names(namedStruct.getNamesList())
        .struct(
            Type.Struct.builder()
                .fields(
                    struct.getTypesList().stream()
                        .map(protoTypeConverter::from)
                        .collect(java.util.stream.Collectors.toList()))
                .nullable(ProtoTypeConverter.isNullable(struct.getNullability()))
                .build())
        .build();
  }
}
