package io.substrait.type;

import io.substrait.type.proto.TypeProtoConverter;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface NamedStruct {
  Type.Struct struct();

  List<String> names();

  static NamedStruct of(Iterable<String> names, Type.Struct type) {
    return ImmutableNamedStruct.builder().addAllNames(names).struct(type).build();
  }

  default io.substrait.proto.NamedStruct toProto() {
    var type = struct().accept(TypeProtoConverter.INSTANCE);
    return io.substrait.proto.NamedStruct.newBuilder()
        .setStruct(type.getStruct())
        .addAllNames(names())
        .build();
  }
}
