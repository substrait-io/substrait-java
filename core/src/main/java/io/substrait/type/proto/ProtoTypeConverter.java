package io.substrait.type.proto;

import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

/** Converts from {@link io.substrait.proto.Type} to {@link io.substrait.type.Type} */
public class ProtoTypeConverter {

  private final ExtensionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;

  public ProtoTypeConverter(
      ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
    this.lookup = lookup;
    this.extensions = extensions;
  }

  public Type from(io.substrait.proto.Type type) {
    return switch (type.getKindCase()) {
      case BOOL -> n(type.getBool().getNullability()).BOOLEAN;
      case I8 -> n(type.getI8().getNullability()).I8;
      case I16 -> n(type.getI16().getNullability()).I16;
      case I32 -> n(type.getI32().getNullability()).I32;
      case I64 -> n(type.getI64().getNullability()).I64;
      case FP32 -> n(type.getFp32().getNullability()).FP32;
      case FP64 -> n(type.getFp64().getNullability()).FP64;
      case STRING -> n(type.getString().getNullability()).STRING;
      case BINARY -> n(type.getBinary().getNullability()).BINARY;
      case TIMESTAMP -> n(type.getTimestamp().getNullability()).TIMESTAMP;
      case DATE -> n(type.getDate().getNullability()).DATE;
      case TIME -> n(type.getTime().getNullability()).TIME;
      case INTERVAL_YEAR -> n(type.getIntervalYear().getNullability()).INTERVAL_YEAR;
      case INTERVAL_DAY -> n(type.getIntervalDay().getNullability())
          // precision defaults to 6 (micros) for backwards compatibility, see protobuf
          .intervalDay(
              type.getIntervalDay().hasPrecision() ? type.getIntervalDay().getPrecision() : 6);
      case INTERVAL_COMPOUND -> n(type.getIntervalCompound().getNullability())
          .intervalCompound(type.getIntervalCompound().getPrecision());
      case TIMESTAMP_TZ -> n(type.getTimestampTz().getNullability()).TIMESTAMP_TZ;
      case UUID -> n(type.getUuid().getNullability()).UUID;
      case FIXED_CHAR -> n(type.getFixedChar().getNullability())
          .fixedChar(type.getFixedChar().getLength());
      case VARCHAR -> n(type.getVarchar().getNullability()).varChar(type.getVarchar().getLength());
      case FIXED_BINARY -> n(type.getFixedBinary().getNullability())
          .fixedBinary(type.getFixedBinary().getLength());
      case DECIMAL -> n(type.getDecimal().getNullability())
          .decimal(type.getDecimal().getPrecision(), type.getDecimal().getScale());
      case PRECISION_TIMESTAMP -> n(type.getPrecisionTimestamp().getNullability())
          .precisionTimestamp(type.getPrecisionTimestamp().getPrecision());
      case PRECISION_TIMESTAMP_TZ -> n(type.getPrecisionTimestampTz().getNullability())
          .precisionTimestampTZ(type.getPrecisionTimestampTz().getPrecision());
      case STRUCT -> n(type.getStruct().getNullability())
          .struct(
              type.getStruct().getTypesList().stream()
                  .map(this::from)
                  .collect(java.util.stream.Collectors.toList()));
      case LIST -> fromList(type.getList());
      case MAP -> n(type.getMap().getNullability())
          .map(from(type.getMap().getKey()), from(type.getMap().getValue()));
      case USER_DEFINED -> {
        var userDefined = type.getUserDefined();
        var t = lookup.getType(userDefined.getTypeReference(), extensions);
        yield n(userDefined.getNullability()).userDefined(t.uri(), t.name());
      }
      case USER_DEFINED_TYPE_REFERENCE -> throw new UnsupportedOperationException(
          "Unsupported user defined reference: " + type);
      case KIND_NOT_SET -> throw new UnsupportedOperationException("Type is not set: " + type);
    };
  }

  public Type.ListType fromList(io.substrait.proto.Type.List list) {
    return n(list.getNullability()).list(from(list.getType()));
  }

  public static boolean isNullable(io.substrait.proto.Type.Nullability nullability) {
    return io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE == nullability;
  }

  private static TypeCreator n(io.substrait.proto.Type.Nullability n) {
    return n == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE
        ? TypeCreator.NULLABLE
        : TypeCreator.REQUIRED;
  }
}
