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
      final ExtensionLookup lookup, final SimpleExtension.ExtensionCollection extensions) {
    this.lookup = lookup;
    this.extensions = extensions;
  }

  public Type from(final io.substrait.proto.Type type) {
    switch (type.getKindCase()) {
      case BOOL:
        return n(type.getBool().getNullability()).BOOLEAN;
      case I8:
        return n(type.getI8().getNullability()).I8;
      case I16:
        return n(type.getI16().getNullability()).I16;
      case I32:
        return n(type.getI32().getNullability()).I32;
      case I64:
        return n(type.getI64().getNullability()).I64;
      case FP32:
        return n(type.getFp32().getNullability()).FP32;
      case FP64:
        return n(type.getFp64().getNullability()).FP64;
      case STRING:
        return n(type.getString().getNullability()).STRING;
      case BINARY:
        return n(type.getBinary().getNullability()).BINARY;
      case TIMESTAMP:
        return n(type.getTimestamp().getNullability()).TIMESTAMP;
      case DATE:
        return n(type.getDate().getNullability()).DATE;
      case TIME:
        return n(type.getTime().getNullability()).TIME;
      case INTERVAL_YEAR:
        return n(type.getIntervalYear().getNullability()).INTERVAL_YEAR;
      case INTERVAL_DAY:
        return n(type.getIntervalDay().getNullability())
            // precision defaults to 6 (micros) for backwards compatibility, see protobuf
            .intervalDay(
                type.getIntervalDay().hasPrecision() ? type.getIntervalDay().getPrecision() : 6);
      case INTERVAL_COMPOUND:
        return n(type.getIntervalCompound().getNullability())
            .intervalCompound(type.getIntervalCompound().getPrecision());
      case TIMESTAMP_TZ:
        return n(type.getTimestampTz().getNullability()).TIMESTAMP_TZ;
      case UUID:
        return n(type.getUuid().getNullability()).UUID;
      case FIXED_CHAR:
        return n(type.getFixedChar().getNullability()).fixedChar(type.getFixedChar().getLength());
      case VARCHAR:
        return n(type.getVarchar().getNullability()).varChar(type.getVarchar().getLength());
      case FIXED_BINARY:
        return n(type.getFixedBinary().getNullability())
            .fixedBinary(type.getFixedBinary().getLength());
      case DECIMAL:
        return n(type.getDecimal().getNullability())
            .decimal(type.getDecimal().getPrecision(), type.getDecimal().getScale());
      case PRECISION_TIME:
        return n(type.getPrecisionTime().getNullability())
            .precisionTime(type.getPrecisionTime().getPrecision());
      case PRECISION_TIMESTAMP:
        return n(type.getPrecisionTimestamp().getNullability())
            .precisionTimestamp(type.getPrecisionTimestamp().getPrecision());
      case PRECISION_TIMESTAMP_TZ:
        return n(type.getPrecisionTimestampTz().getNullability())
            .precisionTimestampTZ(type.getPrecisionTimestampTz().getPrecision());
      case STRUCT:
        return n(type.getStruct().getNullability())
            .struct(
                type.getStruct().getTypesList().stream()
                    .map(this::from)
                    .collect(java.util.stream.Collectors.toList()));
      case LIST:
        return fromList(type.getList());
      case MAP:
        return fromMap(type.getMap());
      case USER_DEFINED:
        {
          final io.substrait.proto.Type.UserDefined userDefined = type.getUserDefined();
          final SimpleExtension.Type t = lookup.getType(userDefined.getTypeReference(), extensions);
          return n(userDefined.getNullability()).userDefined(t.urn(), t.name());
        }
      case USER_DEFINED_TYPE_REFERENCE:
        throw new UnsupportedOperationException("Unsupported user defined reference: " + type);
      case KIND_NOT_SET:
        throw new UnsupportedOperationException("Type is not set: " + type);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type.getKindCase());
    }
  }

  public Type.ListType fromList(final io.substrait.proto.Type.List list) {
    return n(list.getNullability()).list(from(list.getType()));
  }

  public Type.Map fromMap(final io.substrait.proto.Type.Map map) {
    return n(map.getNullability()).map(from(map.getKey()), from(map.getValue()));
  }

  public static boolean isNullable(final io.substrait.proto.Type.Nullability nullability) {
    return io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE == nullability;
  }

  private static TypeCreator n(final io.substrait.proto.Type.Nullability n) {
    return n == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE
        ? TypeCreator.NULLABLE
        : TypeCreator.REQUIRED;
  }
}
