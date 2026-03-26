package io.substrait.relation;

import com.google.protobuf.Message;

/**
 * Interface for objects that can be converted to protobuf messages.
 *
 * @param <T> the protobuf message type
 */
public interface ToProto<T extends Message> {
  /**
   * Converts this object to its protobuf representation.
   *
   * @param converter the converter to use for nested conversions
   * @return the protobuf message
   */
  T toProto(RelProtoConverter converter);
}
