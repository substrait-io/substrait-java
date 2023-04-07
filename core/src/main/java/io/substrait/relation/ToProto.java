package io.substrait.relation;

import com.google.protobuf.Message;

public interface ToProto<T extends Message> {
  T toProto();
}
