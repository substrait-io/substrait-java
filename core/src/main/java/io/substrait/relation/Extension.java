package io.substrait.relation;

/** Contains tag interfaces for handling {@link com.google.protobuf.Any} types within Substrait. */
public class Extension {

  private interface ToProto {
    com.google.protobuf.Any toProto();
  }

  public interface Optimization extends ToProto {}

  public interface Enhancement extends ToProto {}

  public interface LeafRelDetail extends ToProto {}

  public interface SingleRelDetail extends ToProto {}

  public interface MultiRelDetail extends ToProto {}
}
