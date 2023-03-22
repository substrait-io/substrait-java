package io.substrait.relation;

import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;

/** Contains tag interfaces for handling {@link com.google.protobuf.Any} types within Substrait. */
public class Extension {

  private interface ToProto {
    com.google.protobuf.Any toProto();
  }

  public interface Optimization extends ToProto {}

  public interface Enhancement extends ToProto {}

  public interface LeafRelDetail extends ToProto {
    Type.Struct deriveRecordType();
  }

  public interface SingleRelDetail extends ToProto {
    Type.Struct deriveRecordType(Rel input);
  }

  public interface MultiRelDetail extends ToProto {
    Type.Struct deriveRecordType(List<Rel> inputs);
  }

  public interface ExtensionTableDetail extends ToProto {
    NamedStruct deriveSchema();
  }
}
