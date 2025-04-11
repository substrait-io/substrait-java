package io.substrait.plan;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.AdvancedExtension;
import io.substrait.relation.Rel;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Plan {

  public abstract List<Root> getRoots();

  public abstract List<String> getExpectedTypeUrls();

  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  public static ImmutablePlan.Builder builder() {
    return ImmutablePlan.builder();
  }

  @Value.Immutable
  public abstract static class Root {
    public abstract Rel getInput();

    public abstract List<String> getNames();

    public static ImmutableRoot.Builder builder() {
      return ImmutableRoot.builder();
    }
  }

  /**
   * Serializes this plan as protobuf.
   *
   * @return this plan in protobuf format
   */
  public io.substrait.proto.Plan toProto() {
    return new PlanProtoConverter().toProto(this);
  }

  /**
   * Serializes this plan as a protobuf JSON string.
   *
   * @return this plan as a protobuf JSON string
   */
  public String toJsonString() {
    try {
      return JsonFormat.printer().includingDefaultValueFields().print(this.toProto());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Can not generate JSON from proto.", e);
    }
  }
}
