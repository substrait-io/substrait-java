package io.substrait.plan;

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
}
