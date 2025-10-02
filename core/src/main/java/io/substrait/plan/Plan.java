package io.substrait.plan;

import io.substrait.SubstraitVersion;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Rel;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Plan {

  @Value.Default
  public Version getVersion() {
    return Version.DEFAULT_VERSION;
  }

  public abstract List<Root> getRoots();

  public abstract List<String> getExpectedTypeUrls();

  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  public static ImmutablePlan.Builder builder() {
    return ImmutablePlan.builder();
  }

  @Value.Immutable
  public abstract static class Version {
    public static final Version DEFAULT_VERSION;

    static {
      DEFAULT_VERSION = loadVersion();
    }

    public abstract int getMajor();

    public abstract int getMinor();

    public abstract int getPatch();

    public abstract Optional<String> getGitHash();

    public abstract Optional<String> getProducer();

    public static ImmutableVersion.Builder builder() {
      return ImmutableVersion.builder();
    }

    private static Version loadVersion() {
      final String[] versionComponents = SubstraitVersion.VERSION.split("\\.");

      return builder()
          .major(Integer.parseInt(versionComponents[0]))
          .minor(Integer.parseInt(versionComponents[1]))
          .patch(Integer.parseInt(versionComponents[2]))
          .producer(Optional.of("substrait-java"))
          .build();
    }
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
