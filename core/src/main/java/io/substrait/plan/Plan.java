package io.substrait.plan;

import io.substrait.SubstraitVersion;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Rel;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(Root.class);

    public abstract Rel getInput();

    public abstract List<String> getNames();

    @Value.Check
    protected void check() {
      final int actualNameCount = getNames().size();
      if (actualNameCount == 0) {
        LOGGER.warn(
            "Plan.Root built without output names; this will be an error in the next release");
        return;
      }

      final int expectedFieldCount =
          NamedFieldCountingTypeVisitor.countNames(getInput().getRecordType());
      if (actualNameCount != expectedFieldCount) {
        throw new IllegalArgumentException(
            String.format(
                "Plan.Root names count (%d) must match input record type depth-first named-field count (%d)",
                actualNameCount, expectedFieldCount));
      }
    }

    public static ImmutableRoot.Builder builder() {
      return ImmutableRoot.builder();
    }
  }
}
