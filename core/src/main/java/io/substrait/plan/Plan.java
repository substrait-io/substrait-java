package io.substrait.plan;

import io.substrait.proto.AdvancedExtension;
import io.substrait.relation.Rel;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.jar.Attributes.Name;
import java.util.jar.Manifest;
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
      final String[] versionComponents = loadVersion();
      DEFAULT_VERSION =
          ImmutableVersion.builder()
              .major(Integer.parseInt(versionComponents[0]))
              .minor(Integer.parseInt(versionComponents[1]))
              .patch(Integer.parseInt(versionComponents[2]))
              .producer(Optional.of("substrait-java"))
              .build();
    }

    public abstract int getMajor();

    public abstract int getMinor();

    public abstract int getPatch();

    public abstract Optional<String> getGitHash();

    public abstract Optional<String> getProducer();

    private static String[] loadVersion() {
      // load the specification version from the JAR manifest
      String specificationVersion = Version.class.getPackage().getSpecificationVersion();

      // load the manifest directly from the classpath if the specification version is null which is
      // the case if the Version class is not in a JAR, e.g. during the Gradle build
      if (specificationVersion == null) {
        try {
          Manifest manifest =
              new Manifest(
                  Version.class.getClassLoader().getResourceAsStream("META-INF/MANIFEST.MF"));
          specificationVersion = manifest.getMainAttributes().getValue(Name.SPECIFICATION_VERSION);
        } catch (IOException e) {
          throw new IllegalStateException("Could not load version from manifest", e);
        }
      }

      if (specificationVersion == null) {
        specificationVersion = "0.0.0";
      }

      return specificationVersion.split("\\.");
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
