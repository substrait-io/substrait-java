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

  public abstract Optional<ExecutionBehavior> getExecutionBehavior();

  /**
   * Validates that the execution behavior is properly configured.
   *
   * <p>This validation method ensures that:
   *
   * <ul>
   *   <li>The {@link ExecutionBehavior} field is present (not null or empty) - ExecutionBehavior is
   *       a required field
   *   <li>The {@link ExecutionBehavior.VariableEvaluationMode} is set to a valid value (not {@link
   *       ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_UNSPECIFIED})
   * </ul>
   *
   * @throws IllegalArgumentException if the execution behavior is not present, or if the variable
   *     evaluation mode is set to {@link
   *     ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_UNSPECIFIED}
   */
  @Value.Check
  protected void check() {
    if (!getExecutionBehavior().isPresent()) {
      throw new IllegalArgumentException("ExecutionBehavior is required but was not set");
    }
    ExecutionBehavior behavior = getExecutionBehavior().get();
    if (behavior.getVariableEvaluationMode()
        == ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_UNSPECIFIED) {
      throw new IllegalArgumentException(
          "ExecutionBehavior requires a specified VariableEvaluationMode, but got: "
              + behavior.getVariableEvaluationMode());
    }
  }

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

  @Value.Immutable
  public abstract static class ExecutionBehavior {
    public abstract VariableEvaluationMode getVariableEvaluationMode();

    public static ImmutableExecutionBehavior.Builder builder() {
      return ImmutableExecutionBehavior.builder();
    }

    public enum VariableEvaluationMode {
      VARIABLE_EVALUATION_MODE_UNSPECIFIED,
      VARIABLE_EVALUATION_MODE_PER_PLAN,
      VARIABLE_EVALUATION_MODE_PER_RECORD
    }
  }
}
