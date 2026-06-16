package io.substrait.plan;

import io.substrait.SubstraitVersion;
import io.substrait.extension.AdvancedExtension;
import io.substrait.relation.Rel;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/** A complete Substrait plan: a set of root relations together with version and metadata. */
@Value.Immutable
public abstract class Plan {

  /**
   * Returns the Substrait version this plan was produced for.
   *
   * @return the plan version
   */
  @Value.Default
  public Version getVersion() {
    return Version.DEFAULT_VERSION;
  }

  /**
   * Returns the root relations of the plan.
   *
   * @return the plan roots
   */
  public abstract List<Root> getRoots();

  /**
   * Returns the type URLs expected to be referenced by {@code Any} messages in the plan.
   *
   * @return the expected type URLs
   */
  public abstract List<String> getExpectedTypeUrls();

  /**
   * Returns the plan-level advanced extension, if present.
   *
   * @return the optional advanced extension
   */
  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  /**
   * Returns the execution behavior describing how the plan should be evaluated.
   *
   * @return the execution behavior
   */
  public abstract ExecutionBehavior getExecutionBehavior();

  /**
   * Validates that the execution behavior is properly configured.
   *
   * <p>This validation method ensures that:
   *
   * <ul>
   *   <li>The {@link ExecutionBehavior} field is present (not null or empty) - ExecutionBehavior is
   *       a required field
   *   <li>The {@link ExecutionBehavior.VariableEvaluationMode} is set to a valid value (not {@link
   *       ExecutionBehavior.VariableEvaluationMode#UNSPECIFIED})
   * </ul>
   *
   * @throws IllegalArgumentException if the execution behavior is not present, or if the variable
   *     evaluation mode is set to {@link ExecutionBehavior.VariableEvaluationMode#UNSPECIFIED}
   */
  @Value.Check
  protected void check() {
    ExecutionBehavior behavior = getExecutionBehavior();
    if (behavior.getVariableEvaluationMode()
        == ExecutionBehavior.VariableEvaluationMode.UNSPECIFIED) {
      throw new IllegalArgumentException(
          "ExecutionBehavior requires a specified VariableEvaluationMode, but got: "
              + behavior.getVariableEvaluationMode());
    }
  }

  /**
   * Creates a builder for {@link Plan}.
   *
   * @return a new builder
   */
  public static ImmutablePlan.Builder builder() {
    return ImmutablePlan.builder();
  }

  /** The Substrait version a plan was produced for, plus optional producer metadata. */
  @Value.Immutable
  public abstract static class Version {
    /** The version of the Substrait specification this build of substrait-java targets. */
    public static final Version DEFAULT_VERSION;

    static {
      DEFAULT_VERSION = loadVersion();
    }

    /**
     * Returns the major version number.
     *
     * @return the major version
     */
    public abstract int getMajor();

    /**
     * Returns the minor version number.
     *
     * @return the minor version
     */
    public abstract int getMinor();

    /**
     * Returns the patch version number.
     *
     * @return the patch version
     */
    public abstract int getPatch();

    /**
     * Returns the git hash of the producer, if known.
     *
     * @return the optional git hash
     */
    public abstract Optional<String> getGitHash();

    /**
     * Returns the name of the producer that created the plan, if known.
     *
     * @return the optional producer name
     */
    public abstract Optional<String> getProducer();

    /**
     * Creates a builder for {@link Version}.
     *
     * @return a new builder
     */
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

  /** A root relation of a plan together with the output field names it exposes. */
  @Value.Immutable
  public abstract static class Root {
    /**
     * Returns the relation producing this root's output.
     *
     * @return the input relation
     */
    public abstract Rel getInput();

    /**
     * Returns the output field names exposed by this root.
     *
     * @return the output field names
     */
    public abstract List<String> getNames();

    /**
     * Creates a builder for {@link Root}.
     *
     * @return a new builder
     */
    public static ImmutableRoot.Builder builder() {
      return ImmutableRoot.builder();
    }
  }

  /** Describes how a plan should be evaluated at execution time. */
  @Value.Immutable
  public abstract static class ExecutionBehavior {
    /**
     * Returns the mode controlling how often variables are evaluated.
     *
     * @return the variable evaluation mode
     */
    public abstract VariableEvaluationMode getVariableEvaluationMode();

    /**
     * Creates a builder for {@link ExecutionBehavior}.
     *
     * @return a new builder
     */
    public static ImmutableExecutionBehavior.Builder builder() {
      return ImmutableExecutionBehavior.builder();
    }

    /** Controls how often variable expressions are evaluated during plan execution. */
    public enum VariableEvaluationMode {
      /** Unspecified evaluation mode. */
      UNSPECIFIED(
          io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
              .VARIABLE_EVALUATION_MODE_UNSPECIFIED),
      /** Variables are evaluated once per plan execution. */
      PER_PLAN(
          io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
              .VARIABLE_EVALUATION_MODE_PER_PLAN),
      /** Variables are evaluated for each record. */
      PER_RECORD(
          io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
              .VARIABLE_EVALUATION_MODE_PER_RECORD);

      private final io.substrait.proto.ExecutionBehavior.VariableEvaluationMode proto;

      VariableEvaluationMode(io.substrait.proto.ExecutionBehavior.VariableEvaluationMode proto) {
        this.proto = proto;
      }

      /**
       * Converts this variable evaluation mode to its protobuf representation.
       *
       * @return the protobuf representation
       */
      public io.substrait.proto.ExecutionBehavior.VariableEvaluationMode toProto() {
        return proto;
      }
    }
  }
}
