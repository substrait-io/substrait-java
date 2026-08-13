package io.substrait.isthmus;

import java.util.Objects;

/**
 * Configures how the output type of a converted Substrait aggregate is chosen and validated.
 *
 * <p>The two settings are independent: {@link OutputTypeSource} decides which type the resulting
 * Calcite {@code AggregateCall} carries, and {@link FunctionBindingValidation} decides whether the
 * plan-declared type is checked against the extension declaration.
 *
 * <p>The {@link #DEFAULT} is {@link OutputTypeSource#PLAN_OUTPUT} with {@link
 * FunctionBindingValidation#NONE}: the plan's declared output type is preserved (so conversion
 * never silently changes a type) without asserting that the plan is spec-compliant.
 */
public final class AggregateConversion {

  /** Where the Calcite output type of a converted aggregate comes from. */
  public enum OutputTypeSource {
    /** Use the type Calcite infers for the operator (ignores the plan's declared type). */
    CALCITE_INFERENCE,
    /** Preserve the {@code AggregateFunction.output_type} declared by the plan. */
    PLAN_OUTPUT
  }

  /** Whether the plan-declared output type is validated against the extension declaration. */
  public enum FunctionBindingValidation {
    /** Do not validate the declared output type. */
    NONE,
    /**
     * Require the declared output type to match the type derived from the extension declaration.
     *
     * <p>Type derivation is fail-closed: a function whose return expression the derivation does not
     * yet support is rejected rather than assumed valid, so this mode is not adoptable for plans
     * that use such functions. On the standard extension catalog the unsupported shapes are the
     * parameterized type classes other than decimal — {@code varchar<L1>}, {@code fixedchar<L1>},
     * {@code precision_time<P>}, {@code precision_timestamp<P>}, {@code precision_timestamp_tz<P>},
     * {@code interval_day<P>}, {@code list<anyN>}, parameterized structs — and multi-line return
     * programs; for example {@code concat}, {@code concat_ws}, {@code assume_timezone} and the
     * {@code strptime_*} family are rejected today. {@code quantile}'s output type cannot be
     * derived at all: its declared return {@code LIST?<any>} uses a plain {@code any}, which
     * carries no identity to bind (spec v0.99.0).
     */
    EXTENSION_DECLARATION
  }

  /** Preserve the plan's output type without validating it against the declaration. */
  public static final AggregateConversion DEFAULT =
      new AggregateConversion(OutputTypeSource.PLAN_OUTPUT, FunctionBindingValidation.NONE);

  private final OutputTypeSource outputTypeSource;
  private final FunctionBindingValidation bindingValidation;

  /**
   * Creates a configuration.
   *
   * @param outputTypeSource where the Calcite output type comes from
   * @param bindingValidation whether the declared type is validated against the declaration
   */
  public AggregateConversion(
      OutputTypeSource outputTypeSource, FunctionBindingValidation bindingValidation) {
    this.outputTypeSource = Objects.requireNonNull(outputTypeSource);
    this.bindingValidation = Objects.requireNonNull(bindingValidation);
  }

  /**
   * Returns the output-type source.
   *
   * @return the output-type source
   */
  public OutputTypeSource outputTypeSource() {
    return outputTypeSource;
  }

  /**
   * Returns the binding-validation policy.
   *
   * @return the binding-validation policy
   */
  public FunctionBindingValidation bindingValidation() {
    return bindingValidation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AggregateConversion)) {
      return false;
    }
    AggregateConversion other = (AggregateConversion) o;
    return outputTypeSource == other.outputTypeSource
        && bindingValidation == other.bindingValidation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(outputTypeSource, bindingValidation);
  }

  @Override
  public String toString() {
    return "AggregateConversion[" + outputTypeSource + ", " + bindingValidation + "]";
  }
}
