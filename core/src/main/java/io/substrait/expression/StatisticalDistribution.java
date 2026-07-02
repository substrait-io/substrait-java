package io.substrait.expression;

/**
 * The {@code distribution} enum argument of the Substrait {@code std_dev} and {@code variance}
 * aggregate functions.
 *
 * <p>Distinguishes between the sample (n-1 denominator, Bessel's correction) and population (n
 * denominator) variants. The enum constant names match the Substrait extension's enum option names
 * ({@code SAMPLE} / {@code POPULATION}), so {@link #name()} yields the value used to build an
 * {@link EnumArg}.
 */
public enum StatisticalDistribution {
  /** Sample distribution (uses the n-1 denominator, Bessel's correction). */
  SAMPLE,
  /** Population distribution (uses the n denominator). */
  POPULATION
}
