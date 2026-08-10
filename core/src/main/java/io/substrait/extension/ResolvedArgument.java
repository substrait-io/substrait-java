package io.substrait.extension;

import io.substrait.type.Type;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * An ordered, kind-aware argument of a resolved function binding.
 *
 * <p>Unlike a bare {@link Type} list, this preserves the argument <em>kind</em> (value, type or
 * enum) and the selected enum option, so that two invocations that differ only by an enum argument
 * — e.g. {@code std_dev(POPULATION, fp32)} vs {@code std_dev(SAMPLE, fp32)} — are not conflated.
 *
 * <p>An enum argument may also carry <em>no</em> option, for a plan that leaves it unspecified;
 * that is distinct from any specified option. An option is kept exactly as the plan spelled it, so
 * identity is case-sensitive and merely conservative: two invocations differing only in the case of
 * an enum symbol stay distinct, which costs a missed deduplication and never rewrites the plan's
 * data. Matching an option against its declaration is case-insensitive, as the spec requires.
 */
public final class ResolvedArgument {

  /** The kind of a function argument. */
  public enum Kind {
    /** A value argument, carrying a data {@link Type}. */
    VALUE,
    /** A type argument, carrying a {@link Type}. */
    TYPE,
    /** An enum argument, carrying a selected option. */
    ENUM
  }

  private final Kind kind;
  private final @Nullable Type type;
  private final @Nullable String enumValue;

  private ResolvedArgument(Kind kind, @Nullable Type type, @Nullable String enumValue) {
    this.kind = kind;
    this.type = type;
    this.enumValue = enumValue;
  }

  /**
   * Creates a value argument.
   *
   * @param type the value type
   * @return the resolved argument
   */
  public static ResolvedArgument value(Type type) {
    return new ResolvedArgument(Kind.VALUE, Objects.requireNonNull(type), null);
  }

  /**
   * Creates a type argument.
   *
   * @param type the argument type
   * @return the resolved argument
   */
  public static ResolvedArgument type(Type type) {
    return new ResolvedArgument(Kind.TYPE, Objects.requireNonNull(type), null);
  }

  /**
   * Creates an enum argument.
   *
   * @param option the selected enum option
   * @return the resolved argument
   */
  public static ResolvedArgument enumOption(String option) {
    return new ResolvedArgument(Kind.ENUM, null, Objects.requireNonNull(option));
  }

  /**
   * Creates an enum argument whose option the plan left unspecified. This is distinct from an
   * argument carrying an empty option: the proto can spell it, so the binding keeps the plan's
   * identity faithful — but the extension schema cannot declare an optional enum argument, so
   * validation always rejects it.
   *
   * @return the resolved argument
   */
  public static ResolvedArgument unspecifiedEnumOption() {
    return new ResolvedArgument(Kind.ENUM, null, null);
  }

  /**
   * Returns the argument kind.
   *
   * @return the kind
   */
  public Kind kind() {
    return kind;
  }

  /**
   * Returns the data type of a value or type argument.
   *
   * @return the type, if this is a value or type argument
   */
  public Optional<Type> type() {
    return Optional.ofNullable(type);
  }

  /**
   * Returns the selected option of an enum argument.
   *
   * @return the enum option, or empty if this is not an enum argument or its option was left
   *     unspecified
   */
  public Optional<String> enumValue() {
    return Optional.ofNullable(enumValue);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResolvedArgument)) {
      return false;
    }
    ResolvedArgument other = (ResolvedArgument) o;
    return kind == other.kind
        && Objects.equals(type, other.type)
        && Objects.equals(enumValue, other.enumValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, type, enumValue);
  }

  @Override
  public String toString() {
    switch (kind) {
      case VALUE:
        return "value(" + type + ")";
      case TYPE:
        return "type(" + type + ")";
      default:
        return "enum(" + (enumValue == null ? "unspecified" : enumValue) + ")";
    }
  }
}
