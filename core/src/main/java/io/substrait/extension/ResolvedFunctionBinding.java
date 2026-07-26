package io.substrait.extension;

import io.substrait.expression.FunctionOption;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A fully-resolved binding of an extension function declaration to concrete arguments.
 *
 * <p>The binding captures the <em>semantic identity</em> of a function invocation: the function
 * anchor, the ordered arguments and the options. The {@linkplain #outputType() output type} is
 * <em>derived</em> from the declaration on demand (see {@link FunctionBindingResolver}) and is
 * deliberately not part of this identity — it belongs to the type-carrying Calcite wrapper, not to
 * the semantic identity of the function. Two bindings are equal when they agree on anchor,
 * arguments and options, which is what lets a consumer distinguish two functions that happen to
 * lower to the same engine operator.
 */
@Value.Immutable
public abstract class ResolvedFunctionBinding {

  /**
   * Returns the canonical function anchor (extension urn + compound signature).
   *
   * @return the function anchor
   */
  public abstract SimpleExtension.FunctionAnchor anchor();

  /**
   * Returns the ordered, kind-aware arguments the function was resolved against.
   *
   * @return the resolved arguments
   */
  public abstract List<ResolvedArgument> arguments();

  /**
   * Returns the options exactly as the invocation carries them: named preference lists, in order,
   * spelled the way the plan spelled them. Selecting a single value from a preference list is a
   * consumer-specific concern and is intentionally not done here.
   *
   * <p>This is a list rather than a map by name because that is what a plan carries — an option
   * name may legitimately appear more than once, and a map would silently drop all but one.
   *
   * @return the function options
   */
  public abstract List<FunctionOption> options();

  /**
   * Returns the preference list of the named option, matched case-insensitively as the Substrait
   * spec requires.
   *
   * @param name the option name
   * @return the preferred values of the first option with that name, if any
   */
  public Optional<List<String>> option(String name) {
    for (FunctionOption option : options()) {
      if (option.getName().equalsIgnoreCase(name)) {
        return Optional.of(option.values());
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the function declaration this binding resolves. Not part of the binding identity.
   *
   * @return the function declaration
   */
  @Value.Auxiliary
  public abstract SimpleExtension.Function declaration();

  /**
   * Derives the output type from the declaration and arguments, applying the declaration's
   * nullability policy. Computed on demand (may throw for as-yet-unsupported derivations); it is
   * neither stored nor part of the binding's identity.
   *
   * @return the derived output type
   */
  public Type outputType() {
    return FunctionBindingResolver.deriveOutputType(declaration(), arguments());
  }

  /**
   * Enforces that the anchor is consistent with the declaration.
   *
   * <p>Options need no normalization: {@link FunctionOption} is itself immutable, so the binding is
   * immutable all the way down and its identity cannot change after it was built. Names and values
   * are kept exactly as the plan spelled them — a binding is also the record of what to convert
   * back, so it must not rewrite the plan's data. Identity is therefore case-sensitive and merely
   * conservative: two invocations that differ only in the case of an option stay distinct, which
   * costs a missed deduplication and never loses information. Validation against the declaration is
   * case-insensitive, as the spec requires.
   */
  @Value.Check
  protected void checkAnchor() {
    if (!anchor().equals(declaration().getAnchor())) {
      throw new IllegalArgumentException(
          String.format(
              "anchor %s does not match declaration anchor %s",
              anchor(), declaration().getAnchor()));
    }
  }

  /**
   * Creates a builder for {@link ResolvedFunctionBinding}. Prefer {@link FunctionBindingResolver},
   * which validates the signature and options before constructing a binding.
   *
   * @return a new builder
   */
  public static ImmutableResolvedFunctionBinding.Builder builder() {
    return ImmutableResolvedFunctionBinding.builder();
  }
}
