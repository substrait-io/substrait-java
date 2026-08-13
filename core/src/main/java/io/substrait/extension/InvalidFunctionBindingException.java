package io.substrait.extension;

import io.substrait.type.Type;

/**
 * Thrown when an extension function invocation cannot be resolved, or when the output type declared
 * by a plan is inconsistent with the type derived from the function's extension declaration.
 *
 * <p>Resolution is fail-closed: rather than trusting a plan-supplied output type, the resolver
 * derives the type from the declaration and raises this exception when they disagree.
 */
public class InvalidFunctionBindingException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception with the given message.
   *
   * @param message the detail message
   */
  public InvalidFunctionBindingException(String message) {
    super(message);
  }

  /**
   * Creates an exception with the given message and cause.
   *
   * @param message the detail message
   * @param cause the underlying failure
   */
  public InvalidFunctionBindingException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates an exception describing a mismatch between the declared and the derived output type.
   *
   * @param anchor the function anchor being resolved
   * @param declared the output type declared by the plan
   * @param derived the output type derived from the declaration
   * @return the exception
   */
  public static InvalidFunctionBindingException outputTypeMismatch(
      SimpleExtension.FunctionAnchor anchor, Type declared, Type derived) {
    return new InvalidFunctionBindingException(
        String.format(
            "Declared output type %s for %s does not match the type %s derived from its declaration",
            declared, anchor, derived));
  }
}
