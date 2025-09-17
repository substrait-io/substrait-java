package io.substrait.isthmus;

import java.util.List;

public class ValidationResult {
  public static final ValidationResult SUCCESS = ok();
  private final boolean isValid;
  private final List<String> messages;

  public ValidationResult(boolean isValid, List<String> messages) {
    this.isValid = isValid;
    this.messages = messages;
  }

  public static ValidationResult ok() {
    return new ValidationResult(true, List.of("OK"));
  }

  public static ValidationResult error(List<String> messages) {
    return new ValidationResult(false, messages);
  }

  public boolean isValid() {
    return isValid;
  }

  public List<String> getMessages() {
    return messages;
  }
}
