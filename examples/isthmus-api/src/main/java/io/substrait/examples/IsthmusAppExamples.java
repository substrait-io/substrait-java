package io.substrait.examples;

import java.util.Arrays;

/** Main class */
public final class IsthmusAppExamples {

  /** Implemented by all examples */
  @FunctionalInterface
  public interface Action {

    /**
     * Run
     *
     * @param args String []
     */
    void run(String[] args);
  }

  private IsthmusAppExamples() {}

  /**
   * Traditional main method
   *
   * @param args string[]
   */
  @SuppressWarnings("unchecked")
  public static void main(final String args[]) {
    try {

      if (args.length == 0) {
        System.err.println(
            "Please provide base classname of example to run. eg ToSql to run class io.substrait.examples.ToSql ");
        System.exit(-1);
      }
      final String exampleClass = args[0];

      final Class<Action> clz =
          (Class<Action>)
              Class.forName(
                  String.format("%s.%s", IsthmusAppExamples.class.getPackageName(), exampleClass));
      final Action action = clz.getDeclaredConstructor().newInstance();
      if (args.length == 1) {
        action.run(new String[] {});
      } else {
        action.run(Arrays.copyOfRange(args, 1, args.length));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
