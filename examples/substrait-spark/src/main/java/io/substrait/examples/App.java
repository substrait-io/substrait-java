package io.substrait.examples;

/** Main class */
public final class App {

  /** Implemented by all examples */
  public interface Action {

    /**
     * Run
     *
     * @param arg argument
     */
    void run(String arg);
  }

  private App() {}

  /**
   * Traditional main method
   *
   * @param args string[]
   */
  public static void main(String args[]) {
    try {

      if (args.length == 0) {
        args = new String[] {"SparkDataset"};
      }
      final String exampleClass = args[0];

      final Class<?> clz = Class.forName(App.class.getPackageName() + "." + exampleClass);
      final Action action = (Action) clz.getDeclaredConstructor().newInstance();

      if (args.length == 2) {
        action.run(args[1]);
      } else {
        action.run(null);
      }

    } catch (final Exception e) {
      e.printStackTrace();
    }
  }
}
