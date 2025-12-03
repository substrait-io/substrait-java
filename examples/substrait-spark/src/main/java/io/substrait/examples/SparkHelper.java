package io.substrait.examples;

import org.apache.spark.sql.SparkSession;

/** Collection of helper fns */
public final class SparkHelper {

  /** Vehicles table */
  public static final String VEHICLE_TABLE = "vehicles";

  /** Tests table (the vehicle safety tests) */
  public static final String TESTS_TABLE = "tests";

  /** Source data - csv */
  public static final String VEHICLES_CSV = "vehicles_subset_2023.csv";

  /** Source data - csv */
  public static final String TESTS_CSV = "tests_subset_2023.csv";

  /** In-container data location */
  public static final String ROOT_DIR = "/opt/spark-data";

  private SparkHelper() {}

  /**
   * Connects to the local spark cluister
   *
   * @return SparkSession
   */
  public static SparkSession connectLocalSpark() {

    final SparkSession spark = SparkSession.builder().enableHiveSupport().getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    return spark;
  }
}
