package io.substrait.examples;

import org.apache.spark.sql.SparkSession;

/** Collection of helper fns */
public final class SparkHelper {

  private SparkHelper() {}

  /** Namespace to use for the data */
  public static final String NAMESPACE = "demo_db";

  /** Vehicles table */
  public static final String VEHICLE_TABLE = "vehicles";

  /** Tests table (the vehicle safety tests) */
  public static final String TESTS_TABLE = "tests";

  /** Source data - parquet */
  public static final String VEHICLES_PQ = "vehicles_subset_2023.parquet";

  /** Source data - parquet */
  public static final String TESTS_PQ = "tests_subset_2023.parquet";

  /** Source data - csv */
  public static final String VEHICLES_CSV = "vehicles_subset_2023.csv";

  /** Source data - csv */
  public static final String TESTS_CSV = "tests_subset_2023.csv";

  /** In-container data location */
  public static final String ROOT_DIR = "/opt/spark-data";

  /**
   * Connect to local spark for demo purposes
   *
   * @param sparkMaster address of the Spark Master to connect to
   * @return SparkSession
   */
  public static SparkSession connectSpark(String sparkMaster) {

    SparkSession spark =
        SparkSession.builder()
            // .config("spark.sql.warehouse.dir", "spark-warehouse")
            .config("spark.master", sparkMaster)
            .enableHiveSupport()
            .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    return spark;
  }

  /**
   * Connects to the local spark cluister
   *
   * @return SparkSession
   */
  public static SparkSession connectLocalSpark() {

    SparkSession spark = SparkSession.builder().enableHiveSupport().getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    return spark;
  }
}
