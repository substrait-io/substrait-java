# Introduction to the Substrait-Spark library

The Substrait-Spark library allows Substrait plans to convert to and from Spark Plans. This example will show how this can be used.

## How does this work in practice?

Once Spark SQL and Spark DataFrame APIs queries have been created, Spark's optimized query plan can be used to generate Substrait plans; and Substrait Plans can be executed on a Spark cluster. Below is a description of how to use this library; there are two sample datasets included for demonstration.

The most commonly used logical relations are supported, including those generated from all the TPC-H queries, but there are currently some gaps in support that prevent all the TPC-DS queries from being translatable.


## Running the examples

There are 3 example classes:

- [SparkDataset](./app/src/main/java/io/substrait/examples/SparkDataset.java) that creates a plan starting with the Spark Dataset API
- [SparkSQL](./app/src/main/java/io/substrait/examples/SparkSQL.java) that creates a plan starting with the Spark SQL API
- [SparkConsumeSubstrait](./app/src/main/java/io/substrait/examples/SparkConsumeSubstrait.java) that loads a Substrait plan and executes it



### Requirements

To run these you will need:

- Java 17 or greater
- Docker to start a test Spark Cluster
  - You could use your own cluster, but would need to adjust file locations defined in [SparkHelper](./app/src/main/java/io/substrait/examples/SparkHelper.java)
- The [just task runner](https://github.com/casey/just#installation) is optional, but very helpful to run the bash commands
- [Two datafiles](./app/src/main/resources/) are provided (CSV format)

For building using the `substrait-spark` library youself, using the [mvn repository](https://mvnrepository.com/artifact/io.substrait/spark)

Using Maven:
```xml
<!-- https://mvnrepository.com/artifact/io.substrait/spark -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>spark</artifactId>
    <version>0.36.0</version>
</dependency>
```

Using Gradle (Groovy)
```groovy
// https://mvnrepository.com/artifact/io.substrait/spark
implementation 'io.substrait:spark:0.36.0'
```

### Setup configuration

Firstly the application needs to be built; this is a simple Java application. As well issuing the `gradle` build command it also creates two directories `_apps` and `_data`. The JAR file and will be copied to the `_apps` directory and the datafiles to the `_data`. Note that the permissions on the `_data` directory are set to group write - this allows the Spark process in the docker container to write the output plan

To run using `just`
```
just buildapp

# or

./gradlew build
mkdir -p ./_data  && chmod g+w ./_data
mkdir -p ./_apps

cp ./app/build/libs/app.jar ./_apps
cp ./app/src/main/resources/*.csv ./_data

```

- In the `_data` directory there are now two csv files [tests_subset_2023.csv](./app/src/main/resources/tests_subset_2023.csv) and [vehicles_subset_2023.csv](./app/src/main/resources/vehicles_subset_2023.csv)


Second, you can start the basic Spark cluster - this uses `docker compose`.  It is best to start this is a separate window

```
just spark
```

- In [SparkHelper](./app/src/main/java/io/substrait/examples/SparkHelper.java) there are constants defined to match these locations

```java
  public static final String VEHICLES_PQ_CSV = "vehicles_subset_2023.csv";
  public static final String TESTS_PQ_CSV = "tests_subset_2023.csv";
  public static final String ROOT_DIR = "file:/opt/spark-data";
```

- To run the application `exec` into the SparkMaster node, and issue `spark-submit`

```
docker exec -it subtrait-spark-spark-1 bash
/opt/spark/bin/spark-submit --master spark://subtrait-spark-spark-1:7077  --driver-memory 1G --executor-memory 1G /opt/spark-apps/app.jar <args>
```

The `justfile` has three targets to make it easy to run the examples

- `just dataset` runs the Dataset API and produces `spark_dataset_substrait.plan`
- `just sql` runs the SQL api and produces `spark_sql_substrait.plan`
- `just consume <planfile>` runs the specified plan (from the `_data` directory)
- run `just` without arguments to get a summary of the targets available.
```
just
Available recipes:
    buildapp    # Builds the application into a JAR file
    consume arg # Consumes the Substrait plan file passed as the argument
    dataset     # Runs a Spark dataset api query and produces a Substrait plan
    spark       # Starts a simple Spark cluster locally in docker
    sql         # Runs a Spark SQL api query and produces a Substrait plan
```


## Creating a Substrait Plan

In [SparkSQL](./app/src/main/java/io/substrait/examples/SparkSQL.java) is a simple use of SQL to join the two tables; after reading the two CSV files, the SQL query is defined. This is then run on Spark.

### Loading data

Firstly the filenames are created, and the CSV files read. Temporary views need to be created to refer to these tables in the SQL query.

```java
      String vehiclesFile = Paths.get(ROOT_DIR, VEHICLES_CSV).toString();
      String testsFile = Paths.get(ROOT_DIR, TESTS_CSV).toString();

      spark.read().option("delimiter", ",").option("header", "true").csv(vehiclesFile)
          .createOrReplaceTempView(VEHICLE_TABLE);
      spark.read().option("delimiter", ",").option("header", "true").csv(testsFile)
        .createOrReplaceTempView(TESTS_TABLE);
```

### Creating the SQL query

The following standard SQL query string finds the counts of all cars (grouped by colour) of all vehicles that have passed the vehicle safety test.

```java
      String sqlQuery = """
           SELECT vehicles.colour, count(*) as colourcount
           FROM vehicles
           INNER JOIN tests ON vehicles.vehicle_id=tests.vehicle_id
           WHERE tests.test_result = 'P'
           GROUP BY vehicles.colour
           ORDER BY count(*)
          """;
      var result = spark.sql(sqlQuery);
      result.show();
```

If we were to just run this as-is, the output table would be below.
```
+------+-----------+
|colour|colourcount|
+------+-----------+
| GREEN|          1|
|BRONZE|          1|
|   RED|          2|
| BLACK|          2|
|  GREY|          2|
|  BLUE|          2|
|SILVER|          3|
| WHITE|          5|
+------+-----------+
```

### Logical and Optimized Query Plans

The next step is to look at the logical and optimised query plans that Spark has constructed.

```java
      LogicalPlan logical = result.logicalPlan();
      System.out.println(logical);

      LogicalPlan optimised = result.queryExecution().optimizedPlan();
      System.out.println(optimised);
```

The logical plan will be:

```
Sort [colourcount#30L ASC NULLS FIRST], true
+- Aggregate [colour#3], [colour#3, count(1) AS colourcount#30L]
   +- Filter (test_result#19 = P)
      +- Join Inner, (vehicle_id#0L = vehicle_id#15L)
         :- SubqueryAlias vehicles
         :  +- View (`vehicles`, [vehicle_id#0L,make#1,model#2,colour#3,fuel_type#4,cylinder_capacity#5L,first_use_date#6])
         :     +- Relation [vehicle_id#0L,make#1,model#2,colour#3,fuel_type#4,cylinder_capacity#5L,first_use_date#6] csv
         +- SubqueryAlias tests
            +- View (`tests`, [test_id#14L,vehicle_id#15L,test_date#16,test_class#17,test_type#18,test_result#19,test_mileage#20L,postcode_area#21])
               +- Relation [test_id#14L,vehicle_id#15L,test_date#16,test_class#17,test_type#18,test_result#19,test_mileage#20L,postcode_area#21] csv
```

Similarly, the optimized plan can be found; here the `SubQuery` and `View` have been converted into Project and Filter

```
Sort [colourcount#30L ASC NULLS FIRST], true
+- Aggregate [colour#3], [colour#3, count(1) AS colourcount#30L]
   +- Project [colour#3]
      +- Join Inner, (vehicle_id#0L = vehicle_id#15L)
         :- Project [vehicle_id#0L, colour#3]
         :  +- Filter isnotnull(vehicle_id#0L)
         :     +- Relation [vehicle_id#0L,make#1,model#2,colour#3,fuel_type#4,cylinder_capacity#5L,first_use_date#6] csv
         +- Project [vehicle_id#15L]
            +- Filter ((isnotnull(test_result#19) AND (test_result#19 = P)) AND isnotnull(vehicle_id#15L))
               +- Relation [test_id#14L,vehicle_id#15L,test_date#16,test_class#17,test_type#18,test_result#19,test_mileage#20L,postcode_area#21] csv
```

### Dataset API

Alternatively, the Dataset API can be used to create the plans, the code for this in [`SparkDataset`](./app/src/main/java/io/substrait/examples/SparkDataset.java).  The overall flow of the code is very similar

Rather than create a temporary view, the reference to the datasets are kept in `dsVehicles` and `dsTests`
```java
      dsVehicles = spark.read().option("delimiter", ",").option("header", "true").csv(vehiclesFile);
      dsVehicles.show();

      dsTests = spark.read().option("delimiter", ",").option("header", "true").csv(testsFile);
      dsTests.show();
```

They query can be constructed based on these two datasets

```java
      Dataset<Row> joinedDs = dsVehicles.join(dsTests, dsVehicles.col("vehicle_id").equalTo(dsTests.col("vehicle_id")))
          .filter(dsTests.col("test_result").equalTo("P"))
          .groupBy(dsVehicles.col("colour"))
          .count();

      joinedDs = joinedDs.orderBy(joinedDs.col("count"));
      joinedDs.show();
```

Using the same APIs, the Spark's optimized plan is available. If you compare this to the plan above you will see that structurally it is identical.

```
Sort [count#189L ASC NULLS FIRST], true
+- Aggregate [colour#20], [colour#20, count(1) AS count#189L]
   +- Project [colour#20]
      +- Join Inner, (vehicle_id#17 = vehicle_id#86)
         :- Project [vehicle_id#17, colour#20]
         :  +- Filter isnotnull(vehicle_id#17)
         :     +- Relation [vehicle_id#17,make#18,model#19,colour#20,fuel_type#21,cylinder_capacity#22,first_use_date#23] csv
         +- Project [vehicle_id#86]
            +- Filter ((isnotnull(test_result#90) AND (test_result#90 = P)) AND isnotnull(vehicle_id#86))
               +- Relation [test_id#85,vehicle_id#86,test_date#87,test_class#88,test_type#89,test_result#90,test_mileage#91,postcode_area#92] csv
```

### Substrait Creation

This optimized plan is the best starting point to produce a Substrait Plan; there's a `createSubstrait(..)` function that does the work and produces a binary protobuf Substrait file.

```
    LogicalPlan optimised = result.queryExecution().optimizedPlan();
    System.out.println(optimised);

    createSubstrait(optimised);
```

Let's look at the APIs in the `createSubstrait(...)` method to see how it's using the `Substrait-Spark` Library.

```java
    ToSubstraitRel toSubstrait = new ToSubstraitRel();
    io.substrait.plan.Plan plan = toSubstrait.convert(enginePlan);
```

`ToSubstraitRel` is the main class and provides the convert method; this takes the Spark plan (optimized plan is best) and produces the Substrait Plan.  The most common relations are supported currently - and the optimized plan is more likely to use these.

The `io.substrait.plan.Plan` object is a high-level Substrait POJO representing a plan. This could be used directly or more likely be persisted. protobuf is the canonical serialization form.  It's easy to convert this and store in a file

```java
    PlanProtoConverter planToProto = new PlanProtoConverter();
    byte[] buffer = planToProto.toProto(plan).toByteArray();
    try {
      Files.write(Paths.get(ROOT_DIR, "spark_sql_substrait.plan"),buffer);
    } catch (IOException e){
      e.printStackTrace();
    }
```

For the dataset approach, the `spark_dataset_substrait.plan` is created, and for the SQL approach the `spark_sql_substrait.plan` is created.  These Intermediate Representations of the query can be saved, transferred and reloaded into a Data Engine.

We can also review the Substrait plan's structure; the canonical format of the Substrait plan is the binary protobuf format, but it's possible to produce a textual version, an example is below (please see the [SubstraitStringify utility class](./src/main/java/io/substrait/examples/util/SubstraitStringify.java); it's also a good example of how to use some of the visitor patterns).  Both the Substrait plans from the Dataset or SQL APIs generate the same output.

```
<Substrait Plan>
Root ::  ImmutableSort [colour, count]
<Substrait Relation>
+- Sort:: FieldRef#/I64/StructField{offset=1}  ASC_NULLS_FIRST
   +- Project:: [Str, I64, Str, I64]
      +- Aggregate:: FieldRef#/Str/StructField{offset=0}
         +- Project:: [Str, Str, Str, Str]
            +- Join:: INNER <ScalarFn>equal:any_any
                : arg0 = FieldRef#/Str/StructField{offset=0}
                : arg1 = FieldRef#/Str/StructField{offset=2}
               +- Project:: [Str, Str, Str, Str, Str, Str, Str, Str, Str]
                  +- Filter:: <ScalarFn>is_not_null:any
                      : arg0 = FieldRef#/Str/StructField{offset=0}
                     +- LocalFiles::
                         :  file:///opt/spark-data/vehicles_subset_2023.csv len=1547 partition=0 start=0
               +- Project:: [Str, Str, Str, Str, Str, Str, Str, Str, Str]
                  +- Filter:: <ScalarFn>and:bool
                      : arg0 = <ScalarFn>and:bool
                         : arg0 = <ScalarFn>is_not_null:any
                            : arg0 = FieldRef#/Str/StructField{offset=5}
                         : arg1 = <ScalarFn>equal:any_any
                            : arg0 = FieldRef#/Str/StructField{offset=5}
                            : arg1 = <StrLiteral P>
                      : arg1 = <ScalarFn>is_not_null:any
                         : arg0 = FieldRef#/Str/StructField{offset=1}
                     +- LocalFiles::
                         :  file:///opt/spark-data/tests_subset_2023.csv len=1491 partition=0 start=0
```

There is more detail in this version than the Spark version; details of the functions called for example are included. However, the structure of the overall plan is identical with 1 exception. There is an additional `project` relation included between the `sort` and `aggregate` - this is necessary to get the correct types of the output data.

We can also see in this case as the plan came from Spark directly it's also included the location of the datafiles. Below when we reload this into Spark, the locations of the files don't need to be explicitly included.


As the `Substrait Spark` library also allows plans to be loaded and executed, so the next step is to consume these Substrait plans.

## Consuming a Substrait Plan

The [`SparkConsumeSubstrait`](./app/src/main/java/io/substrait/examples/SparkConsumeSubstrait.java) code shows how to load this file, and most importantly how to convert it to a Spark engine plan to execute.

Loading the binary protobuf file is the reverse of the writing process (in the code the file name comes from a command line argument, here we're showing the hardcoded file name )

```java
      byte[] buffer = Files.readAllBytes(Paths.get("spark_sql_substrait.plan"));
      io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);

      ProtoPlanConverter protoToPlan = new ProtoPlanConverter();
      Plan plan = protoToPlan.from(proto);
```

The loaded byte array is first converted into the protobuf Plan, and then into the Substrait Plan object. Note it can be useful to name the variables, and/or use the full class names to keep track whether it's the ProtoBuf Plan or the high-level POJO Plan. For example `io.substrait.proto.Plan` or `io.substrait.Plan`


Finally this can be converted to a Spark Plan:

```java
      ToLogicalPlan substraitConverter = new ToLogicalPlan(spark);
      LogicalPlan sparkPlan = substraitConverter.convert(plan);
```

If you were to print out this plan, it has the identical structure to the plan seen earlier on.

```
+- Sort [count(1)#18L ASC NULLS FIRST], true
   +- Aggregate [colour#5], [colour#5, count(1) AS count(1)#18L]
      +- Project [colour#5]
         +- Join Inner, (vehicle_id#2 = vehicle_id#10)
            :- Project [vehicle_id#2, colour#5]
            :  +- Filter isnotnull(vehicle_id#2)
            :     +- Relation [vehicle_id#2,make#3,model#4,colour#5,fuel_type#6,cylinder_capacity#7,first_use_date#8] csv
            +- Project [vehicle_id#10]
               +- Filter ((isnotnull(test_result#14) AND (test_result#14 = P)) AND isnotnull(vehicle_id#10))
                  +- Relation [test_id#9,vehicle_id#10,test_date#11,test_class#12,test_type#13,test_result#14,test_mileage#15,postcode_area#16] csv
```

Execution of this plan is then a simple `Dataset.ofRows(spark, sparkPlan).show();` giving the output of

```java
+------+-----+
|colour|count|
+------+-----+
| GREEN|    1|
|BRONZE|    1|
|   RED|    2|
| BLACK|    2|
|  GREY|    2|
|  BLUE|    2|
|SILVER|    3|
| WHITE|    5|
+------+-----+
```

### Observations

To recap on the steps above

- Two CSV files have been loaded into Spark
- Using either the Spark SQL or the Spark Dataset API we can produce a query across those two datasets
- Both queries result in Spark creating a logical and optimized query plan
  - And both being are structurally identical
- Using the Substrait-Java library, we can convert the optimized plan into the Substrait format.
- This Substrait intermediate representation of the query can be serialized via the protobuf format
  - Here store as a flat file containing the bytes of that protobuf
- *Separately* this file can be loaded and the Substrait Plan converted to a Spark Plan
- This can be run in an application on Spark getting the same results

---
## Plan Comparison

The structure of the query plans for both Spark and Substrait are structurally very similar.

### Aggregate and Sort

Spark's plan has a Project that filters down to the colour, followed by the Aggregation and Sort.
```
+- Sort [count(1)#18L ASC NULLS FIRST], true
   +- Aggregate [colour#5], [colour#5, count(1) AS count(1)#18L]
      +- Project [colour#5]
```

When converted to Substrait the Sort and Aggregate is in the same order, but there are additional projects; it's not reduced the number of fields as early.

```
+- Sort:: FieldRef#/I64/StructField{offset=1}  ASC_NULLS_FIRST
   +- Project:: [Str, I64, Str, I64]
      +- Aggregate:: FieldRef#/Str/StructField{offset=0}
```

These look different due to two factors. Firstly the Spark optimizer has swapped the project and aggregate functions.
Secondly projects within the Substrait plan joined the fields together but don't reduce the number of fields. Any such filtering is done on the outer relations.

### Inner Join

Spark's inner join is taking as inputs the two filtered relations; it's ensuring the join key is not null but also the `test_result==p` check.

```
         +- Join Inner, (vehicle_id#2 = vehicle_id#10)
            :- Project [vehicle_id#2, colour#5]
            :  +- Filter isnotnull(vehicle_id#2)

            +- Project [vehicle_id#10]
               +- Filter ((isnotnull(test_result#14) AND (test_result#14 = P)) AND isnotnull(vehicle_id#10))
```

The Substrait Representation looks longer, but is showing the same structure. (note that this format is a custom format implemented as [SubstraitStingify](./src/main/java/io/substrait/examples/util/SubstraitStringify.java) as the standard text output can be hard to read).

```
            +- Join:: INNER <ScalarFn>equal:any_any
                : arg0 = FieldRef#/Str/StructField{offset=0}
                : arg1 = FieldRef#/Str/StructField{offset=2}
               +- Project:: [Str, Str, Str, Str, Str, Str, Str, Str, Str]
                  +- Filter:: <ScalarFn>is_not_null:any
                      : arg0 = FieldRef#/Str/StructField{offset=0}

               +- Project:: [Str, Str, Str, Str, Str, Str, Str, Str, Str]
                  +- Filter:: <ScalarFn>and:bool
                      : arg0 = <ScalarFn>and:bool
                         : arg0 = <ScalarFn>is_not_null:any
                            : arg0 = FieldRef#/Str/StructField{offset=5}
                         : arg1 = <ScalarFn>equal:any_any
                            : arg0 = FieldRef#/Str/StructField{offset=5}
                            : arg1 = <StrLiteral P>
                      : arg1 = <ScalarFn>is_not_null:any
                         : arg0 = FieldRef#/Str/StructField{offset=1}
```

### LocalFiles

The source of the data originally was two csv files; in the Spark plan this is referred to by csv suffix: ` Relation [...] csv`; this is represented in the Substrait plan as
```
   +- LocalFiles::
         :  file:///opt/spark-data/tests_subset_2023.csv len=1491 partition=0 start=0
```

There is a dedicated Substrait `ReadRel` relation for referencing files, it does include additional information about the type of the file, size, format and options for reading those specific formats.  Parquet/Arrow/Orc/ProtoBuf/Dwrf currently all have specific option structures.

## Data Locations

The implication of a relation that includes a filename is seen when the plan is deserialized and executed; the binary Substrait plan needs to be read, converted into a Substrait Plan POJO and passed to the Spark-Substrait library to be converted. Once converted it can be directly executed.

The plan itself contains all the information needed to be able to execute the query.

A slight difference is observed when the Spark DataFrame is saved as a Hive table. Using `saveAsTable(...)` and `table(...)` the data can be persisted.

```java
      String vehiclesFile = Paths.get(ROOT_DIR, VEHICLES_CSV).toString();
      Dataset<Row> dsVehicles = spark.read().option("delimiter", ",").option("header", "true").csv(vehiclesFile);
      dsVehicles.write().saveAsTable("vehicles");

      spark.read().table("vehicles").show();
```

When this is table is read and used in queries the Substrait "ReadRel" will be a `NamedScan` instead; this is referring to a table
`[spark_catalog, default, vehicles]` - default is the name of the default Spark database.

```
   +- NamedScan::  Tables=[spark_catalog, default, vehicles] Fields=vehicle_id[Str],make[Str],model[Str],colour[Str],fuel_type[Str],cylinder_capacity[Str],first_use_date[Str]
```

This plan can be consumed in exactly the same many as the other plans; the only difference being, _if the table is not aleady_ present it will fail to execute. There isn't the source of the data, rather a reference name, and the expected fields. Ensuring the data is present in Spark, the query will execute without issue.

## Observations on LoadFiles/NamedScan

Including the information on the location of the data permits easy use of the plan. In the example here this worked well; however there could be difficulties depending on the recipient engine. Substrait as an intermediate form gives the ability to transfer the plans between engines; how different engines catalogue their data will be relevant.

For example the above plan can be handled with PyArrow or DuckDB (as an example there are a variety of other engines); the code for consuming the plans is straightforward.

```python
   with open(PLAN_FILE, "wb") as file:
      planbytes = file.read()
      reader = substrait.run_query(
         base64.b64decode(planbytes),
            table_provider=self.simple_provider,
        )
      result = reader.read_all()
```

When run with the plan pyarrow instantly rejects it with

```
pyarrow.lib.ArrowNotImplementedError: non-default substrait::ReadRel::LocalFiles::FileOrFiles::length
```

DuckDB has a simiar API `connection.from_substrait(planbyhtes)` and produces a different error

```
duckdb.duckdb.IOException: IO Error: No files found that match the pattern "file:///opt/spark-data/tests_subset_2023.csv"
```

This shows that different engines will potentially have different supported relations; PyArrow wants to delegate the loading of the data to the user, whereas DuckDB is happy to load files. DuckDB though of course can only proceed with the information that it has, the URI of the file here is coupled to the location of the data on the originating engine.  Something like a s3 uri could be potentially useful.

Creating a plan from Spark but where the data is saved as table provides an alternative. Depending on the engine this can also need some careful handling. In the `NamedScan` above, the name was a list of 3 strings. `Tables=[spark_catalog, default, vehicles]`.  Whilst DuckDB's implementation understands that these are referring to a table, its own catalogue can't be indexed with these three values.

```
duckdb.duckdb.CatalogException: Catalog Error: Table with name spark_catalog does not exist!
```

PyArrow takes a different approach in locating the data. In the PyArrow code above there is a reference to a `table_provider`; the job of 'providing a table' is delegated back to the user.

Firstly we need to load the datasets to PyArrow datasets
```python
   test = pq.read_table(TESTS_PQ_FILE)
   vehicles = pq.read_table(VEHICLES_PQ_FILE)
```

We can define a `table_provider` function; this logs which table is being requested, but also what the expected schema is.
As names is a array, we can check the final part of the name and return the matching dataset.

```python
    def table_provider(self, names, schema):
        print(f"== Requesting table {names} with schema: \n{schema}\n")

        if not names:
            raise Exception("No names provided")
        else:
            if names[-1] == "tests":
                return self.test
            elif names[-1] == "vehicles":
                return self.vehicles

            raise Exception(f"Unrecognized table name {names}")
```


When run the output is along these lines (the query is slightly different here for simplicity); we can see the tables being requested and the schema expected. Nothing is done with the schema here but could be useful for ensuring that the expectations of the plan match the schema of the data held in the engine.

```
== Requesting table ['spark_catalog', 'default', 'vehicles'] with schema:
vehicle_id: string
make: string
model: string
colour: string
fuel_type: string
cylinder_capacity: string
first_use_date: string

== Requesting table ['spark_catalog', 'default', 'tests'] with schema:
test_id: string
vehicle_id: string
test_date: string
test_class: string
test_type: string
test_result: string
test_mileage: string
postcode_area: string

   colour test_result
0   WHITE           P
1   WHITE           F
2   BLACK           P
3   BLACK           P
4     RED           P
5   BLACK           P
6    BLUE           P
7  SILVER           F
8  SILVER           F
9   BLACK           P
```

# Summary

The Substrait intermediate representation of the query can be serialized via the protobuf format and transferred between engines of the same type or between different engines.

In the case of Spark for example, identical plans can be created with the Spark SQL or the Spark Dataset API.
*Separately* this file can be loaded and the Substrait Plan converted to a Spark Plan. Assuming that the consuming engine has the same understanding of the reference to LocalFiles the plan can be read and executed.

Logical references to a 'table' via a `NamedScan` gives more flexibility; but the structure of the reference still needs to be properly understood and agreed upon.

Once common understanding is agreed upon, transferring plans between engines brings great flexibility and potential.
