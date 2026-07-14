# End-to-end example

The repository ships a runnable example, [`examples/substrait-spark`](https://github.com/substrait-io/substrait-java/tree/main/examples/substrait-spark),
that walks the full loop: build a small Spark application, start a Spark cluster in Docker, produce
a Substrait `.plan` protobuf file from a query, then load that file back and execute it. This page
follows that example; see its `README.md` for the complete narrative and sample output.

## What the example contains

The application lives under `examples/substrait-spark/src/main/java/io/substrait/examples/` and has
three entry points (dispatched by `App` on the first command-line argument):

| Class | Role |
| --- | --- |
| `SparkSQL` | Builds a plan from the **SQL** API, writes `spark_sql_substrait.plan` |
| `SparkDataset` | Builds a plan from the **DataFrame/Dataset** API, writes `spark_dataset_substrait.plan` |
| `SparkConsumeSubstrait` | **Loads** a `.plan` file and executes it on Spark |

`SparkHelper` holds the shared constants — notably `ROOT_DIR = /opt/spark-data` (the in-container
data directory) and the `vehicles`/`tests` table and CSV names. Two CSV datasets are provided under
`src/main/resources/`. `SparkSQL` and `SparkDataset` both express the same query (count vehicles by
colour that passed a safety test), and both convert `queryExecution().optimizedPlan()` — the
[optimized plan](producing-plans.md) — so they emit structurally identical Substrait.

The example uses a [`just`](https://github.com/casey/just) task runner (`justfile`) to wrap the
Docker and `spark-submit` commands.

## 1. Build the application

`just buildapp` compiles the app and stages the artifacts the Docker cluster expects: it runs the
Gradle build, creates `_apps/` and `_data/`, copies the built jar to `_apps/app.jar`, and copies
the CSV datasets into `_data/`.

```bash
cd examples/substrait-spark
just buildapp
```

!!! note "Why `_data` is group-writable"
    `buildapp` runs `chmod g+w _data` so that the Spark process inside the container (running as a
    different user) can **write the output `.plan` file** back into the mounted directory. `_data`
    is mounted into the containers at `/opt/spark-data` (which is `ROOT_DIR`), and `_apps` at
    `/opt/spark-apps`.

## 2. Start the Spark cluster

`just spark` brings up a small Spark cluster (a master and one worker, using the `bitnami/spark`
image) via `docker compose`. Run it in its own terminal and leave it running.

```bash
just spark
```

Under the hood this is `docker compose up`, with your uid/gid exported so the container can write to
the mounted `_data` volume.

## 3. Produce a `.plan` file

With the cluster up, run either query. Each does `spark-submit` of `app.jar` inside the master
container and writes its `.plan` file into `_data/`.

=== "SQL"

    ```bash
    just sql        # writes _data/spark_sql_substrait.plan
    ```

=== "DataFrame / Dataset"

    ```bash
    just dataset    # writes _data/spark_dataset_substrait.plan
    ```

Internally the example converts and serializes exactly as described in
[Producing plans](producing-plans.md):

```java
ToSubstraitRel toSubstrait = new ToSubstraitRel();
io.substrait.plan.Plan plan = toSubstrait.convert(optimised);

byte[] buffer = new PlanProtoConverter().toProto(plan).toByteArray();
Files.write(Paths.get(ROOT_DIR, "spark_sql_substrait.plan"), buffer);
```

Both entry points also print a human-readable rendering of the plan using the example's
`SubstraitStringify` utility — a good illustration of walking the Substrait POJO model with the
visitor pattern.

## 4. Load and execute the plan

`just consume <planfile>` runs `SparkConsumeSubstrait`, which reads the `.plan` file from `_data/`,
rebuilds the Spark logical plan, and executes it — producing the same result table as the original
query.

```bash
just consume spark_sql_substrait.plan
```

The consuming code follows [Consuming plans](consuming-plans.md):

```java
byte[] buffer = Files.readAllBytes(Paths.get(ROOT_DIR, arg));
io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);

Plan plan = new ProtoPlanConverter(SparkExtension.COLLECTION()).from(proto);

ToLogicalPlan toSpark = new ToLogicalPlan(spark);
LogicalPlan sparkPlan = toSpark.convert(plan);

Dataset.ofRows(spark, sparkPlan).show();
```

!!! warning "Spark 4.0 imports"
    In the example, `SparkConsumeSubstrait` and `SparkHelper` import `Dataset` and `SparkSession`
    from `org.apache.spark.sql.classic` — the Spark 4.0 location. On Spark 3.4/3.5 these come from
    `org.apache.spark.sql`. See the [Spark 4.0 package caveat](compatibility.md#spark-40-package-caveat).

Run `just` with no arguments to list all recipes (`buildapp`, `spark`, `sql`, `dataset`,
`consume`).

## Cross-engine consumption

Because the serialized plan is engine-neutral protobuf, engines other than Spark can consume it —
but what they need from a plan differs, and the example README explores this. The behavior depends
on which read relation Spark emitted:

- **DuckDB** (`connection.from_substrait(plan_bytes)`) is happy to load files itself, but the
  `LocalFiles` URIs are coupled to the machine that produced the plan — so it fails if the file
  paths do not exist locally. For a `NamedScan`, DuckDB cannot index its catalog with Spark's
  three-part name such as `spark_catalog.default.vehicles`.
- **PyArrow** (`substrait.run_query(plan_bytes, table_provider=...)`) delegates data loading back
  to the caller via a `table_provider` callback, which receives the requested table names and
  expected schema. It rejects Spark's non-default `LocalFiles` length field, so the `NamedScan`
  (table-reference) form is the better fit there.

```python
# PyArrow: the caller resolves table names to datasets
def table_provider(self, names, schema):
    if names[-1] == "vehicles":
        return self.vehicles
    elif names[-1] == "tests":
        return self.tests
    raise Exception(f"Unrecognized table name {names}")
```

The takeaway: transferring plans between engines is powerful, but the consuming engine must share an
understanding of how source data is referenced — file URIs versus catalog names. See
[Consuming plans](consuming-plans.md#where-the-data-comes-from) for how Spark itself handles each
read type.
