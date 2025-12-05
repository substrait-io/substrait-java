# Isthmus API Examples

The Isthmus library converts Substrait plans to and from SQL Plans. There are two examples showing conversion in each direction.

## How does this work in theory?

The [Calcite](https://calcite.apache.org/) library is used to do parsing and generation of the SQL String. Calcite has it's own relational object model, distinct from substrait's. There are classes within Isthmus to convert Substrait to and from Calcite's object model.

The conversion flows work as follows:

**SQL to Substrait:**
`SQL ---[Calcite parsing]---> Calcite Object Model ---[Isthmus conversion]---> Substrait`

**Substrait to SQL:**
`Substrait ---[Isthmus conversion]---> Calcite Object Model ---[Calcite SQL generation]---> SQL`

## Running the examples

There are 2 example classes:

- [FromSql](./src/main/java/io/substrait/examples/FromSql.java) that creates a plan starting from SQL
- [ToSql](./app/src/main/java/io/substrait/examples/ToSQL.java) that reads a plan and creates the SQL


### Requirements

To run these you will need:

- Java 17 or greater
- [Two datafiles](./app/src/main/resources/) are provided for the sample data


## Creating a Substrait Plan from SQL

To run [`FromSql.java`](./src/main/java/io/substrait/examples/FromSql.java), execute the command below from the root of this repository. The example writes a binary plan to `substrait.plan` and outputs the text format of the protobuf to stdout. The output is quite lengthy, so it has been abbreviated here.

```bash
 ./gradlew examples:isthmus-api:run --args "FromSql substrait.plan"
> Task :examples:isthmus-api:run
extension_uris {
  extension_uri_anchor: 2
  uri: "/functions_aggregate_generic.yaml"
}
extension_uris {
  extension_uri_anchor: 1
  uri: "/functions_comparison.yaml"
}
extensions {
  extension_function {
    extension_uri_reference: 1
    function_anchor: 1
    name: "equal:any_any"
    extension_urn_reference: 1
  }
}
extensions {
  extension_function {
    extension_uri_reference: 2
    function_anchor: 2
    name: "count:"
    extension_urn_reference: 2
  }
}
relations {....}
}
version {
  minor_number: 77
  producer: "isthmus"
}
extension_urns {
  extension_urn_anchor: 1
  urn: "extension:io.substrait:functions_comparison"
}
extension_urns {
  extension_urn_anchor: 2
  urn: "extension:io.substrait:functions_aggregate_generic"
}

File written to substrait.plan
```


Please see the code comments for details of how the conversion is done.

## Creating SQL from a Substrait Plan

To run [`ToSql.java`](./src/main/java/io/substrait/examples/ToSql.java), execute the command below from the root of this repository. The example reads from `substrait.plan` (likely the file created by `FromSql`) and outputs SQL. The text format of the protobuf has been abbreviated.
```bash
./gradlew examples:isthmus-api:run --args "ToSql substrait.plan"

> Task :examples:isthmus-api:run
Reading from substrait.plan
extension_uris {
  extension_uri_anchor: 2
  uri: "/functions_aggregate_generic.yaml"
}
extension_uris {
  extension_uri_anchor: 1
  uri: "/functions_comparison.yaml"
}
extensions {
  extension_function {
    extension_uri_reference: 1
    function_anchor: 1
    name: "equal:any_any"
    extension_urn_reference: 1
  }
}
extensions {....}
relations {....}
version {
  minor_number: 77
  producer: "isthmus"
}
extension_urns {
  extension_urn_anchor: 1
  urn: "extension:io.substrait:functions_comparison"
}
extension_urns {
  extension_urn_anchor: 2
  urn: "extension:io.substrait:functions_aggregate_generic"
}


SELECT `t2`.`colour0` AS `COLOUR`, `t2`.`$f1` AS `COLOURCOUNT`
FROM (SELECT `vehicles`.`colour` AS `colour0`, COUNT(*) AS `$f1`
FROM `vehicles`
INNER JOIN `tests` ON `vehicles`.`vehicle_id` = `tests`.`vehicle_id`
WHERE `tests`.`test_result` = 'P'
GROUP BY `vehicles`.`colour`
ORDER BY COUNT(*) IS NULL, 2) AS `t2`

```

The SQL statement in the selected dialect will be created (MySql is used in the example).
