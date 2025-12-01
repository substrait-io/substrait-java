# Isthmus API Examples

The Isthmus library converts Substrait plans to and from SQL Plans. There are two examples showing convertion in each direction.

## How does this work in theory?

In both cases, the Calcite library is used to do parsing and generation of the SQL String. Calcite has it's own relational object model, so there are classes within Ishtmus to convert Substrait to and from Calcites object model.

Converting to Substrait from SQL uses Calcite to parse the SQL to an object model, and then it will be converted to Substrait.

Converting from Substrait to SQL involves converting Substrait to Calcite's object model, then asking Calcite to generate SQL strings.

## Running the examples

There are 2 example classes:

- [FromSql](./src/main/java/io/substrait/examples/FromSql.java) that creates a plan starting from SQL
- [ToSql](./app/src/main/java/io/substrait/examples/ToSQL.java) that reads a plan and creates the SQL


### Requirements

To run these you will need:

- Java 17 or greater
- [Two datafiles](./app/src/main/resources/) are provided for the sample data


## Creating a Substrait Plan from SQL

To run [`FromSql.java`](./src/main/java/io/substrait/examples/FromSql.java) from the root of this repository. `subtrait.plan` is the name of file written.

```bash
 ./gradlew examples:isthmus-api:run --args "FromSql substrait.plan"
> Task :examples:isthmus-api:run
Plan{version=Version{major=0, minor=77, patch=0, producer=isthmus}, roots=[Root{input=Sort{input=Aggregate{input=Project{remap=Remap{indices=[15]}, input=Filter{input=Join{left=NamedScan{initialSchema=NamedStruct{struct=Struct{nullable=false, fields=[VarChar{nullable=true, length=15}, VarChar{nullable=true, length=40}, VarChar{nullable=true, length=40}, VarChar{nullable=true, length=15}, VarChar{nullable=true, length=15}, I32{nullable=true}, VarChar{nullable=true, length=15}]}, names=[vehicle_id, make, model, colour, fuel_type, cylinder_capacity, first_use_date]}, names=[vehicles]}, right=NamedScan{initialSchema=NamedStruct{struct=Struct{nullable=false, fields=[VarChar{nullable=true, length=15}, VarChar{nullable=true, length=15}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=15}, I32{nullable=true}, VarChar{nullable=true, length=15}]}, names=[test_id, vehicle_id, test_date, test_class, test_type, test_result, test_mileage, postcode_area]}, names=[tests]}, condition=ScalarFunctionInvocation{declaration=equal:any_any, arguments=[FieldReference{segments=[StructField{offset=0}], type=VarChar{nullable=true, length=15}}, FieldReference{segments=[StructField{offset=8}], type=VarChar{nullable=true, length=15}}], options=[], outputType=Bool{nullable=true}}, joinType=INNER}, condition=ScalarFunctionInvocation{declaration=equal:any_any, arguments=[FieldReference{segments=[StructField{offset=12}], type=VarChar{nullable=true, length=15}}, VarCharLiteral{nullable=false, value=P, length=15}], options=[], outputType=Bool{nullable=true}}}, expressions=[FieldReference{segments=[StructField{offset=3}], type=VarChar{nullable=true, length=15}}]}, groupings=[Grouping{expressions=[FieldReference{segments=[StructField{offset=0}], type=VarChar{nullable=true, length=15}}]}], measures=[Measure{function=AggregateFunctionInvocation{declaration=count:, arguments=[], options=[], aggregationPhase=INITIAL_TO_RESULT, sort=[], outputType=I64{nullable=false}, invocation=ALL}}]}, sortFields=[SortField{expr=FieldReference{segments=[StructField{offset=1}], type=Struct{nullable=false, fields=[VarChar{nullable=true, length=15}, I64{nullable=false}]}}, direction=ASC_NULLS_LAST}]}, names=[COLOUR, COLOURCOUNT]}], expectedTypeUrls=[]}
File written to substrait.plan
```

It is a binary file, so to check the file written out
```bash
ls -l examples/isthmus-api/substrait.plan
-rw-r--r-- 1 matthew matthew 808 Dec  1 12:05 examples/isthmus-api/substrait.plan
```

Please see the code comments for details of how the conversion is done.

## Creating SQL from a Substrait Plan

To run [`ToSql.java`](./src/main/java/io/substrait/examples/ToSql.java) from the root of this repository
`subtrait.plan` is the name of file to be read - and probably will be the first created with `FromSql`.

```bash
./gradlew examples:isthmus-api:run --args "ToSql substrait.plan"

> Task :examples:isthmus-api:run
Reading from substrait.plan
Plan{version=Version{major=0, minor=77, patch=0, producer=isthmus}, roots=[Root{input=Sort{input=Aggregate{input=Project{remap=Remap{indices=[15]}, input=Filter{input=Join{left=NamedScan{initialSchema=NamedStruct{struct=Struct{nullable=false, fields=[VarChar{nullable=true, length=15}, VarChar{nullable=true, length=40}, VarChar{nullable=true, length=40}, VarChar{nullable=true, length=15}, VarChar{nullable=true, length=15}, I32{nullable=true}, VarChar{nullable=true, length=15}]}, names=[vehicle_id, make, model, colour, fuel_type, cylinder_capacity, first_use_date]}, names=[vehicles]}, right=NamedScan{initialSchema=NamedStruct{struct=Struct{nullable=false, fields=[VarChar{nullable=true, length=15}, VarChar{nullable=true, length=15}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=20}, VarChar{nullable=true, length=15}, I32{nullable=true}, VarChar{nullable=true, length=15}]}, names=[test_id, vehicle_id, test_date, test_class, test_type, test_result, test_mileage, postcode_area]}, names=[tests]}, condition=ScalarFunctionInvocation{declaration=equal:any_any, arguments=[FieldReference{segments=[StructField{offset=0}], type=VarChar{nullable=true, length=15}}, FieldReference{segments=[StructField{offset=8}], type=VarChar{nullable=true, length=15}}], options=[], outputType=Bool{nullable=true}}, joinType=INNER}, condition=ScalarFunctionInvocation{declaration=equal:any_any, arguments=[FieldReference{segments=[StructField{offset=12}], type=VarChar{nullable=true, length=15}}, VarCharLiteral{nullable=false, value=P, length=15}], options=[], outputType=Bool{nullable=true}}}, expressions=[FieldReference{segments=[StructField{offset=3}], type=VarChar{nullable=true, length=15}}]}, groupings=[Grouping{expressions=[FieldReference{segments=[StructField{offset=0}], type=VarChar{nullable=true, length=15}}]}], measures=[Measure{function=AggregateFunctionInvocation{declaration=count:, arguments=[], options=[], aggregationPhase=INITIAL_TO_RESULT, sort=[], outputType=I64{nullable=false}, invocation=ALL}}]}, sortFields=[SortField{expr=FieldReference{segments=[StructField{offset=1}], type=I64{nullable=false}}, direction=ASC_NULLS_LAST}]}, names=[COLOUR, COLOURCOUNT]}], expectedTypeUrls=[]}

SELECT `t2`.`colour0` AS `COLOUR`, `t2`.`$f1` AS `COLOURCOUNT`
FROM (SELECT `vehicles`.`colour` AS `colour0`, COUNT(*) AS `$f1`
FROM `vehicles`
INNER JOIN `tests` ON `vehicles`.`vehicle_id` = `tests`.`vehicle_id`
WHERE `tests`.`test_result` = 'P'
GROUP BY `vehicles`.`colour`
ORDER BY COUNT(*) IS NULL, 2) AS `t2`

```

The SQL statement in the selected dialect will be created (MySql is used in the example).
