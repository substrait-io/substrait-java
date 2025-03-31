package io.substrait.isthmus.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.Resources;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.plan.Plan.Root;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

/** API level testing */
public class TestIsthmusEndToEnd {

  /** Conversion of a Substrait Protobuf to */
  @Test
  public void substraitToSqlViaCalcite() throws IOException {

    // create the protobuf Substrait plan
    byte[] planProtobuf = Resources.toByteArray(Resources.getResource("substrait_sql_003.plan"));
    io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.parseFrom(planProtobuf);
    // convert this to the Substrait Plan POJO
    ProtoPlanConverter converter = new io.substrait.plan.ProtoPlanConverter();
    io.substrait.plan.Plan pojoPlan = converter.from(protoPlan);

    System.out.println("POJO Substrait Plan::\n" + pojoPlan);

    // To convert from Substrait first need to convert to Calcite, and then Calcite
    // to SQL
    SimpleExtension.ExtensionCollection extensions = SimpleExtension.loadDefaults();
    SubstraitToCalcite substrait2Calcite =
        new SubstraitToCalcite(
            extensions, new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM));

    List<Root> planRoots = pojoPlan.getRoots();
    assertEquals(planRoots.size(), 1);

    var calciteRel = substrait2Calcite.convert(planRoots.get(0)).project();
    String sql = SubstraitToSql.toSql(calciteRel).replace("\n", " ");

    assertEquals(
        sql,
        Resources.toString(
            Resources.getResource("substrait_sql_003.sql"), Charset.defaultCharset()));
    System.out.println(sql);
  }

  /** SQL to Substrait using a set of `CREATE TABLE...` statements to define the schema */
  @Test
  public void sqlToSubstraitCreateTables() throws SqlParseException {

    String sqlQuery =
        "SELECT * FROM vehicles INNER JOIN tests ON vehicles.vehicle_id=tests.vehicle_id WHERE tests.test_result = 'F' and tests.test_mileage < 70000";

    String createTests =
        "CREATE TABLE \"tests\" (\"test_id\" varchar(15), \"vehicle_id\" varchar(15), \"test_date\" varchar(20), \"test_class\" varchar(20), \"test_type\" varchar(20), \"test_result\" varchar(15),\"test_mileage\" int, \"postcode_area\" varchar(15)) ";

    String createVehicles =
        "CREATE TABLE \"vehicles\" (\"vehicle_id\" varchar(15), \"make\" varchar(40), \"model\" varchar(40), \"colour\" varchar(15), \"fuel_type\" varchar(15), \"cylinder_capacity\" int, \"first_use_date\" varchar(15))";

    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    io.substrait.proto.Plan protoPlan =
        sqlToSubstrait.execute(sqlQuery, List.of(createTests, createVehicles));

    ProtoPlanConverter converter = new io.substrait.plan.ProtoPlanConverter();
    io.substrait.plan.Plan pojoPlan = converter.from(protoPlan);

    System.out.println("POJO Substrait Plan::\n" + pojoPlan);
  }

  @Test
  public void sqltoSubstraitCalciteSchema() throws SqlParseException {

    String sqlQuery =
        "SELECT * FROM vehicles INNER JOIN tests ON vehicles.vehicle_id=tests.vehicle_id WHERE tests.test_result = 'F' and tests.test_mileage < 70000";

    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    Schema schema = new ReflectiveSchema(new ExampleCalciteReflectiveSchema());

    io.substrait.proto.Plan protoPlan = sqlToSubstrait.execute(sqlQuery, "ExampleSchema", schema);

    ProtoPlanConverter converter = new io.substrait.plan.ProtoPlanConverter();
    io.substrait.plan.Plan pojoPlan = converter.from(protoPlan);

    System.out.println("POJO Substrait Plan::\n" + pojoPlan);
  }
}
