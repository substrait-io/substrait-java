package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/isthmus/substrait-to-sql.md}. Regions marked with {@code //
 * --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet
 * includes.
 */
class SubstraitToSqlDocTest extends PlanTestBase {

  private Plan samplePlan() {
    return sb.plan(
        sb.root(
            sb.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I64, R.STRING)),
            List.of("a", "b")));
  }

  @Test
  void convert() {
    Plan plan = samplePlan();
    // --8<-- [start:convert]
    List<String> sql = new SubstraitToSql().convert(plan, SubstraitSqlDialect.DEFAULT);
    String firstStatement = sql.get(0);
    // --8<-- [end:convert]
    assertNotNull(firstStatement);
  }

  @Test
  void roundTrip() {
    Plan plan = samplePlan();
    // --8<-- [start:round-trip]
    // POJO Plan -> protobuf
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);

    // protobuf -> POJO Plan
    Plan restored = new ProtoPlanConverter().from(proto);

    // POJO Plan -> SQL
    List<String> sql = new SubstraitToSql().convert(restored, SubstraitSqlDialect.DEFAULT);
    // --8<-- [end:round-trip]
    assertNotNull(sql);
  }
}
