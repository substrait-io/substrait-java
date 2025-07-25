package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.FieldReference;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

class SubqueryConversionTest extends PlanTestBase {
  protected final SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);

  @Test
  void testOuterFieldReferenceOneStep() {
    /*
     * SELECT
     *   orders.o_orderkey,
     *   (SELECT customer.c_nationkey FROM customer WHERE customer.c_custkey = orders.o_custkey)
     * FROM orders
     */
    final Rel root =
        substraitBuilder.project(
            input ->
                List.of(
                    // orders.o_orderkey
                    substraitBuilder.fieldReference(input, 0),
                    // (SELECT customer.c_nationkey FROM customer WHERE customer.c_custkey =
                    // orders.o_custkey)
                    substraitBuilder.scalarSubquery(
                        substraitBuilder.project(
                            input2 -> List.of(substraitBuilder.fieldReference(input2, 1)),
                            Remap.of(List.of(1)),
                            substraitBuilder.filter(
                                input2 ->
                                    substraitBuilder.equal(
                                        // customer.c_custkey
                                        substraitBuilder.fieldReference(input2, 0),
                                        // orders.o_custkey
                                        FieldReference.newRootStructOuterReference(
                                            1, TypeCreator.REQUIRED.I64, 1)),
                                substraitBuilder.namedScan(
                                    List.of("customer"),
                                    List.of("c_custkey", "c_nationkey"),
                                    List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64)))),
                        TypeCreator.NULLABLE.I64)),
            Remap.of(List.of(2, 3)),
            substraitBuilder.namedScan(
                List.of("orders"),
                List.of("o_orderkey", "o_custkey"),
                List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64)));

    final RelNode calciteRel = converter.convert(root);

    // LogicalFilter has field reference with $cor0 correlation variable
    // outer LogicalProject has variablesSet containing $cor0 correlation variable
    assertEquals(
        "LogicalProject(variablesSet=[[$cor0]], o_orderkey0=[$0], $f3=[$SCALAR_QUERY({\n"
            + "LogicalProject(c_nationkey=[$1])\n"
            + "  LogicalFilter(condition=[=($0, $cor0.o_custkey)])\n"
            + "    LogicalTableScan(table=[[customer]])\n"
            + "})])\n"
            + "  LogicalTableScan(table=[[orders]])\n",
        calciteRel.explain());

    assertEquals(
        "SELECT \"o_orderkey\" AS \"o_orderkey0\", (((SELECT \"c_nationkey\"\n"
            + "FROM \"customer\"\n"
            + "WHERE \"c_custkey\" = \"orders\".\"o_custkey\"))) AS \"$f3\"\n"
            + "FROM \"orders\"",
        SubstraitSqlDialect.toSql(calciteRel).getSql());
  }

  @Test
  void testOuterFieldReferenceTwoSteps() {
    /*
     * SELECT
     *   orders.o_orderkey,
     *   (
     *     SELECT
     *       n_name
     *     FROM nation
     *     WHERE n_nationkey =
     *       (
     *         SELECT
     *           customer.c_nationkey
     *         FROM customer
     *         WHERE
     *           customer.c_custkey = orders.o_custkey
     *       )
     *   )
     * FROM orders
     */
    final Rel root =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.fieldReference(input, 0),
                    substraitBuilder.scalarSubquery(
                        substraitBuilder.project(
                            input2 -> List.of(substraitBuilder.fieldReference(input2, 1)),
                            Remap.of(List.of(2)),
                            substraitBuilder.filter(
                                input2 ->
                                    substraitBuilder.equal(
                                        substraitBuilder.fieldReference(input2, 0),
                                        substraitBuilder.scalarSubquery(
                                            substraitBuilder.project(
                                                input3 ->
                                                    List.of(
                                                        substraitBuilder.fieldReference(input3, 1)),
                                                Remap.of(List.of(1)),
                                                substraitBuilder.filter(
                                                    input3 ->
                                                        substraitBuilder.equal(
                                                            // customer.c_custkey
                                                            substraitBuilder.fieldReference(
                                                                input3, 0),
                                                            // orders.o_custkey
                                                            FieldReference
                                                                .newRootStructOuterReference(
                                                                    1,
                                                                    TypeCreator.REQUIRED.I64,
                                                                    2)),
                                                    substraitBuilder.namedScan(
                                                        List.of("customer"),
                                                        List.of("c_custkey", "c_nationkey"),
                                                        List.of(
                                                            TypeCreator.REQUIRED.I64,
                                                            TypeCreator.REQUIRED.I64)))),
                                            TypeCreator.NULLABLE.I64)),
                                substraitBuilder.namedScan(
                                    List.of("nation"),
                                    List.of("n_nationkey", "n_name"),
                                    List.of(
                                        TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING)))),
                        TypeCreator.NULLABLE.STRING)),
            Remap.of(List.of(2, 3)),
            substraitBuilder.namedScan(
                List.of("orders"),
                List.of("o_orderkey", "o_custkey"),
                List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64)));

    final RelNode calciteRel = converter.convert(root);

    // most inner LogicalFilter has field reference with $cor0 correlation variable
    // most outer LogicalProject has variablesSet containing $cor0 correlation variable
    assertEquals(
        "LogicalProject(variablesSet=[[$cor0]], o_orderkey0=[$0], $f3=[$SCALAR_QUERY({\n"
            + "LogicalProject(n_name0=[$1])\n"
            + "  LogicalFilter(condition=[=($0, $SCALAR_QUERY({\n"
            + "LogicalProject(c_nationkey=[$1])\n"
            + "  LogicalFilter(condition=[=($0, $cor0.o_custkey)])\n"
            + "    LogicalTableScan(table=[[customer]])\n"
            + "}))])\n"
            + "    LogicalTableScan(table=[[nation]])\n"
            + "})])\n"
            + "  LogicalTableScan(table=[[orders]])\n",
        calciteRel.explain());

    assertEquals(
        "SELECT \"o_orderkey\" AS \"o_orderkey0\", (((SELECT \"n_name\" AS \"n_name0\"\n"
            + "FROM \"nation\"\n"
            + "WHERE \"n_nationkey\" = (((SELECT \"c_nationkey\"\n"
            + "FROM \"customer\"\n"
            + "WHERE \"c_custkey\" = \"orders\".\"o_custkey\")))))) AS \"$f3\"\n"
            + "FROM \"orders\"",
        SubstraitSqlDialect.toSql(calciteRel).getSql());
  }

  @Test
  void testInPredicateOuterFieldReference() {
    /*
     * SELECT
     *   orders.o_orderkey,
     *   (
     *     SELECT
     *       n_name
     *     FROM nation
     *     WHERE n_nationkey IN
     *       (
     *         SELECT
     *           customer.c_nationkey
     *         FROM customer
     *         WHERE
     *           customer.c_custkey = orders.o_custkey
     *       )
     *   )
     * FROM orders
     */
    final Rel root =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.fieldReference(input, 0),
                    substraitBuilder.scalarSubquery(
                        substraitBuilder.project(
                            input2 -> List.of(substraitBuilder.fieldReference(input2, 1)),
                            Remap.of(List.of(2)),
                            substraitBuilder.filter(
                                input2 ->
                                    substraitBuilder.inPredicate(
                                        substraitBuilder.project(
                                            input3 ->
                                                List.of(substraitBuilder.fieldReference(input3, 1)),
                                            Remap.of(List.of(1)),
                                            substraitBuilder.filter(
                                                input3 ->
                                                    substraitBuilder.equal(
                                                        // customer.c_custkey
                                                        substraitBuilder.fieldReference(input3, 0),
                                                        // orders.o_custkey
                                                        FieldReference.newRootStructOuterReference(
                                                            1, TypeCreator.REQUIRED.I64, 2)),
                                                substraitBuilder.namedScan(
                                                    List.of("customer"),
                                                    List.of("c_custkey", "c_nationkey"),
                                                    List.of(
                                                        TypeCreator.REQUIRED.I64,
                                                        TypeCreator.REQUIRED.I64)))),
                                        substraitBuilder.fieldReference(input2, 0)),
                                substraitBuilder.namedScan(
                                    List.of("nation"),
                                    List.of("n_nationkey", "n_name"),
                                    List.of(
                                        TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING)))),
                        TypeCreator.NULLABLE.STRING)),
            Remap.of(List.of(2, 3)),
            substraitBuilder.namedScan(
                List.of("orders"),
                List.of("o_orderkey", "o_custkey"),
                List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64)));

    final RelNode calciteRel = converter.convert(root);

    // most inner LogicalFilter has field reference with $cor0 correlation variable
    // most outer LogicalProject has variablesSet containing $cor0 correlation variable
    assertEquals(
        "LogicalProject(variablesSet=[[$cor0]], o_orderkey0=[$0], $f3=[$SCALAR_QUERY({\n"
            + "LogicalProject(n_name0=[$1])\n"
            + "  LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(c_nationkey=[$1])\n"
            + "  LogicalFilter(condition=[=($0, $cor0.o_custkey)])\n"
            + "    LogicalTableScan(table=[[customer]])\n"
            + "})])\n"
            + "    LogicalTableScan(table=[[nation]])\n"
            + "})])\n"
            + "  LogicalTableScan(table=[[orders]])\n",
        calciteRel.explain());

    assertEquals(
        "SELECT \"o_orderkey\" AS \"o_orderkey0\", (((SELECT \"n_name\" AS \"n_name0\"\n"
            + "FROM \"nation\"\n"
            + "WHERE \"n_nationkey\" IN (SELECT \"c_nationkey\"\n"
            + "FROM \"customer\"\n"
            + "WHERE \"c_custkey\" = \"orders\".\"o_custkey\")))) AS \"$f3\"\n"
            + "FROM \"orders\"",
        SubstraitSqlDialect.toSql(calciteRel).getSql());
  }

  @Test
  void testSetPredicateOuterFieldReference() {
    /*
     * SELECT
     *   orders.o_orderkey,
     *   (
     *     SELECT
     *       n_name
     *     FROM nation
     *     WHERE EXISTS
     *       (
     *         SELECT
     *           customer.c_nationkey
     *         FROM customer
     *         WHERE
     *           customer.c_custkey = orders.o_custkey
     *           AND customer.c_nationkey = nation.n_nationkey
     *       )
     *   )
     * FROM orders
     */
    final Rel root =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.fieldReference(input, 0),
                    substraitBuilder.scalarSubquery(
                        substraitBuilder.project(
                            input2 -> List.of(substraitBuilder.fieldReference(input2, 1)),
                            Remap.of(List.of(2)),
                            substraitBuilder.filter(
                                input2 ->
                                    substraitBuilder.exists(
                                        substraitBuilder.project(
                                            input3 ->
                                                List.of(substraitBuilder.fieldReference(input3, 1)),
                                            Remap.of(List.of(1)),
                                            substraitBuilder.filter(
                                                input3 ->
                                                    substraitBuilder.and(
                                                        substraitBuilder.equal(
                                                            // customer.c_custkey
                                                            substraitBuilder.fieldReference(
                                                                input3, 0),
                                                            // orders.o_custkey
                                                            FieldReference
                                                                .newRootStructOuterReference(
                                                                    1,
                                                                    TypeCreator.REQUIRED.I64,
                                                                    2)),
                                                        substraitBuilder.equal(
                                                            // customer.c_nationkey
                                                            substraitBuilder.fieldReference(
                                                                input3, 1),
                                                            // nation.n_nationkey
                                                            FieldReference
                                                                .newRootStructOuterReference(
                                                                    0,
                                                                    TypeCreator.REQUIRED.I64,
                                                                    1))),
                                                substraitBuilder.namedScan(
                                                    List.of("customer"),
                                                    List.of("c_custkey", "c_nationkey"),
                                                    List.of(
                                                        TypeCreator.REQUIRED.I64,
                                                        TypeCreator.REQUIRED.I64))))),
                                substraitBuilder.namedScan(
                                    List.of("nation"),
                                    List.of("n_nationkey", "n_name"),
                                    List.of(
                                        TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING)))),
                        TypeCreator.NULLABLE.STRING)),
            Remap.of(List.of(2, 3)),
            substraitBuilder.namedScan(
                List.of("orders"),
                List.of("o_orderkey", "o_custkey"),
                List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.I64)));

    final RelNode calciteRel = converter.convert(root);

    // most inner LogicalFilter has field references with $cor0 and $cor1 correlation variables
    // most outer LogicalProject has variablesSet containing $cor0 correlation variable
    // most outer LogicalFilter has variablesSet containing $cor1 correlation variable
    assertEquals(
        "LogicalProject(variablesSet=[[$cor0]], o_orderkey0=[$0], $f3=[$SCALAR_QUERY({\n"
            + "LogicalProject(n_name0=[$1])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalProject(c_nationkey=[$1])\n"
            + "  LogicalFilter(condition=[AND(=($0, $cor0.o_custkey), =($1, $cor1.n_nationkey))])\n"
            + "    LogicalTableScan(table=[[customer]])\n"
            + "})], variablesSet=[[$cor1]])\n"
            + "    LogicalTableScan(table=[[nation]])\n"
            + "})])\n"
            + "  LogicalTableScan(table=[[orders]])\n",
        calciteRel.explain());

    assertEquals(
        "SELECT \"o_orderkey\" AS \"o_orderkey0\", (((SELECT \"n_name\" AS \"n_name0\"\n"
            + "FROM \"nation\"\n"
            + "WHERE EXISTS (SELECT \"c_nationkey\"\n"
            + "FROM \"customer\"\n"
            + "WHERE \"c_custkey\" = \"orders\".\"o_custkey\" AND \"c_nationkey\" = \"nation\".\"n_nationkey\")))) AS \"$f3\"\n"
            + "FROM \"orders\"",
        SubstraitSqlDialect.toSql(calciteRel).getSql());
  }
}
