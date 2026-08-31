package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Project;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * A {@link UserTypeMapper} has to reach a relation's literals as well as its schema, or the two
 * disagree and {@link VirtualTableScan} rejects the result.
 */
class UserTypeMapperLiteralTest extends PlanTestBase {

  private static final String URN = "extension:test:user_types";

  /**
   * Maps Calcite's character types to whatever the test asks for, leaving everything else alone.
   */
  private static ConverterProvider providerMapping(Function<Boolean, Type> characterTypes) {
    UserTypeMapper mapper =
        new UserTypeMapper() {
          @Nullable
          @Override
          public Type toSubstrait(RelDataType relDataType) {
            SqlTypeName name = relDataType.getSqlTypeName();
            if (name == SqlTypeName.CHAR || name == SqlTypeName.VARCHAR) {
              return characterTypes.apply(relDataType.isNullable());
            }
            return null;
          }

          @Nullable
          @Override
          public RelDataType toCalcite(Type.UserDefined type) {
            return null;
          }
        };
    return ConverterProvider.builder().typeConverter(new TypeConverter(mapper)).build();
  }

  private LogicalValues charValues(boolean withNull) {
    RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 1);
    RelDataType rowType =
        typeFactory
            .builder()
            .add("c", typeFactory.createTypeWithNullability(charType, withNull))
            .build();
    RexLiteral a = builder.getRexBuilder().makeLiteral("a");
    ImmutableList<ImmutableList<RexLiteral>> rows =
        withNull
            ? ImmutableList.of(
                ImmutableList.of(a),
                ImmutableList.of(
                    builder
                        .getRexBuilder()
                        .makeNullLiteral(typeFactory.createTypeWithNullability(charType, true))))
            : ImmutableList.of(ImmutableList.of(a));
    return LogicalValues.create(builder.getCluster(), rowType, rows);
  }

  private VirtualTableScan convert(ConverterProvider provider, LogicalValues values) {
    return assertInstanceOf(VirtualTableScan.class, SubstraitRelVisitor.convert(values, provider));
  }

  @Test
  void mappedCharacterTypeReachesTheLiterals() {
    VirtualTableScan converted =
        convert(
            providerMapping(
                nullable -> nullable ? TypeCreator.NULLABLE.STRING : TypeCreator.REQUIRED.STRING),
            charValues(false));

    assertEquals(List.of(R.STRING), converted.getInitialSchema().struct().fields());
    assertEquals(
        List.of(ExpressionCreator.string(false, "a")), converted.getRows().get(0).fields());
  }

  @Test
  void mappedVarcharWidthReachesTheLiterals() {
    VirtualTableScan converted =
        convert(
            providerMapping(
                nullable ->
                    nullable ? TypeCreator.NULLABLE.varChar(40) : TypeCreator.REQUIRED.varChar(40)),
            charValues(false));

    assertEquals(List.of(R.varChar(40)), converted.getInitialSchema().struct().fields());
    assertEquals(
        List.of(ExpressionCreator.varChar(false, "a", 40)), converted.getRows().get(0).fields());
  }

  /**
   * The mapped type already reached a null literal, which takes its type from the conversion rather
   * than being rebuilt; the non-null one beside it has to agree with it.
   */
  @Test
  void nullAndNonNullLiteralsCarryTheSameMappedType() {
    VirtualTableScan converted =
        convert(
            providerMapping(
                nullable -> nullable ? TypeCreator.NULLABLE.STRING : TypeCreator.REQUIRED.STRING),
            charValues(true));

    assertEquals(List.of(N.STRING), converted.getInitialSchema().struct().fields());
    assertEquals(List.of(ExpressionCreator.string(true, "a")), converted.getRows().get(0).fields());
    assertEquals(
        List.of(ExpressionCreator.typedNull(N.STRING)), converted.getRows().get(1).fields());
  }

  /**
   * A fixedchar literal carries no length of its own, so its text has to be padded to the declared
   * width. This bites without any mapper too, whenever a row field is wider than its literal.
   */
  @Test
  void aFixedCharLiteralIsPaddedToTheDeclaredWidth() {
    VirtualTableScan converted =
        convert(
            providerMapping(
                nullable ->
                    nullable
                        ? TypeCreator.NULLABLE.fixedChar(40)
                        : TypeCreator.REQUIRED.fixedChar(40)),
            charValues(false));

    assertEquals(List.of(R.fixedChar(40)), converted.getInitialSchema().struct().fields());
    Expression only = converted.getRows().get(0).fields().get(0);
    assertEquals(R.fixedChar(40), only.getType());
    assertEquals(ExpressionCreator.fixedChar(false, "a" + " ".repeat(39)), only);
  }

  @Test
  void aFixedCharLiteralNarrowerThanItsRowFieldIsPaddedWithoutAnyMapper() {
    RelDataType wide = typeFactory.createSqlType(SqlTypeName.CHAR, 3);
    RelDataType rowType = typeFactory.builder().add("c", wide).build();
    LogicalValues values =
        LogicalValues.create(
            builder.getCluster(),
            rowType,
            ImmutableList.of(ImmutableList.of(builder.getRexBuilder().makeLiteral("a"))));

    VirtualTableScan converted = convert(ConverterProvider.DEFAULT, values);

    assertEquals(List.of(R.fixedChar(3)), converted.getInitialSchema().struct().fields());
    assertEquals(
        List.of(ExpressionCreator.fixedChar(false, "a  ")), converted.getRows().get(0).fields());
  }

  @Test
  void aCharacterValueWiderThanItsDeclaredTypeIsRejected() {
    ConverterProvider provider =
        providerMapping(
            nullable ->
                nullable ? TypeCreator.NULLABLE.fixedChar(1) : TypeCreator.REQUIRED.fixedChar(1));
    RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 3);
    RelDataType rowType = typeFactory.builder().add("c", charType).build();
    LogicalValues values =
        LogicalValues.create(
            builder.getCluster(),
            rowType,
            ImmutableList.of(ImmutableList.of(builder.getRexBuilder().makeLiteral("abc"))));

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> convert(provider, values));
    assertTrue(e.getMessage().contains("fixedchar<1>"), e.getMessage());
  }

  /** A projection of a character literal, which no schema stands behind. */
  private RelNode charLiteralProject() {
    RelNode input = builder.values(new String[] {"i"}, 1).build();
    RexNode literal = builder.getRexBuilder().makeLiteral("a");
    return LogicalProject.create(input, List.of(), List.of(literal), List.of("c"), Set.of());
  }

  /**
   * A mapping outside the character family leaves the literal in the form Calcite declares. Its
   * type has no character literal form to build, and reaching one would need the value's encoding
   * in that type, which a {@link UserTypeMapper} has no way to give.
   */
  @Test
  void aMappingWithNoCharacterLiteralFormKeepsTheCalciteForm() {
    ConverterProvider provider =
        providerMapping(nullable -> TypeCreator.of(nullable).userDefined(URN, "u_type"));

    Project project =
        assertInstanceOf(
            Project.class, SubstraitRelVisitor.convert(charLiteralProject(), provider));

    assertEquals(ExpressionCreator.fixedChar(false, "a"), project.getExpressions().get(0));
  }

  /**
   * What that leaves open: a virtual table's rows still have to carry the schema's types, and a
   * mapping outside the character family puts the two out of step -- the schema takes the mapped
   * type while the literal keeps Calcite's. Closing it needs a literal-side hook the mapper does
   * not have.
   */
  @Test
  void aMappingWithNoCharacterLiteralFormStillDisagreesWithAVirtualTableSchema() {
    ConverterProvider provider =
        providerMapping(nullable -> TypeCreator.of(nullable).userDefined(URN, "u_type"));

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> convert(provider, charValues(false)));

    assertTrue(e.getMessage().contains("does not match schema field type"), e.getMessage());
  }
}
