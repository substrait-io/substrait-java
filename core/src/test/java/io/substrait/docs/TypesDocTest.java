package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/types.md}. Regions marked with {@code // --8<--
 * [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet includes.
 */
class TypesDocTest extends TestBase {

  @Test
  void aliases() {
    // --8<-- [start:aliases]
    TypeCreator R = TypeCreator.REQUIRED;
    TypeCreator N = TypeCreator.NULLABLE;
    // --8<-- [end:aliases]
    assertNotNull(R.I32);
    assertNotNull(N.STRING);
  }

  @Test
  void ofNullability() {
    boolean nullable = true;
    // --8<-- [start:of]
    TypeCreator t = TypeCreator.of(nullable); // NULLABLE if true, else REQUIRED
    // --8<-- [end:of]
    assertNotNull(t.I32);
  }

  @Test
  void flipNullability() {
    // --8<-- [start:flip-nullability]
    io.substrait.type.Type nullableI32 = TypeCreator.asNullable(R.I32);
    io.substrait.type.Type requiredI32 = TypeCreator.asNotNullable(N.I32);
    // --8<-- [end:flip-nullability]
    assertNotNull(nullableI32);
    assertNotNull(requiredI32);
  }

  @Test
  void scalarConstants() {
    // --8<-- [start:scalar-constants]
    TypeCreator R = TypeCreator.REQUIRED;

    io.substrait.type.Type i32 = R.I32; // non-nullable i32
    io.substrait.type.Type str = R.STRING; // non-nullable string
    io.substrait.type.Type fp64 = R.FP64; // non-nullable fp64
    io.substrait.type.Type date = R.DATE; // non-nullable date
    // --8<-- [end:scalar-constants]
    assertNotNull(i32);
    assertNotNull(str);
    assertNotNull(fp64);
    assertNotNull(date);
  }

  @Test
  void parameterizedTypes() {
    // --8<-- [start:parameterized]
    TypeCreator R = TypeCreator.REQUIRED;

    // decimal(precision, scale)
    io.substrait.type.Type dec = R.decimal(10, 2);

    // fixed- and variable-length character/binary
    io.substrait.type.Type fchar = R.fixedChar(20);
    io.substrait.type.Type vchar = R.varChar(255);
    io.substrait.type.Type fbin = R.fixedBinary(16);

    // temporal types with fractional-second precision
    io.substrait.type.Type ts = R.precisionTimestamp(6);
    io.substrait.type.Type tstz = R.precisionTimestampTZ(6);
    io.substrait.type.Type time = R.precisionTime(6);
    io.substrait.type.Type iday = R.intervalDay(6);
    // --8<-- [end:parameterized]
    assertNotNull(dec);
    assertNotNull(fchar);
    assertNotNull(vchar);
    assertNotNull(fbin);
    assertNotNull(ts);
    assertNotNull(tstz);
    assertNotNull(time);
    assertNotNull(iday);
  }

  @Test
  void structsListsMaps() {
    // --8<-- [start:struct-list-map]
    TypeCreator R = TypeCreator.REQUIRED;

    // struct with i32 and string fields
    io.substrait.type.Type.Struct struct = R.struct(R.I32, R.STRING);

    // list of strings
    io.substrait.type.Type list = R.list(R.STRING);

    // map from string to i32
    io.substrait.type.Type map = R.map(R.STRING, R.I32);
    // --8<-- [end:struct-list-map]
    assertNotNull(struct);
    assertNotNull(list);
    assertNotNull(map);
  }

  @Test
  void userDefinedType() {
    // --8<-- [start:user-defined]
    io.substrait.type.Type udt =
        TypeCreator.REQUIRED.userDefined("extension:my.org:my_types", "point");
    // --8<-- [end:user-defined]
    assertNotNull(udt);
  }

  @Test
  void namedStruct() {
    // --8<-- [start:named-struct]
    NamedStruct schema =
        NamedStruct.of(List.of("a", "b"), TypeCreator.REQUIRED.struct(R.I32, R.STRING));
    // --8<-- [end:named-struct]
    assertNotNull(schema);
  }
}
