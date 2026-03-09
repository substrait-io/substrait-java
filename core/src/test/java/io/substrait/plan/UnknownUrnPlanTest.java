package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.protobuf.util.JsonFormat;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;

/**
 * Verifies that plans declaring extensions with unknown URNs can be deserialized, as long as the
 * relations don't reference those unknown functions.
 */
class UnknownUrnPlanTest {

  @Test
  void testPlanWithUnusedUnknownUrn() throws Exception {
    String json =
        """
        {
          "extensionUrns": [
            { "extensionUrnAnchor": 1, "urn": "extension:com.unknown:functions_custom" }
          ],
          "extensions": [
            {
              "extensionFunction": {
                "functionAnchor": 1,
                "name": "custom_func:i32_i32",
                "extensionUrnReference": 1
              }
            }
          ],
          "relations": [
            {
              "root": {
                "input": {
                  "read": {
                    "baseSchema": {
                      "names": ["a"],
                      "struct": { "types": [{ "i32": {} }] }
                    },
                    "namedTable": { "names": ["t"] }
                  }
                },
                "names": ["a"]
              }
            }
          ],
          "version": { "minorNumber": 75 }
        }
        """;

    Plan.Builder builder = Plan.newBuilder();
    JsonFormat.parser().merge(json, builder);
    Plan plan = builder.build();

    ProtoPlanConverter converter =
        new ProtoPlanConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);
    assertDoesNotThrow(() -> converter.from(plan));
  }
}
