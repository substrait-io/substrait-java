# Isthmus

## Overview

Substrait Isthmus is a Java library which enables serializing SQL to [Substrait Protobuf](https://substrait.io/serialization/binary_serialization/) via
the Calcite SQL compiler. Optionally, you can leverage the Calcite RelNode to Substrait Plan translator as an IR translation.

## Build

Isthmus can be built as a native executable via Graal

```
./gradlew nativeImage
```

## Usage

```
$ ./isthmus/build/graal/isthmus

Usage: isthmus [-m] [-c=<createStatements>]... <sql>
Converts a SQL query to a Substrait Plan
      <sql>              The sql we should parse.
  -c, --create=<createStatements>
                         One or multiple create table statements e.g. CREATE
                           TABLE T1(foo int, bar bigint)
  -m, --multistatement   Allow multiple statements terminated with a semicolon
```

## Example

```
> $ ./isthmus/build/graal/isthmus \
  -c "CREATE TABLE Persons ( firstName VARCHAR, lastName VARCHAR, zip INT )" \
     "SELECT lastName, firstName FROM Persons WHERE zip = 90210"

{
  "extensionUris": [{
    "extensionUriAnchor": 1,
    "uri": "/functions_comparison.yaml"
  }],
  "extensions": [{
    "extensionFunction": {
      "extensionUriReference": 1,
      "functionAnchor": 0,
      "name": "equal:any1_any1"
    }
  }],
  "relations": [{
    "root": {
      "input": {
        "project": {
          "common": {
            "emit": {
              "outputMapping": [3, 4]
            }
          },
          "input": {
            "filter": {
              "common": {
                "direct": {
                }
              },
              "input": {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["FIRSTNAME", "LASTNAME", "ZIP"],
                    "struct": {
                      "types": [{
                        "string": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }, {
                        "string": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }, {
                        "i32": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }],
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_REQUIRED"
                    }
                  },
                  "namedTable": {
                    "names": ["PERSONS"]
                  }
                }
              },
              "condition": {
                "scalarFunction": {
                  "functionReference": 0,
                  "args": [{
                    "selection": {
                      "directReference": {
                        "structField": {
                          "field": 2
                        }
                      },
                      "rootReference": {
                      }
                    }
                  }, {
                    "literal": {
                      "i32": 90210,
                      "nullable": false,
                      "typeVariationReference": 0
                    }
                  }],
                  "outputType": {
                    "bool": {
                      "typeVariationReference": 0,
                      "nullability": "NULLABILITY_NULLABLE"
                    }
                  },
                  "arguments": []
                }
              }
            }
          },
          "expressions": [{
            "selection": {
              "directReference": {
                "structField": {
                  "field": 1
                }
              },
              "rootReference": {
              }
            }
          }, {
            "selection": {
              "directReference": {
                "structField": {
                  "field": 0
                }
              },
              "rootReference": {
              }
            }
          }]
        }
      },
      "names": ["LASTNAME", "FIRSTNAME"]
    }
  }],
  "expectedTypeUrls": []
}
```
