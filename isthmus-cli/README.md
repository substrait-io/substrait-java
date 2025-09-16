# Isthmus-CLI

## Overview

Isthmus-CLI provides a native command-line interface to drive the [Isthmus](../isthmus) library. This can be used to convert SQL queries to [Substrait Protobuf](https://substrait.io/serialization/binary_serialization/) and SQL expressions to [Extended Expressions](https://substrait.io/expressions/extended_expression/) using the Calcite SQL compiler.

## Build

Isthmus can be built as a native executable via Graal. This requires a GraalVM JDK to be installed, and its install location set in the `GRAALVM_HOME` environment variable.

```
./gradlew nativeCompile
```

## Usage

### Version

```
$ ./isthmus-cli/build/native/nativeCompile/isthmus --version

isthmus 0.1
```

### Help

```
$ ./isthmus-cli/build/native/nativeCompile/isthmus --help

Usage: isthmus [-hV] [--outputformat=<outputFormat>]
               [--unquotedcasing=<unquotedCasing>] [-c=<createStatements>]...
               [-e=<sqlExpressions>...]... [<sql>]
Convert SQL Queries and SQL Expressions to Substrait
      [<sql>]     A SQL query
  -c, --create=<createStatements>
                  One or multiple create table statements e.g. CREATE
                    TABLE T1(foo int, bar bigint)
  -e, --expression=<sqlExpressions>...
                  One or more SQL expressions e.g. col + 1
  -h, --help      Show this help message and exit.
      --outputformat=<outputFormat>
                  Set the output format for the generated plan:
                    PROTOJSON, PROTOTEXT, BINARY
      --unquotedcasing=<unquotedCasing>
                  Calcite's casing policy for unquoted identifiers:
                    UNCHANGED, TO_UPPER, TO_LOWER
  -V, --version   Print version information and exit.
```

## Example

### SQL to Substrait Plan

```
> $ ./isthmus-cli/build/native/nativeCompile/isthmus \
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

### SQL Expression to Substrait Extended Expression

#### Projection

```
$ ./isthmus-cli/build/native/nativeCompile/isthmus -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
    -e  "N_REGIONKEY + 10"

{
  "extensionUris": [{
    "extensionUriAnchor": 1,
    "uri": "/functions_arithmetic.yaml"
  }],
  "extensions": [{
    "extensionFunction": {
      "extensionUriReference": 1,
      "functionAnchor": 0,
      "name": "add:i64_i64"
    }
  }],
  "referredExpr": [{
    "expression": {
      "scalarFunction": {
        "functionReference": 0,
        "args": [],
        "outputType": {
          "i64": {
            "typeVariationReference": 0,
            "nullability": "NULLABILITY_REQUIRED"
          }
        },
        "arguments": [{
          "value": {
            "selection": {
              "directReference": {
                "structField": {
                  "field": 2
                }
              },
              "rootReference": {
              }
            }
          }
        }, {
          "value": {
            "cast": {
              "type": {
                "i64": {
                  "typeVariationReference": 0,
                  "nullability": "NULLABILITY_REQUIRED"
                }
              },
              "input": {
                "literal": {
                  "i32": 10,
                  "nullable": false,
                  "typeVariationReference": 0
                }
              },
              "failureBehavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
            }
          }
        }],
        "options": []
      }
    },
    "outputNames": ["new-column"]
  }],
  "baseSchema": {
    "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
    "struct": {
      "types": [{
        "i64": {
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_REQUIRED"
        }
      }, {
        "fixedChar": {
          "length": 25,
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_NULLABLE"
        }
      }, {
        "i64": {
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_REQUIRED"
        }
      }, {
        "varchar": {
          "length": 152,
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_NULLABLE"
        }
      }],
      "typeVariationReference": 0,
      "nullability": "NULLABILITY_REQUIRED"
    }
  },
  "expectedTypeUrls": []
}
```

#### Filter

```
$ ./isthmus-cli/build/native/nativeCompile/isthmus -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
    -e  "N_REGIONKEY > 10"

{
  "extensionUris": [{
    "extensionUriAnchor": 1,
    "uri": "/functions_comparison.yaml"
  }],
  "extensions": [{
    "extensionFunction": {
      "extensionUriReference": 1,
      "functionAnchor": 0,
      "name": "gt:any_any"
    }
  }],
  "referredExpr": [{
    "expression": {
      "scalarFunction": {
        "functionReference": 0,
        "args": [],
        "outputType": {
          "bool": {
            "typeVariationReference": 0,
            "nullability": "NULLABILITY_REQUIRED"
          }
        },
        "arguments": [{
          "value": {
            "selection": {
              "directReference": {
                "structField": {
                  "field": 2
                }
              },
              "rootReference": {
              }
            }
          }
        }, {
          "value": {
            "cast": {
              "type": {
                "i64": {
                  "typeVariationReference": 0,
                  "nullability": "NULLABILITY_REQUIRED"
                }
              },
              "input": {
                "literal": {
                  "i32": 10,
                  "nullable": false,
                  "typeVariationReference": 0
                }
              },
              "failureBehavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
            }
          }
        }],
        "options": []
      }
    },
    "outputNames": ["new-column"]
  }],
  "baseSchema": {
    "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
    "struct": {
      "types": [{
        "i64": {
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_REQUIRED"
        }
      }, {
        "fixedChar": {
          "length": 25,
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_NULLABLE"
        }
      }, {
        "i64": {
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_REQUIRED"
        }
      }, {
        "varchar": {
          "length": 152,
          "typeVariationReference": 0,
          "nullability": "NULLABILITY_NULLABLE"
        }
      }],
      "typeVariationReference": 0,
      "nullability": "NULLABILITY_REQUIRED"
    }
  },
  "expectedTypeUrls": []
}
```
