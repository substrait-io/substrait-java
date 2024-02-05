# Isthmus

## Overview

Substrait Isthmus is a Java library which enables serializing SQL to [Substrait Protobuf](https://substrait.io/serialization/binary_serialization/) and SQL Expression to [Extended Expression](https://substrait.io/expressions/extended_expression/) via
the Calcite SQL compiler. Optionally, you can leverage the Calcite RelNode to Substrait Plan translator as an IR translation.

## Build

Isthmus can be built as a native executable via Graal

```
./gradlew nativeImage
```

## Usage

### Version

```
$ ./isthmus/build/graal/isthmus --version

isthmus 0.1
```

### Help

```
$ ./isthmus/build/graal/isthmus --help

Usage: isthmus [-hmV] [--crossjoinpolicy=<crossJoinPolicy>]
               [--outputformat=<outputFormat>]
               [--sqlconformancemode=<sqlConformanceMode>]
               [-c=<createStatements>]... [-e=<sqlExpressions>...]... [<sql>]
Substrait Java Native Image for parsing SQL Query and SQL Expressions
      [<sql>]            The sql we should parse.
  -c, --create=<createStatements>
                         One or multiple create table statements e.g. CREATE
                           TABLE T1(foo int, bar bigint)
      --crossjoinpolicy=<crossJoinPolicy>
                         One of built-in Calcite SQL compatibility modes:
                           KEEP_AS_CROSS_JOIN, CONVERT_TO_INNER_JOIN
  -e, --expression=<sqlExpressions>...
                         One or more SQL expressions e.g. col + 1
  -h, --help             Show this help message and exit.
  -m, --multistatement   Allow multiple statements terminated with a semicolon
      --outputformat=<outputFormat>
                         Set the output format for the generated plan:
                           PROTOJSON, PROTOTEXT, BINARY
      --sqlconformancemode=<sqlConformanceMode>
                         One of built-in Calcite SQL compatibility modes:
                           DEFAULT, LENIENT, BABEL, STRICT_92, STRICT_99,
                           PRAGMATIC_99, BIG_QUERY, MYSQL_5, ORACLE_10,
                           ORACLE_12, STRICT_2003, PRAGMATIC_2003, PRESTO,
                           SQL_SERVER_2008
  -V, --version          Print version information and exit.

```

## Example

### SQL to Substrait Plan

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

### SQL Expression to Substrait Extended Expression

#### Projection

```
$ ./isthmus/build/graal/isthmus -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
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
$ ./isthmus/build/graal/isthmus -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
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
