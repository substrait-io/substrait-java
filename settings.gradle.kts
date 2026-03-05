rootProject.name = "substrait"

includeBuild("build-logic")

include(
  "bom",
  "core",
  "isthmus",
  "isthmus-cli",
  "spark",
  "spark:spark-3.4_2.12",
  "spark:spark-3.5_2.12",
  "spark:spark-4.0_2.13",
  "examples:substrait-spark",
  "examples:isthmus-api",
)
