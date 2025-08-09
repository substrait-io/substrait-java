rootProject.name = "substrait"

includeBuild("build-logic")

include("bom", "core", "isthmus", "isthmus-cli", "spark", "examples:substrait-spark")
