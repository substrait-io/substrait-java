plugins {
    `maven-publish`
    id("java")
    id("scala")
    id("idea")
    //  id("com.palantir.graal") version "0.10.0"
    id("com.diffplug.spotless") version "6.5.1"
}

publishing { publications { create<MavenPublication>("maven") { from(components["java"]) } } }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
    implementation(project(":core")) {
        exclude("org.slf4j", "slf4j-jdk14")
        exclude("org.antlr", "antlr4")
    }
    implementation("org.scala-lang:scala-library:2.12.16")
    testImplementation("org.scalatest:scalatest_2.12:3.3.0-SNAP3")
    implementation("org.apache.spark:spark-sql_2.12:3.3.0")
    testImplementation("org.apache.spark:spark-sql_2.12:3.3.0:tests")
    testImplementation("org.apache.spark:spark-core_2.12:3.3.0:tests")
    testImplementation("org.apache.spark:spark-catalyst_2.12:3.3.0:tests")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    implementation("org.reflections:reflections:0.9.12")
    implementation("com.google.guava:guava:29.0-jre")
    //    implementation("org.graalvm.sdk:graal-sdk:22.0.0.2")
    //    implementation("info.picocli:picocli:4.6.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.4")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.4")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.12.4")
    implementation("com.google.protobuf:protobuf-java-util:3.17.3") {
        exclude("com.google.guava", "guava")
                .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
    }
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("com.github.ben-manes.caffeine:caffeine:3.0.4")
    implementation("org.immutables:value-annotations:2.8.8")
}

tasks {
    test {
        jvmArgs(
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.cs=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.lang=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.io=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.net=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.nio=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.util=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.security.action=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.util.calendar=ALL-UNNAMED"
        )
    }
}
