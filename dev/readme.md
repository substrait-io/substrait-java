Substrait Java Code Development

# Semantic Release conventions


## Commit Conventions

Substrait Java follows [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit message structure. You can use [`pre-commit`](https://pre-commit.com/) to check your messages for you, but note that you must install pre-commit using `pre-commit install --hook-type commit-msg` for this to work. CI will also lint your commit messages. Please also ensure that your PR title and initial comment together form a valid commit message; that will save us some work formatting the merge commit message when we merge your PR.

```bash
$ pre-commit install --hook-type commit-msg
pre-commit installed at .git/hooks/commit-msg
```

Examples of commit messages can be seen [here](https://www.conventionalcommits.org/en/v1.0.0/#examples).


# Use Substrait Java artifacts

## Maven Use

pom.xml
```xml
    <repositories>
        <repository>
            <id>github</id>
            <url>https://maven.pkg.github.com/davisusanibar/substrait-java</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>io.substrait</groupId>
            <artifactId>substrait-java</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>
    </dependencies>
```

settings.xml
````xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>github</id>
      <username>davisusanibar</username>
      <password>YOUR_TOKEN_WITH_READ_ACCESS</password>
    </server>
  </servers>
</settings>
````

## Gradle Use

build.gradle
```shell
repositories {
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/davisusanibar/substrait-java")
        credentials {
            username = System.getenv("USERNAME")
            password = System.getenv("GITHUB_TOKEN_READ")
        }
    }
}

dependencies {
    implementation 'io.substrait:substrait-java:0.0.1-SNAPSHOT'
    testImplementation 'org.junit.jupiter:junit-jupiter-api:5.7.0'
    testRuntimeOnly 'org.junit.jupiter:junit-jupiter-engine:5.7.0'
}
```


