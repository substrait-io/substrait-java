plugins {
  id("pmd")
}

plugins.withType<PmdPlugin>().configureEach {
  configure<PmdExtension> {
    setConsoleOutput(true)
    setToolVersion("7.15.0")
    setRuleSetConfig(resources.text.fromUri(getBuildscript().getClassLoader().getResource("substrait-pmd.xml").toURI()))
  }
}
