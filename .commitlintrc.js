module.exports = {
  "extends": ["@commitlint/config-conventional"],
  "rules": {
    "body-max-line-length": [0, "always", Infinity],
    "footer-max-line-length": [0, "always", Infinity],
    "body-leading-blank": [0, "always"]
  },
  // Workaround for https://github.com/dependabot/dependabot-core/issues/5923
  "ignores": [
    (message) => /^Bumps \[.+]\(.+\) from .+ to .+\.$/m.test(message),
    (message) => /^Updates the requirements on .+ to permit the latest version\.$/m.test(message)
  ]
}
