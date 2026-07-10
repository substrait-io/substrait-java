// SPDX-License-Identifier: Apache-2.0
//
// semantic-release configuration.
//
// This is an ESM (.mjs) config rather than .releaserc.json because the
// release-notes-generator needs a `writerOpts.transform` function to strip git
// trailers (e.g. `Signed-off-by:`) that the conventional-commits parser folds
// into `BREAKING CHANGE` notes. JSON cannot carry functions.
//
// This config defaults to a dry run as a fail-safe: the publishing plugins
// (@semantic-release/github, @semantic-release/git) and the exec verify/publish
// commands are only included when RELEASE_DRY_RUN=false. ci/release/run.sh opts
// in to a real release that way; every other invocation -- including a typo'd or
// forgotten env var -- stays harmless.
//
// The env var is needed on top of --dry-run because --dry-run alone is not
// enough: semantic-release still runs every plugin's verifyConditions step in
// dry-run mode (it skips only prepare, publish, addChannel, success and fail).
// @semantic-release/github's verifyConditions fails without a GITHUB_TOKEN, so
// the side-effecting plugins must be omitted entirely, not merely guarded by
// --dry-run, to allow a credential-free dry run.

import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

// Load the conventionalcommits preset. The release scripts run semantic-release
// via `npx -p ...`, which installs the preset alongside semantic-release in a
// temporary node_modules. A bare `import` here would resolve relative to this
// config file's directory (the repo, which has no node_modules) and fail, so
// fall back to resolving the preset relative to the running semantic-release
// binary (process.argv[1]). The plain import still covers local dev where the
// preset is installed alongside the project.
const loadPreset = async () => {
  try {
    return (await import("conventional-changelog-conventionalcommits")).default;
  } catch {
    const require = createRequire(pathToFileURL(process.argv[1]));
    const resolved = pathToFileURL(
      require.resolve("conventional-changelog-conventionalcommits")
    ).href;
    return (await import(resolved)).default;
  }
};
const conventionalcommits = await loadPreset();

// Git trailers that should never appear in the changelog or release notes.
const TRAILER_KEYS = [
  "Signed-off-by",
  "Co-authored-by",
  "Co-developed-by",
  "Reviewed-by",
  "Acked-by",
  "Tested-by",
  "Reported-by",
  "Suggested-by",
  "Helped-by",
  "Cc",
];
const TRAILER = new RegExp(`^(?:${TRAILER_KEYS.join("|")}):\\s`, "i");

// The conventional-commits parser ends a BREAKING CHANGE note only at a
// recognized reference (closes #..., fixes #...) or another note keyword, not
// at a git trailer -- so a trailing `Signed-off-by:` gets absorbed into the
// note text. Strip such trailing trailer lines.
const stripTrailers = (text) => {
  if (!text) {
    return text;
  }

  const lines = text.split("\n");
  while (lines.length) {
    const last = lines[lines.length - 1].trim();
    if (last === "" || TRAILER.test(last)) {
      lines.pop();
    } else {
      break;
    }
  }
  return lines.join("\n");
};

const preset = await conventionalcommits();
const presetTransform = preset.writer.transform;
const dryRun = process.env.RELEASE_DRY_RUN !== "false";

export default {
  branches: [
    { name: "+([0-9])?(.{+([0-9]),x}).x" },
    { name: "main" },
    { name: "next" },
    { name: "next-major" },
    { name: "beta", prerelease: true },
    { name: "alpha", prerelease: true },
  ],
  preset: "conventionalcommits",
  dryRun,
  plugins: [
    [
      "@semantic-release/release-notes-generator",
      {
        // Only `transform` is overridden; the generator merges this over the
        // preset's writer options, so templates/grouping/sorting are kept.
        writerOpts: {
          transform(commit, context) {
            const out = presetTransform(commit, context);
            if (out && Array.isArray(out.notes)) {
              out.notes = out.notes.map((note) => ({
                ...note,
                text: stripTrailers(note.text),
              }));
            }
            return out;
          },
        },
      },
    ],
    [
      "@semantic-release/commit-analyzer",
      {
        releaseRules: [{ breaking: true, release: "minor" }],
      },
    ],
    [
      "@semantic-release/exec",
      dryRun
        ? {}
        : {
            verifyConditionsCmd: "ci/release/verify.sh",
            prepareCmd: "ci/release/prepare.sh ${nextRelease.version}",
            publishCmd: "ci/release/publish.sh",
          },
    ],
    [
      "@semantic-release/changelog",
      {
        changelogTitle: "Release Notes\n---",
        changelogFile: "CHANGELOG.md",
      },
    ],
    ...(dryRun
      ? []
      : [
          [
            "@semantic-release/github",
            {
              assets: [
                {
                  path: "native/libs/isthmus-macOS-latest",
                  name: "isthmus-macOS-${nextRelease.version}",
                  label: "Isthmus Native Image - macOS",
                },
                {
                  path: "native/libs/isthmus-ubuntu-latest",
                  name: "isthmus-ubuntu-${nextRelease.version}",
                  label: "Isthmus Native Image - Linux",
                },
              ],
              successComment: false,
            },
          ],
          [
            "@semantic-release/git",
            {
              assets: ["gradle.properties", "CHANGELOG.md"],
              message: "chore(release): ${nextRelease.version}",
            },
          ],
        ]),
  ],
};
