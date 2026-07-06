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
// (@semantic-release/github, @semantic-release/git) and the verify/publish exec
// commands are only included when RELEASE_DRY_RUN=false. ci/release/run.sh opts
// in to a real release that way; every other invocation stays harmless.

import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

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
