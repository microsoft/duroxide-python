---
name: release-preparation
description: Preparing duroxide-python release pull requests. Use when bumping the package version, updating release notes, or preparing a new PyPI release.
---

# Release Preparation

This skill prepares the repository changes for a release. Publishing is handled
by Microsoft's internal release infrastructure as described in
`RELEASE_POLICY.md`.

Do not publish to PyPI, create a GitHub Release, create or push a Git tag, start
the internal release pipeline, or request publishing credentials.

## Prepare the Release

1. Confirm the target semantic version with the user if it was not provided.
2. Set the same version in:
   - `pyproject.toml` under `[project]`
   - `Cargo.toml` under `[package]`
3. Move the relevant entries from `CHANGELOG.md`'s `[Unreleased]` section into
   a section named for the target version and current date.
4. Keep an empty `[Unreleased]` section above the new release entry.
5. Update directly related documentation when the release changes documented
   behavior.
6. Confirm `Cargo.toml` uses registry versions for `duroxide` and
   `duroxide-pg`, with no local path overrides or `[patch.crates-io]` section.

Do not invent changelog entries or include unrelated changes in the release
preparation.

## Validate the Preparation

Before presenting the changes:

- Confirm the versions in `pyproject.toml` and `Cargo.toml` match.
- Confirm the changelog version and date are correct.
- Confirm the diff contains only the intended release preparation.

After the release pull request is opened, check that its `Build & Smoke`
workflow passes before considering the preparation complete.
