---
name: release-preparation
description: Preparing duroxide-python release pull requests. Use when bumping the package version, updating release notes, or preparing a new PyPI release.
---

# Release Preparation

This skill prepares the repository changes for a release. Publishing is handled
by Microsoft's internal release infrastructure as described in
`RELEASE_POLICY.md`.

Do not publish to PyPI, create a GitHub Release, start the internal release
pipeline, or request publishing credentials.

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

## Create the Release Pull Request

Commit the release preparation, push the branch, and create a pull request.
Check that its `Build & Smoke` workflow passes. Do not merge the pull request
on the user's behalf.

## Tag the Merged Release

After the pull request is merged:

1. Resolve the pull request's exact merge commit SHA from GitHub and confirm
   the pull request state is `MERGED`.
2. Fetch `origin/main`, confirm the merge SHA is contained in it, and verify the
   package and changelog versions at that exact commit.
3. Ask the user for explicit approval to create and push the release tag. Prior
   approval to prepare the release or create the pull request is not sufficient.
4. Confirm the `v`-prefixed tag does not already exist locally or on `origin`.
5. Create the tag on the recorded merge SHA and push it to `origin`.

For example, package version `0.1.29` uses tag `v0.1.29`. Treat release tags as
immutable; never move or reuse an existing tag. Never substitute the current
`origin/main` or `HEAD` tip for the release pull request's merge SHA.
