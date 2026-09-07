---
name: pypi-publish
description: Releasing duroxide-python to PyPI through the internal tag-driven pipeline. Use when preparing or publishing a new version.
---

# Publishing duroxide-python to PyPI

Releases are published only through the internal release pipeline. The pipeline
is started manually with a merged Git tag, validates the release, and pauses at
its approval step before publishing to PyPI.

Do not publish from a local machine, create a GitHub Release to trigger
publication, or add PyPI credentials to this repository.

## 1. Prepare the release change

Choose a semantic version and use the matching `v`-prefixed Git tag. For
example, package version `0.1.29` uses tag `v0.1.29`.

Update all release metadata in one change:

- Set the same package version in `pyproject.toml` and `Cargo.toml`
- Move the relevant `CHANGELOG.md` entries from `[Unreleased]` into a dated
  section for the new version
- Follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format
- Include all Added/Changed/Fixed/Removed sections as applicable
- Confirm `README.md` still links to `CHANGELOG.md`
- Confirm `Cargo.toml` resolves `duroxide` and `duroxide-pg` from crates.io,
  with no local path overrides or `[patch.crates-io]` section

## 2. Validate before merge

Check that the PR's `Build & Smoke` workflow passes before merging. It builds
the source distribution and stable-ABI wheels for all supported platforms,
then installs and exercises those wheels on Python 3.9 through 3.14.

Merge the release change before creating the tag. Never start the internal
pipeline from an unmerged commit or a branch-only tag.

## 3. Tag the merged release commit

After the release change is merged, fetch `main`, verify the merged versions,
and tag that exact merged commit. In a worktree session, tag `origin/main`
directly instead of trying to check out `main`:

```bash
VERSION=0.1.29
TAG="v${VERSION}"

git fetch origin main
git show origin/main:pyproject.toml | grep -F "version = \"${VERSION}\""
git show origin/main:Cargo.toml | grep -F "version = \"${VERSION}\""
git tag "${TAG}" origin/main
git push origin "${TAG}"
```

The package versions must equal the tag without the leading `v`. Treat release
tags as immutable; do not move or reuse a published tag.

## 4. Run and approve the internal pipeline

1. Start the internal release pipeline manually.
2. Pass the Git tag, including the leading `v`, as the pipeline's tag
   parameter.
3. Wait for all build, validation, and publishing-preparation stages to
   succeed.
4. When the pipeline pauses at the approval step, report that it is waiting for
   an authorized human to approve it. Do not approve on the user's behalf or
   claim that the release is published before this approval.
5. After approval and pipeline completion, verify that the matching version is
   available on PyPI and imports in a clean environment:

   ```bash
   python3 -m venv /tmp/test-duroxide
   source /tmp/test-duroxide/bin/activate
   pip install duroxide==0.1.29
   python -c "from duroxide import SqliteProvider; print('loaded successfully')"
   deactivate
   rm -rf /tmp/test-duroxide
   ```

If the pipeline fails before approval, do not approve or publish manually.
Fix the release through the normal merge process and run the internal pipeline
with the appropriate merged release tag.
