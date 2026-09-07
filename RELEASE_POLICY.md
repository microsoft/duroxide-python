# Release Policy

`duroxide` is published to PyPI by Microsoft's internal open-source release
infrastructure. Publishing credentials and release execution remain outside
this repository.

## Contributor Workflow

Contributors can prepare a release through a pull request:

1. Update the version in `pyproject.toml` and `Cargo.toml`.
2. Update `CHANGELOG.md` and relevant documentation.
3. Confirm the pull request's `Build & Smoke` workflow passes.
4. Open a pull request for review.

After the release change is merged, the matching release tag is created from
`main` with explicit approval. A Microsoft maintainer then uses that tag with
the internal release pipeline to build and publish the package to PyPI.

## Publishing Boundary

- Do not publish directly to PyPI for the Microsoft release.
- Do not create a GitHub Release manually.
- Do not request or store Microsoft publishing credentials in this repository.

For release questions, open a GitHub issue without including credentials or
internal pipeline configuration.
