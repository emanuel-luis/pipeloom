# Changelog fragments

Each pull request adds its CHANGELOG entries as one file here, named after the branch (for example
`feat-table-diff.md`), instead of editing `CHANGELOG.md`. Parallel pull requests then never conflict on the
changelog.

A fragment uses the same sections as the changelog, in this order: Added, Changed, Deprecated, Removed, Fixed,
Security, Documentation.

```markdown
### Added

- One line per change, written for users, with links to the ADR or guide page.
```

The release commit (`chore(release): X.Y.Z`) folds every fragment into a new section of `CHANGELOG.md` and
deletes the fragments.
