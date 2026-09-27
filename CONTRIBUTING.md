# Contributing to Conductor

Thanks for your interest. Conductor is a small project with one maintainer, so
the process is deliberately light — but a few things are fixed.

## How changes land

GitHub is the development home. Branch from `main` and open a pull request
into `main`. Build, test and privacy checks must pass before merge. Changes
ship in tagged releases; see [the release guide](docs/RELEASING.md).

Use a GitHub noreply address for commit authorship if you prefer to keep
your personal address private. Review your diff and commit messages before
pushing: public history, logs and uploaded screenshots are public data.

## Before you start

- **Bugs** — open a [bug report](https://github.com/spencercnorton/conductor/issues/new/choose).
  A report with reproduction steps, versions and a scrubbed log excerpt is
  usually fixed faster than a pull request that arrives without one.
- **Features** — open a feature request first. Conductor has strong opinions
  about credential isolation, slot accounting and fail-closed guide data
  (see the README); an idea that cuts across them needs a conversation before
  code.
- **Security** — never in a public issue. Use
  [private vulnerability reporting](https://github.com/spencercnorton/conductor/security/advisories/new);
  see [SECURITY.md](SECURITY.md).

## Working on the code

```bash
go build ./...
go test -race -count=1 ./...        # what CI runs
go vet ./...        # lint; CI enforces it
```

- the HDHomeRun and XMLTV output shapes are contracts that Plex parses — change them only with a test that pins the exact bytes
- Keep a change to one concern. A pull request that fixes a bug and
  reformats a file is two pull requests.
- Tests: a bug fix carries a regression test; a feature carries the smallest
  test that fails without it.
- Commits carry a `Signed-off-by:` line (`git commit -s`, the Developer
  Certificate of Origin). There is no CLA.
- No secrets, hostnames, personal data or screenshots of a real desktop in
  the diff — the export gate rejects them and the pull request will be sent
  back.

## Out of scope

So nobody wastes an evening on it, Conductor will not accept:

- telemetry or analytics of any kind
- a hosted service, or anything that needs an account
- transcoding and virtual channels — Plex transcodes, and Tunarr already does virtual channels

## Pull request checklist

The template asks for what changed, why, and how it was tested, plus a
confirmation that the diff carries no secrets, machine names or personal
paths. Fill it in — it is what the reviewer reads first.

## Licence

By contributing you agree that your contribution is licensed under the
[MIT](LICENSE) that covers the project.
