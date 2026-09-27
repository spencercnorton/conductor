# conductor development journal

Newest entries first. Record public engineering decisions without credentials,
operator infrastructure details or personal data.

## 2026-09-27 — GitHub development cutover

- GitHub pull requests are the source of truth for development.
- Added operations and release guides, required CI and privacy checks.
- Release tags rerun validation before producing checksummed public artifacts.
- Integration fixtures create independent disposable databases; media CI installs
  FFmpeg and FFprobe.
- Existing private history stays outside this repository. Demonstrations use
  synthetic data and reviewed public media.
