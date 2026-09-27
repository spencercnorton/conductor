# Releasing

GitHub is the development home. Branch from `main`, submit a pull request,
and merge after the required build, test and privacy checks pass. Keep changes
under `## Unreleased` in the changelog until preparing a release.

## Release checklist

1. Confirm the release commit is on `main`, with all required checks green.
2. Run the documented build and tests from a fresh clone. Test desktop changes
   in a disposable user session or virtual machine; test database changes against
   a disposable database. Never attach production state to CI.
3. Review changed screenshots and every frame of animations. Use a demo account,
   invented notes and a synthetic media library. Strip metadata, window titles,
   credentials, personal paths and identifying network details.
4. Update the product version and changelog together. Package metadata must agree
   with the application version. Submit those changes as a pull request.
5. Create an immutable semantic-version tag on the checked release commit:

   ```bash
   git fetch origin main --tags
   git switch main
   git merge --ff-only origin/main
   git tag vX.Y.Z
   git push origin refs/tags/vX.Y.Z
   ```

6. Create a GitHub Release with the changelog entry, build artifacts and
   `SHA256SUMS.txt`. Build artifacts from that exact public tag; do not upload
   local configuration, data volumes, debug logs or a private checkout's archive.
7. Verify download checksums and installation on a clean supported system.
   Update the website's guides and media when visible behavior changes.

## Deployment and recovery

Application source and public build checks live here. Signing keys, registry
credentials, production configuration and deployment approvals live in the
operator's secret manager and private deployment system. CI artifacts are
candidates until their release and installation checks pass.

Record the running version and image digest before an upgrade. Back up state,
rehearse its restore, and preserve the previous artifact. Database migrations
can make a binary-only downgrade unsafe; recover the matching backup when a
migration is not reversible. Never mutate an existing release tag.
