# Arcane fork (viogus)

This repository is a fork of [getarcaneapp/arcane](https://github.com/getarcaneapp/arcane)
with a few additions. Everything else — setup, Helm/Compose examples, API docs — is
upstream's and lives at [getarcane.app](https://getarcane.app).

## Branches

| Branch | Tracks          | Notes                                                              |
| ------ | --------------- | ------------------------------------------------------------------ |
| `main` | upstream 2.17.x | Maintenance line; still the branch the 2.x images were built from. |
| `v3`   | upstream 3.1.x  | Current line. Carries the fork features below.                     |

The fork never merges `v3` into `main`, so a 2.x deployment is not forced onto v3 by
accident.

## Images

| Image                         | Tags                       |
| ----------------------------- | -------------------------- |
| `ghcr.io/viogus/arcane`       | `latest`, `3.1.0-cdf.1`, … |
| `ghcr.io/viogus/arcane-agent` | `latest`, `3.1.0-cdf.1`, … |

- `+` is not valid in a Docker tag, so a build labelled `3.1.0+cdf.1` is published as
  `3.1.0-cdf.1`; the version embedded in the binary keeps the `+`.
- Images are the **lean** variant: the same static binary on `scratch`, with the CA bundle
  and zoneinfo (`timetzdata`) inside the image. They are about 34 MB smaller than the
  distroless build.
- Images are multi-arch (`linux/amd64`, `linux/arm64`) and built by
  `.github/workflows/publish.yml` on GitHub-hosted runners:

  ```bash
  gh workflow run publish.yml --ref v3 -f version=3.1.0+cdf.1 -f move_latest=true
  ```

  `--ref` matters: without it the workflow builds the default branch.

## Deploying

```yaml
services:
  arcane:
    image: ghcr.io/viogus/arcane:latest
    container_name: arcane
    ports:
      - '3552:3552'
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - arcane-data:/app/data
      - /host/path/to/projects:/host/path/to/projects
      - /host/path/to/builds:/builds
    environment:
      - ARCANE_ENCRYPTION_KEY=replace_me_with_a_random_32_char_value
      - ARCANE_PROJECTS_DIRECTORY=/host/path/to/projects
      - TZ=UTC
    restart: unless-stopped

volumes:
  arcane-data:
```

A ready-to-edit version of that file, including the agent side, is in
[`docker/examples/compose.fork.yaml`](docker/examples/compose.fork.yaml).

## Fork features

- **Docker host environments.** An environment can point straight at a Docker API
  endpoint (`tcp://192.168.2.33:2375`, `unix:///var/run/docker.sock`, …) instead of
  running an agent. The manager serves those environments in-process against that
  daemon: containers, projects, backups, events and scheduled jobs all run against it.
  Create one from _Environments → New environment → Docker host environment_.
- **Per-environment backup history.** Volume backup history and policies are scoped to the
  environment that owns the volume, so two daemons can each have an `app-data` volume.
- **Backup kill switch.** `ARCANE_BACKUPS_DISABLED=true` turns the Rustic engine off: no
  helper image pull, no helper container, no repository volume.
  See the generated config schema (`just docs config`) for the documented value.
- **Fork image publishing.** See the Images section above.

## Upgrading 2.x → 3.x

Upstream v3 is a breaking release; the parts that bite a deployment:

1. **Every environment variable gained the `ARCANE_` prefix**: `PORT` → `ARCANE_PORT`,
   `DATABASE_URL` → `ARCANE_DATABASE_URL`, `UPDATE_CHECK_DISABLED` →
   `ARCANE_UPDATE_CHECK_DISABLED`, `AGENT_MODE` → `ARCANE_AGENT`, … Variables without the
   prefix are ignored. `TZ`, `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` keep their names.
2. **Database migrations moved to Goose** with a v2 baseline, so an existing v2 database is
   upgraded in place on first start. Take a backup first: going back to 2.x needs the
   database restored, not just the old image.
3. **The agent was renamed** and enrollment replaced the shared registration flow:
   re-enroll agents from the manager rather than reusing old tokens.
4. Image tags are unchanged in shape (`3.1.0-cdf.1`), but `latest` now points at 3.x.

## Local development

Same as upstream: `just dev docker`, `just test backend`, `just format all`, `just lint all`.
The `just` recipes expect `golangci-lint`, `vp` (Vite+) and, for `format`/`lint`,
`just` itself.
