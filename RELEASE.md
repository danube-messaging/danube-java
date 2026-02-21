# Release guide (Maven Central)

This project is configured for Sonatype OSSRH publishing under the `release` Maven profile.

## 1) Prerequisites

1. Sonatype OSSRH account with publish access for `com.danubemessaging`.
2. GPG key pair available locally.
3. `~/.m2/settings.xml` configured with server credentials:

```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>${env.OSSRH_USERNAME}</username>
      <password>${env.OSSRH_PASSWORD}</password>
    </server>
  </servers>
</settings>
```

4. Environment variables set for CI/local release:

```bash
export OSSRH_USERNAME=...
export OSSRH_PASSWORD=...
export GPG_TTY=$(tty)
```

If using passphrase-based signing in CI, also pass:

```bash
-Dgpg.passphrase=...
```

## 2) Local verification (no deploy)

```bash
mvn -Prelease -DskipTests verify
```

This validates source jar, javadoc jar, and artifact signing configuration.

## 3) Publish snapshot

```bash
mvn -Prelease -DskipTests deploy
```

For `-SNAPSHOT` versions this publishes to:

- `https://s01.oss.sonatype.org/content/repositories/snapshots`

## 4) Publish release version

1. Update root `<version>` from `x.y.z-SNAPSHOT` to `x.y.z`.
2. Commit and tag the release.
3. Run:

```bash
mvn -Prelease -DskipTests deploy
```

Release artifacts are staged and closed/released automatically via `nexus-staging-maven-plugin`.

## 5) After release

1. Bump to next development version (e.g. `x.y.(z+1)-SNAPSHOT`).
2. Push commit and tag.
