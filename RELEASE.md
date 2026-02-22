# Release guide (Maven Central)

This project publishes to Maven Central via the **Sonatype Central Portal** (`central.sonatype.com`).
The `release` Maven profile handles sources jar, javadoc jar, GPG signing, and upload automatically.

---

## 1) One-time setup: Central Portal account

> Skip this section if the account already exists.

1. Go to **https://central.sonatype.com** and sign up (GitHub SSO works).
2. Click **Namespaces** → **Add Namespace**.
3. Enter `com.danube-messaging`.
4. Sonatype will show a **DNS TXT verification token**. Add it to the `danube-messaging.com` DNS:
   ```
   TXT  @  sonatype-central-verification=<token>
   ```
5. Click **Verify Namespace**. Once verified the namespace is permanently linked to your account.

---

## 2) One-time setup: Portal user token (credentials)

1. In the Central Portal: **Account** → **Generate User Token**.
2. Copy the `username` and `password` values shown.
3. Add them to `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>YOUR_TOKEN_USERNAME</username>
      <password>YOUR_TOKEN_PASSWORD</password>
    </server>
  </servers>
</settings>
```

> **Never** commit these credentials. Use environment variable substitution if needed:
> ```xml
> <username>${env.CENTRAL_TOKEN_USERNAME}</username>
> <password>${env.CENTRAL_TOKEN_PASSWORD}</password>
> ```

---

## 3) One-time setup: GPG signing key

```bash
# Generate a key (if you don't have one)
gpg --gen-key

# List keys and note the key ID
gpg --list-secret-keys --keyid-format=long

# Publish the public key to a keyserver
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
```

Set the passphrase in your environment for headless signing:

```bash
export GPG_TTY=$(tty)
```

---

## 4) Local verification (no deploy)

Before publishing, verify that all artifacts build and sign correctly:

```bash
mvn -Prelease -DskipTests verify
```

This produces and signs:
- `danube-client-proto-0.2.0.jar` + sources + javadoc
- `danube-client-0.2.0.jar` + sources + javadoc

---

## 5) Publish release version

1. Confirm root `pom.xml` version is `0.2.0` (no `-SNAPSHOT` suffix).
2. Commit and tag the release:

```bash
git add -A
git commit -m "release: v0.2.0"
git tag v0.2.0
git push origin main --tags
```

3. Deploy to Maven Central:

```bash
mvn -Prelease -DskipTests deploy
```

Both `danube-client-proto` and `danube-client` are published in one command (multi-module build).
The `central-publishing-maven-plugin` uploads, validates, and releases automatically
(`autoPublish=true`, `waitUntil=published`). Artifacts are typically searchable within 30 minutes.

If GPG passphrase entry is required during deployment:

```bash
mvn -Prelease -DskipTests -Dgpg.passphrase=YOUR_PASSPHRASE deploy
```

---

## 6) After release

1. Bump to the next development version:

```bash
# In pom.xml, danube-client/pom.xml, danube-client-proto/pom.xml
# Change version: 0.2.0 → 0.3.0-SNAPSHOT
```

2. Commit and push:

```bash
git add -A
git commit -m "chore: bump to 0.3.0-SNAPSHOT"
git push origin main
```

---

## CI/CD (GitHub Actions)

To automate publishing from CI, set these repository secrets:

| Secret | Value |
|--------|-------|
| `CENTRAL_TOKEN_USERNAME` | Portal user token username |
| `CENTRAL_TOKEN_PASSWORD` | Portal user token password |
| `GPG_PRIVATE_KEY` | Output of `gpg --armor --export-secret-keys <KEY_ID>` |
| `GPG_PASSPHRASE` | GPG key passphrase |

Then in your workflow:

```yaml
- name: Import GPG key
  run: echo "${{ secrets.GPG_PRIVATE_KEY }}" | gpg --batch --import

- name: Publish to Maven Central
  run: mvn -Prelease -DskipTests -Dgpg.passphrase=${{ secrets.GPG_PASSPHRASE }} deploy
  env:
    CENTRAL_TOKEN_USERNAME: ${{ secrets.CENTRAL_TOKEN_USERNAME }}
    CENTRAL_TOKEN_PASSWORD: ${{ secrets.CENTRAL_TOKEN_PASSWORD }}
```
