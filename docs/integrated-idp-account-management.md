# Controlling Who Can Log In: the Integrated Identity Provider and the Account Allowlist

**Audience:** Administrators who operate an on-premises Ariadne Engine installation and
decide which people may use it.

The Ariadne Engine ships with its own built-in login system — the **integrated identity
provider** (integrated IdP). With it, your users log in with a username and password that
are stored locally on your hardware; no external Ariadne account is needed.

On top of that login system you can switch on a **restricted mode with a fixed account
list**. In restricted mode, only the accounts you explicitly list in a JSON file may use
the installation. Nobody else can register, log in, or make their existing sessions work
anymore. This is the building block for controlled customer and team deployments:
you — the administrator — decide who gets an account, who loses it, and who gets it back.

This document explains everything an administrator needs:

- [The two modes at a glance](#1-the-two-modes-at-a-glance)
- [How restricted mode works, in plain language](#2-how-restricted-mode-works-in-plain-language)
- [Getting started: your first fixed user list](#3-getting-started-your-first-fixed-user-list)
- [The account file, field by field](#4-the-account-file-field-by-field)
- [Everyday administration: add, block, and re-enable users](#5-everyday-administration-add-block-and-re-enable-users)
- [What happens to passwords and recovery keys](#6-what-happens-to-passwords-and-recovery-keys)
- [What happens to a blocked user, exactly](#7-what-happens-to-a-blocked-user-exactly)
- [Locking down an existing open installation](#8-locking-down-an-existing-open-installation)
- [Going back to open registration](#9-going-back-to-open-registration)
- [Protecting the account file](#10-protecting-the-account-file)
- [Verifying that the engine applied your changes](#11-verifying-that-the-engine-applied-your-changes)
- [Troubleshooting](#12-troubleshooting)
- [Appendix: environment variables and API endpoints](#appendix-environment-variables-and-api-endpoints)

---

## 1. The two modes at a glance

The integrated IdP always has two behaviors that you control with two environment
variables:

| Environment variable | Default | Meaning |
|---|---|---|
| `AAA_IDENTITY_SOURCE` | `ariadne-anyverse` | Which login system the engine uses. Set it to `integrated-idp` to use the built-in local login instead of Ariadne's cloud account system. |
| `AAA_INTEGRATED_IDP_ALLOW_REGISTRATION` | `1` (open) | `1`/`true`: anyone may register an account through the app. `0`/`false`: registration is closed, and the account allowlist below is enforced. |
| `AAA_INTEGRATED_IDP_ACCOUNTS_FILE` | `$AAA_STORAGE_BASE_DIR/integrated_idp_accounts.json` | Where your JSON account list lives. Only evaluated in restricted mode. |

That gives you two operating modes:

| | **Open mode** (default) | **Restricted mode** (fixed user list) |
|---|---|---|
| Who can register? | Anyone, through the app's sign-up screen. | Nobody. Registration is switched off. |
| Who can log in? | Everyone with an account on this installation. | Only the accounts on your list. |
| Accounts not on the list | N/A. | Kept with all their data, but **blocked**: they cannot log in, and existing sessions and API keys stop working. |
| Account file | Ignored entirely (the engine does not read it). | The single source of truth for who is allowed. |

> **Note for local deployments:** the engine-wide default of `AAA_IDENTITY_SOURCE` is
> `ariadne-anyverse` (cloud account login). For a local or on-premises installation you
> should set it to `integrated-idp` — the shipped `docker-compose-example.yml` already
> does this for you. Restricted mode only makes sense with the integrated IdP; with the
> cloud provider, the account file is never read.

Both settings are read from the environment at startup (a `.env` file for Docker
deployments, the service environment for the native binary). There is no runtime toggle
in the UI: to switch modes or apply list changes, restart the engine (see
[Verification](#11-verifying-that-the-engine-applied-your-changes)).

---

## 2. How restricted mode works, in plain language

You maintain one JSON file. It contains every account that may use the installation.
Whenever the engine starts, it reads that file and synchronizes it with its database:

1. **Accounts on the list that do not exist yet** are created automatically, with the
   username and initial password you specified. The engine generates an internal
   technical identifier (`identity_key`) and a personal recovery key for each new
   account, and writes both back into your file.
2. **Accounts on the list that already exist** are simply (re-)activated. Nothing about
   their password or recovery state is touched.
3. **Every account that is not on the list** is blocked. Its data stays intact, but the
   account can no longer log in, refresh its session, or use API keys — the engine
   treats all of those as inactive.

Two properties of this design are worth understanding:

- **The file is the truth, the database only stores the result.** After the engine
  starts, login decisions are made from the database, not from the file. The file is
  never watched while the engine runs — your changes take effect on the **next
  restart**. Plan a controlled restart whenever you add or block a user.
- **Blocking is reversible and non-destructive.** Removing an account from the file
  does not delete the user, their chats, their workspace, their password, or their API
  keys. It only switches the account off. Put the entry back and restart, and the same
  account — with its old password — is usable again.

---

## 3. Getting started: your first fixed user list

### Step 1 — Create the account file

Create the JSON file **before** the first start in restricted mode. You can do this
even on an installation that has been running with open registration so far (see
[Section 8](#8-locking-down-an-existing-open-installation)). An empty list is valid —
but note that after switching, nobody can log in anymore.

A minimal file with one administrative starter account:

```json
{
  "version": 1,
  "accounts": [
    {
      "username": "admin",
      "initial_password": "ChangeMeNow0First"
    }
  ]
}
```

### Step 2 — Configure the environment

Docker deployment (add these lines to your `docker-compose.yml`, matching the shipped
example):

```yaml
    environment:
      - AAA_IDENTITY_SOURCE=integrated-idp
      - AAA_INTEGRATED_IDP_ALLOW_REGISTRATION=0
      - AAA_INTEGRATED_IDP_ACCOUNTS_FILE=/app/aaa-bundle/databases/integrated_idp_accounts.json
    volumes:
      - ./databases:/app/aaa-bundle/databases   # must be persistent and writable by the container
```

Native binary: set the same three variables in the deployment's environment (`.env`).
If you leave `AAA_INTEGRATED_IDP_ACCOUNTS_FILE` unset or empty, the engine uses
`$AAA_STORAGE_BASE_DIR/integrated_idp_accounts.json`.

The engine needs **write access to the file and its parent directory** (it writes
generated keys back atomically via a temporary file in the same directory).

### Step 3 — Restart the engine

Start or restart the engine with the new configuration.

### Step 4 — Check the file after the first restricted start

The engine enriched your entry:

```json
{
  "version": 1,
  "accounts": [
    {
      "username": "admin",
      "initial_password": "ChangeMeNow0First",
      "identity_key": "local-9f3c...",
      "recovery_key": "kQ2v...=="
    }
  ]
}
```

**Keep the generated values exactly as written.** You will need the `identity_key`
every time you edit or restore this entry later (see
[the account file chapter](#4-the-account-file-field-by-field)).

### Step 5 — Hand over credentials

Give the user their username, the initial password, and the recovery key — over a
secure channel, and preferably in separate messages. Ask them to change the initial
password right after their first login. The app reminds users to back up their
recovery key periodically (every 14 days) and they can regenerate it at any time.

---

## 4. The account file, field by field

The file is a JSON object with a version number and a list of accounts:

```json
{
  "version": 1,
  "accounts": [
    {
      "username": "alice",
      "initial_password": "FirstSecurePass01"
    },
    {
      "username": "bob",
      "initial_password": "AnotherStrongPass02",
      "identity_key": "local-1a2b...",
      "recovery_key": "mN8p...=="
    }
  ]
}
```

| Field | Required | Who sets it | Rules and meaning |
|---|---|---|---|
| `version` | Yes | You | Must be the number `1`. |
| `accounts` | Yes | You | List of the accounts allowed to use the installation. `[]` (empty list) is valid — in restricted mode with an empty list, nobody can log in. |
| `username` | Yes, per account | You | Non-empty string, no leading or trailing whitespace. Each username may appear only once in the file. |
| `initial_password` | Yes, per account | You | Non-empty string, no surrounding whitespace. Minimum 12 characters with at least one uppercase letter, one lowercase letter, and one digit (details below). Only used when the account is **newly created** — see [Section 6](#6-what-happens-to-passwords-and-recovery-keys). |
| `identity_key` | No | **The engine** | The stable technical identifier of the account. The engine generates it for new accounts and fills it in for existing ones. **Never set it yourself.** If it is present, it must match the account's stored key and must be unique in the file. |
| `recovery_key` | No | **The engine** | The personal recovery key of a **newly created** account. The engine generates and writes it once. **Never set it yourself.** For existing accounts the engine never generates, replaces, or adds a recovery key. |

### Password rules

The engine validates every `initial_password` (and every user-chosen password,
including passwords entered through the app or recovery flows) against the same rules:

- at least **12 characters**,
- at least one **uppercase** letter (A–Z),
- at least one **lowercase** letter (a–z),
- at least one **digit** (0–9).

Special characters are allowed but not required. Umlauts and other Unicode characters
are allowed but do **not** count as the required upper or lower letter. No leading or
trailing whitespace. There is no maximum length.

| Password | Valid | Why |
|---|---|---|
| `KorrektesStartpasswort1` | Yes | Length and all three character classes present. |
| `ZuKurz1A` | No | Shorter than 12 characters. |
| `nurkleinbuchstaben1` | No | No uppercase letter. |
| `NUR_GROSSBUCHSTABEN1` | No | No lowercase letter. |
| `KeineZifferVorhanden` | No | No digit. |
| ` PasswortMitRand1A` | No | Leading whitespace in the JSON file. |

### What a malformed file does

An invalid format, a duplicate username, a duplicate or mismatching `identity_key`, or
a password that violates the rules **fails the synchronization**. In restricted mode the
engine then **does not start** — you will see the error in the logs. This is deliberate:
a restricted deployment must never silently fall back to open registration.

---

## 5. Everyday administration: add, block, and re-enable users

All changes follow the same pattern:

1. Edit the JSON file (keep the engine-generated `identity_key`/`recovery_key` values
   of every entry you keep — do not delete or retype them).
2. Back up the file before editing (a protected copy).
3. Restart the engine in a controlled way.
4. Check the log message and, for critical changes, test that the expected user can
   (or cannot) log in.

### 5.1 Add a user

- New user: add an entry with just `username` and `initial_password`. The engine
  creates the account, generates the `identity_key` and `recovery_key`, and writes both
  into the file.
- Existing user who was previously blocked: see [5.3](#53-re-enable-a-blocked-user) —
  re-adding the original entry (with its `identity_key`) re-activates the account.

Hand over the credentials securely and ask the user to change the initial password.

### 5.2 Block a user (remove their access)

1. Remove the user's **entire entry** from `accounts`. Do not change any other entry.
2. Restart the engine.
3. Verify: the log shows the successful synchronization, and a login attempt with the
   user's credentials is rejected.

What exactly happens to the blocked account — login, sessions, API keys, data — is
spelled out in [Section 7](#7-what-happens-to-a-blocked-user-exactly).

> **Immediate blocking without a restart:** the file is not watched, so the block
> becomes active only after the restart. If you must cut access instantly (for example,
> a departing employee), also stop the affected access through your operational or
> network controls until the engine has restarted.

### 5.3 Re-enable a blocked user

1. Put the user's **original entry** back into `accounts`. Keep the `identity_key` the
   engine wrote earlier. (You do not need the `recovery_key` for re-activation — but if
   it is in your backup, keep it in the entry as well.)
2. Restart the engine.

The account is re-activated with its last valid password. Nothing is reset.

### 5.4 What an `initial_password` in the file never does

The `initial_password` is a **provisioning value**, not a password reset mechanism:

- For an existing account it is only **format-checked**, never compared with the
  stored password, and never applied. Changing the `initial_password` in the file has
  **no effect** on an existing account's actual password.
- If a user changed their password through the app, later engine starts never
  overwrite it with the value from the file.

If you need to reset someone's password, that is the user's job through the app
(password change) or through their recovery key — not yours through the file.

---

## 6. What happens to passwords and recovery keys

### What is set automatically (for a new account)

When the engine creates an account from your entry, it automatically sets:

| What | How |
|---|---|
| Password | The bcrypt hash of your `initial_password`. The user must log in with that initial password and is expected to change it. |
| `identity_key` | A random local identifier of the form `local-…` — the stable technical key of the account, written back into your file. |
| `recovery_key` | A random 256-bit recovery key (base64), shown to you in the file and to the user in the app. Written back into your file. |
| Role scope | The default `free` scope (no special permissions beyond regular use). |
| Recovery-reminder state | The app reminds the user to keep the recovery key safe, and repeats the reminder at least every 14 days until the user confirms it. |

### Recovery keys in detail

- **New accounts:** the engine generates one recovery key at creation time and stores
  its hash in the database (the plaintext exists only in your account file and in the
  user's possession).
- **Existing accounts:** the engine **never** generates, replaces, or rewrites a
  recovery key, even if the field is missing from the file. Entering a recovery key
  into the file later does **not** replace the stored one either.
- **Users can regenerate their own key** at any time from the app. A regenerated key
  invalidates the previous one — and is deliberately **not** written back into the
  account file. From that moment on, the file no longer contains a valid recovery key
  for that user; that is expected and does not affect login.
- **Recovering an account with a key** replaces both the password and the recovery key
  and logs out all other sessions of that user.
- **Blocked accounts** cannot use their recovery key (see [Section 7](#7-what-happens-to-a-blocked-user-exactly)).

### Practical consequences for the administrator

- Treat the `recovery_key` entries in your file as real secrets: whoever holds them can
  take over that account.
- You do **not** need the recovery keys to administer the allowlist. They matter only
  for handing accounts to users and for recovery situations.
- If you lost the file but the engine is running, the `identity_key` values still exist
  in the engine's database — but you should never have to reconstruct the file by
  inspecting the database. Use your protected backups.

---

## 7. What happens to a blocked user, exactly

When an account is not in `accounts` after a synchronization (restricted mode), the
engine marks it with an internal "disabled" flag. All of the following apply until the
account is listed again:

| What the user tries | Result |
|---|---|
| Log in with username and password | **Rejected** (HTTP 403 "User account is disabled"). The user cannot see which of the two causes it — wrong credentials and disabled accounts are indistinguishable to the user. |
| Use an existing refresh token (e.g. an app that is still logged in) | **Rejected.** The session does not extend itself; the app eventually asks for login. |
| Call an engine API with an existing access token (JWT) | Treated as **inactive**: token introspection reports the token as not active, and engine endpoints reject the call. (Note: the JWT is short-lived — by default it expires after one hour — so this mostly protects the window until expiry and the API-key case below.) |
| Call an engine API with an existing API key | Treated as **inactive**: API-key introspection reports `active: false`, so the key cannot authenticate anything. |
| Recover the account with the recovery key | **Rejected** (HTTP 403). |
| Their data — chats, contexts, workspace files, knowledge graph, automation policies | **Untouched.** Nothing is deleted or modified. |
| Their password, stored API keys, refresh tokens | **Kept.** Re-adding the account restores full functionality with the old password. |

In open mode the "disabled" flag is simply not treated as a block: all accounts are
usable again and self-registration is available.

---

## 8. Locking down an existing open installation

You can switch an installation from open to restricted registration at any time,
including one where users already registered themselves:

1. Create the account file **before** the first restricted start, containing **every
   account that should keep access** — including all the accounts your users already
   created.
2. For each of those existing accounts you must provide `username` and an
   `initial_password`. The password value is only format-checked for existing accounts;
   it is not compared with or applied to the stored password. You can therefore enter
   any rule-compliant generated value (e.g. `LockedDown01Admin`). Still treat it as a
   secret, because the file contains plaintext values.
3. Leave `identity_key` and `recovery_key` out for existing accounts. The engine fills
   in the `identity_key` automatically on the first restricted start. It does **not**
   generate or add a `recovery_key` for existing accounts — the recovery state stored
   in the database stays exactly as it was.
4. Set `AAA_INTEGRATED_IDP_ALLOW_REGISTRATION=0` and restart the engine.

After that start:

- The accounts you listed are active (and, for the first time, their `identity_key` is
  recorded in the file — keep it from then on).
- **Every other existing account on the installation is blocked** (see
  [Section 7](#7-what-happens-to-a-blocked-user-exactly)).
- Registration is closed.

If you are unsure which accounts exist, make the list generously on the first
restricted start (you can always tighten it with the next restart) and review who is
actually using the installation afterwards.

---

## 9. Going back to open registration

Set `AAA_INTEGRATED_IDP_ALLOW_REGISTRATION=1` (or `true`) and restart the engine:

- The account file is **ignored entirely** — even if it is missing.
- All previously blocked accounts become usable again with their stored passwords.
- The sign-up screen accepts new self-registered accounts.

Think carefully before doing this: it removes the access control for every account on
the installation, not only for the ones you had blocked.

---

## 10. Protecting the account file

The file contains **plaintext secrets**:

- `initial_password` works as a login credential until the user changes it.
- `recovery_key` allows taking over the account entirely.

Handle the file like a credentials vault:

- Store it only in a persistent, administratively protected directory. In Docker
  deployments that is the `./databases` bind mount of the engine container.
- Restrict read access to the engine's process user and the responsible administrators,
  e.g. file permissions `0600` on Linux.
- **Do not** commit it to Git, put it into unprotected backups, support attachments, or
  container images.
- Keep a protected copy before every change, including the engine-generated
  `identity_key` and `recovery_key` values.
- Hand over initial passwords and recovery keys separately and over secure channels.
  Ask users to change their initial password promptly.
- Secret-management systems are only suitable if they present the file persistently and
  writable to the engine.

---

## 11. Verifying that the engine applied your changes

On a successful synchronization, the engine writes a log message of the form:

```
Synchronized restricted integrated-IdP account allow-list with N configured accounts.
```

where `N` is the number of entries in your file. That message confirms the file was
read and applied. For a critical change (blocking a user, switching to restricted
mode), additionally test that the expected account can — or cannot — log in.

The engine re-runs the synchronization at startup and in every worker lifecycle,
serialized by a file lock, so multi-process deployments converge on the same state.
The write-back into the file is atomic (temporary file plus rename).

If the engine fails to start in restricted mode, the cause is almost always the file
itself (missing, unreadable, malformed) — see [Troubleshooting](#12-troubleshooting).

---

## 12. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Engine does not start: the accounts file is missing or unreadable | Restricted mode is active but the file does not exist at the configured path (or the Docker volume is not mounted / not writable). | Create a valid file at the exact path; check the bind mount and container user. |
| Engine does not start: file format error | Root is not an object, `version` is not `1`, `accounts` is not a list, or an entry violates the field rules. | Validate the JSON against the schema in [Section 4](#4-the-account-file-field-by-field). |
| Engine does not start: password error | An `initial_password` violates the length or character-class rules. | Use at least 12 characters with upper, lower, and a digit. |
| Engine does not start: `identity_key` error | The key in the file belongs to a different user than the entry claims, or two entries share one key. | Restore the values the engine originally wrote; never copy keys between accounts. |
| Engine cannot write back to the file | The process user cannot write the file **or its parent directory**. | Fix ownership and permissions (both the file and the directory must be writable). |
| A user is still active (or still blocked) after you edited the file | The engine has not been restarted since the edit. The file is not watched. | Restart the engine and check the synchronization log message. |
| Registration is still possible | `AAA_INTEGRATED_IDP_ALLOW_REGISTRATION` is still `1`/`true`, or the running process still has the old environment. | Set it to `0` and restart. |
| A user who was re-activated cannot log in with the old password | Usually not the allowlist: the password change flow or a recovery reset happened in between. | The allowlist re-activation never changes passwords — if the password changed, a password change or recovery was performed by the user or with their recovery key. |

---

## Appendix: environment variables and API endpoints

### Environment variables

| Variable | Default | Notes |
|---|---|---|
| `AAA_IDENTITY_SOURCE` | `ariadne-anyverse` | `integrated-idp` activates the built-in login. The account allowlist only applies to the integrated IdP. |
| `AAA_INTEGRATED_IDP_ALLOW_REGISTRATION` | `1` | Accepts `1`/`true` (open) or `0`/`false` (restricted). Case-insensitive, surrounding whitespace ignored. Any other value fails the evaluation. |
| `AAA_INTEGRATED_IDP_ACCOUNTS_FILE` | `$AAA_STORAGE_BASE_DIR/integrated_idp_accounts.json` | Absolute or user-relative path. Required to be readable and writable (with its parent directory) in restricted mode. Ignored in open mode. |

### Related engine behavior

- Access tokens (JWT) are valid for one hour by default; refresh tokens for 30 days.
  Both are rejected for blocked accounts. These lifetimes are configurable
  (`LOCAL_IDP_ACCESS_TOKEN_EXPIRES`, `LOCAL_IDP_REFRESH_TOKEN_DAYS`) but rarely need
  changing for allowlist operation.
- The sign-up endpoint `POST /integrated_idp/register` returns HTTP 403 in restricted
  mode.
- Blocked accounts are detected at token introspection, which is the mechanism the
  engine's own API gateway and the web app use — so the block applies to every client
  of the installation, not just the app.

### API endpoints of the integrated IdP (reference)

| Endpoint | Purpose |
|---|---|
| `POST /integrated_idp/register` | Self-registration (open mode only). |
| `POST /integrated_idp/oauth2/token` | Login with username and password. |
| `GET /integrated_idp/refresh_token` | Exchange the refresh token for a new token pair. |
| `DELETE /integrated_idp/logout` | Invalidate the session's refresh token. |
| `POST /integrated_idp/change_password` | Change the password (user, with session). |
| `POST /integrated_idp/regenerate_recovery_key` | Regenerate the personal recovery key (user, with session). |
| `POST /integrated_idp/recover_account_by_recovery_key` | Reset password and recovery key using the recovery key. |
| `POST /integrated_idp/confirm_recovery_key_reminder` | Dismiss the recovery-key backup reminder. |
| `POST /integrated_idp/oauth2/introspect` | Check whether a token is currently active. |
| `POST /integrated_idp/api_keys` / `GET /integrated_idp/api_keys/user` / `DELETE /integrated_idp/api_keys/{id}` / `POST /integrated_idp/api_key/introspect` | Manage and check API keys. |
