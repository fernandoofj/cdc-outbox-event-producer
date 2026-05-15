# cdc-outbox-event-producer — engineering rules for Claude

This file is loaded automatically into every Claude session that runs
inside this repository. The rules below are project-wide; break them
only with explicit user approval *for that turn*.

The companion persona for code review lives at
[`.claude/agents/tech-lead.md`](.claude/agents/tech-lead.md). The
Tech Lead is **mandatory** before every non-trivial commit — see the
"Tech Lead gate" section below.

## Always-on engineering practices

  * **Idiomatic Kotlin.** Data classes for value objects, sealed
    classes for closed hierarchies, expression bodies where they read
    better, no `lateinit var` for things that could be constructor
    parameters, no Java-style getters where a `val` works.
  * **SOLID, no shortcuts.** Especially Single Responsibility (no
    God-classes) and Open/Closed (extend via polymorphism, not by
    growing a `when (enum)` switch).
  * **Hexagonal first.** Domain types live in `core/`. Ports are
    pure-Kotlin interfaces with no framework or driver imports.
    Adapters depend on ports, never the reverse. Spring annotations
    live ONLY in `spring-boot-starter/`. The legacy single-module tree
    (`br.com.fltech.cdc.outbox.publisher.**`) is tolerated until the
    modular split lands, but no new code may deepen coupling between
    domain and infrastructure.
  * **Code identifiers + comments in en-US.** User-facing strings
    (log messages, error bodies) in pt-BR when the runtime context is
    Brazilian; en-US otherwise. Pick one per module and stay
    consistent.
  * **Document every public class/method** with short KDoc — focus on
    *why*, not *what*. Comments that restate the code rot.
  * **Errors raise domain exceptions** from the project's exception
    hierarchy (to be introduced in Wave 1). Never throw raw
    `RuntimeException`; never use `ResponseStatusException`-like
    framework exceptions inside domain code. Lesson imported from the
    user's other project (PR #9 there).
  * **No silent error paths.** If a defensive branch skips an
    operation (publisher down, payload malformed, slot conflict), it
    MUST emit a log line at WARN or ERROR AND a Micrometer counter
    once the observability module is in place. The happy path
    returns "everything OK"; everything else surfaces.
  * **No `@Suppress` without a one-line justification** immediately
    above it. Detekt suppressions in particular hide real findings.
  * **Bean Validation on every DTO that crosses an HTTP boundary**
    once the Spring Boot starter lands. Front + back validation
    redundancy is desirable.
  * **No `Thread.sleep` in hot loops without re-asserting the
    interrupt** — `catch (e: InterruptedException) {
    Thread.currentThread().interrupt(); ... }`.

## Delivery guarantees (the core contract)

This project must deliver **at-least-once** semantics. After every
change, the following invariants must hold:

  1. The replication-slot LSN advances ONLY after the message at that
     LSN has been successfully published, explicitly discarded by a
     documented rule, or dead-lettered.
  2. The LSN advanced is the LSN of the **message we just acted on**,
     not `PGReplicationStream.lastReceiveLSN()`, which drifts ahead
     of in-flight messages. The original code violated this — see the
     historical bug fix in Wave 1.
  3. On publisher failure, the loop backs off (exponential + jitter)
     and retries the same message. After N attempts (configurable),
     the message is dead-lettered AND the LSN is advanced; otherwise
     the slot grows forever.
  4. On graceful shutdown, the loop finishes its current message,
     advances the LSN, and closes connections. No partial state.

Violations are **BLOCKER**-severity in code review.

## Tech Lead gate

Before committing any change that is more than a typo or a doc edit,
invoke the Tech Lead persona:

```
Task → subagent_type: tech-lead
prompt: "Review the uncommitted diff on branch <name> against the
user's last instruction in this conversation. Apply the standard
checklist."
```

The Tech Lead reads the diff and the user's most recent ask and
returns a numbered list of findings with severity (BLOCKER / MAJOR
/ MINOR / NIT) and a verdict (PASS / FAIL).

  * **PASS, zero MAJOR+** — proceed to commit.
  * **PASS, MINOR / NIT only** — proceed to commit; address findings
    in a follow-up commit on the same branch when convenient.
  * **FAIL** — fix the BLOCKER / MAJOR findings, re-invoke the Tech
    Lead, and only commit once it returns PASS. Do NOT downgrade your
    own findings to ship faster.

The Tech Lead persona has read-only tools. It does not commit, push,
or fix code itself.

## Delivery workflow (single-developer, no PR by default)

This is a personal/private project. Skip `gh pr create` unless the
user explicitly asks for one (e.g. before sharing with a collaborator
or running an external review tool).

Per round:

  1. Branch from latest `main` (`feat/<descriptive-name>`).
  2. Implement + commit (small, focused commits — one logical change
     per commit; refactor and feature in separate commits).
  3. **Invoke the Tech Lead.** Address BLOCKER / MAJOR findings.
  4. `./gradlew test` — must be green. Unit tests run without
     Docker; integration (`*IT.kt`) requires `./gradlew
     startDockerCompose` first.
  5. Merge into `main` locally (`git merge --no-ff` for traceability).
     Resolve conflicts inline.
  6. `git push origin main`. The network blocks `api.github.com` →
     `4.228.31.149` from this machine; if `gh` operations fail with
     `i/o timeout`, push via HTTPS uses an alternate IP and works.
     See `docs/network-notes.md` (to be added).
  7. Delete the feature branch (local + origin if pushed).
  8. Update `README.md` § Roadmap to reflect the new state. The README
     is the single source of truth for what's done vs planned —
     never let it drift.

## Project conventions

  * **Package root:** `br.com.fltech.cdc.outbox.publisher.**`. The
    legacy `shop.inventa.pg2sns4k.**` tree was migrated in commit
    `25dcf78`; no files under the old root should ever reappear.
  * **Maven `group`:** `br.com.fltech.cdc.outbox`.
  * **External identifiers** (event ids, domain ids in event
    envelopes) are ULIDs or UUIDv7. Internal database PKs are never
    exposed.
  * **Replication slot names** are lowercase snake_case, ≤ 63 chars,
    prefixed with the deploying service (e.g. `orders_outbox_slot`).
  * **Migrations**, when introduced for the MySQL outbox-table
    adapter, are Flyway `Vn__<snake_case>.sql`. Idempotent where
    possible; never edit a published migration.
  * **Background work** uses the JVM's `ScheduledExecutorService` or
    Quartz (in the Spring starter), never `Thread { ... }.start()`.
  * **Configuration** lives in `application.yml` under the
    `cdc.outbox.*` prefix, surfaced by `@ConfigurationProperties`
    POJOs. Production-mutable settings go through a
    `SystemSettingsService` once that pattern lands.

## Git and persistence

  * **Never `git reset --hard`, `git push --force`, `git branch -D`**
    without explicit user consent for the specific command. There is
    almost always a safer alternative (`git revert`,
    `git branch -f` on an unpushed branch, etc.).
  * **Never amend a merge commit.** Add a follow-up fix commit
    instead.
  * **Don't skip hooks** (`--no-verify`, `--no-gpg-sign`). Fix the
    underlying issue.
  * **Don't commit `.env*`, `*.key`, `*.pem`, raw certificates, or
    any secret.** Vault them; environment variables only.

## Status reporting

  * **Always present pending items as a Markdown table** with columns
    `#`, `Item`, `Área`, `Complexidade`
    (baixa = horas, média = 1–3 dias, alta = > 3 dias), and at least
    one of `Bloqueia?` / `Prioridade`. Bullets dilute trade-offs;
    tables make the comparison explicit. The table must appear at the
    end of every multi-step round, and any time the user asks "o que
    falta?", "status", "próximo passo?", or for a list of options.
  * **Mirror the table in `README.md` § Roadmap** as part of every
    round close-out.

## When in doubt

Ask the user with **two or three numbered options** and the
trade-offs. Don't paper over an unknown.
