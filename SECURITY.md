# Security Policy

## Supported versions

This project is pre-1.0 and evolves quickly. Security fixes are only
provided for the latest published version on the `main` branch /
GitHub Packages. There is no long-term support branch at this stage.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security
vulnerabilities.

Instead, use GitHub's private vulnerability reporting for this repo
(**Security** tab → **Report a vulnerability**), or email
**fernandoofj@gmail.com** with:

  * A description of the vulnerability and its impact.
  * Steps to reproduce (a minimal reproduction repo or test case
    helps a lot).
  * The affected module(s) (e.g. `source-postgres`, `sink-aws`) and
    version.

You should get an acknowledgment within a few days. This is a
personal project maintained on a best-effort basis, so please be
patient — but security reports are prioritized over regular issues.

Please give a reasonable amount of time to address the issue before
any public disclosure.

## Scope notes

This library reads database credentials and broker credentials from
whatever configuration mechanism the host application provides
(`application.yml`, environment variables, etc.) — it does not store
or transmit credentials anywhere else. If you find a code path where
a secret (DB password, AWS credentials, broker credentials) could
leak into logs, metrics, or the Actuator endpoints
(`/actuator/cdcOutboxDlq`, `/actuator/cdcOutboxReplay`,
`/actuator/info`), that is treated as a security issue, not just a
bug.
