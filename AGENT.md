# AGENT.md

## Project Purpose

This project is called Latch. It implements, in Python/Flask, a modular batch command manager for running, monitoring, interrupting, and clearing sequential plugin executions configured by module.

The goal is to provide a reusable base for orchestrating local module-based batches, with a simple UI, real-time logs, persisted latest-run state, and safe per-module concurrency control.

This document must be used by AI tools and development agents as the project context source. Every new feature, business rule, architectural change, or relevant pattern must be reflected here. `AGENT.md` must be kept up to date with the code.

## Repository Operating Rules

- Before running any command that interacts with files outside the current project folder, for either reading or writing, ask the user for explicit permission.
- Use `rg`/`rg --files` for search and inspection whenever possible.
- Use `apply_patch` to create or edit files manually.
- Do not revert existing changes without an explicit user request.
- Do not run tests, Python, external shell tools, or tools that may read libraries/files outside the project without explicit permission.

## Architecture

Main structure:

- `app.py`: thin Flask/uWSGI entrypoint that exports `app` for `module = app:app` and keeps compatibility exports for older local imports.
- `latch/web.py`: Flask application object, context processor, authentication gate, and HTTP/SSE routes.
- `latch/config.py`: project paths, application metadata, user constants, run triggers, and timing constants.
- `latch/auth.py`: setup/login settings, password hashes, session helpers, and authorization helpers.
- `latch/modules.py`: module discovery, YAML parsing/preservation, validation, masking, and module file operations.
- `latch/runtime.py`: per-module locks, active execution state, persisted logs, kill state, batch streaming, and detached batch worker.
- `latch/events.py`: global app update event hub, replayable SSE signals, and debounced application monitor.
- `latch/scheduler.py`: module cron scheduler and scheduled detached execution.
- `latch/plugin_registry.py`: plugin type registry and plugin instantiation.
- `modules/user/`: YAML configurations for modules created by the user.
- `modules/system/`: internal modules used by development and automated tests.
- `latch/plugins/`: implementation of plugin types.
- `latch/plugins/variables.py`: validation, resolution, substitution, and masking of module variables used by commands.
- `plugins/`: compatibility wrappers for older `plugins.*` imports; new code should import `latch.plugins.*`.
- `templates/`: HTML UI with Bootstrap CDN and simple JavaScript.
- `templates/_app_header.html` and `templates/_app_footer.html`: shared discreet branding and official footer.
- `settings.yaml`: local authentication configuration with password hashes for the fixed `admin` and `user` accounts.
- `uwsgi.ini`: production uWSGI configuration, exposing HTTP directly for single-process usage with threads.
- `locks/`: per-module lock files using `filelock`.
- `temp/`: temporary execution logs and metadata, ignored by Git.
- `tests/`: contract and behavior tests.

User modules:

- Every valid file in `modules/user/<module>.yaml` is discovered automatically.
- The module ID is the filename without `.yaml` and must contain only letters, numbers, `_`, or `-`.
- Each module is exposed at `/<module>`.

Module configuration:

- Each user module has a YAML file in `modules/user/<module>.yaml`.
- YAML defines `name`, optional `description`, optional `schedule`, optional `variables`, and the ordered `plugins` list.
- `schedule` is an optional classic 5-field cron string, for example `"0 * * * *"` to run hourly.
- `schedule_enabled` is an optional boolean. When absent, a module with `schedule` remains enabled for compatibility; when `false`, the cron stays configured but does not fire.
- Empty, missing, or `null` `schedule` leaves the module without active scheduling.
- Each plugin can declare a text `description` to explain the step.
- Each plugin can declare `timeout` as a positive integer number of seconds and `timeout_retries` as a non-negative integer.
- Missing or `null` `timeout` means wait indefinitely; `timeout_retries` only applies when `timeout` is set.
- `timeout_retries` represents extra retries: `timeout_retries: 1` runs the initial attempt plus one retry.
- The list order is the exact execution order.

Example:

```yaml
name: Analytics
description: Runs an analytical ClickHouse query.
schedule_enabled: true
schedule: "0 * * * *"
variables:
  database:
    type: string
    value: analytics
  batch_limit:
    type: integer
    value: 1000
  clickhouse_password:
    type: sensitive
    value: $CLICKHOUSE_PASSWORD
plugins:
  - id: process_analytics
    type: command_line
    description: Queries analytical events while respecting the configured limit.
    command: "clickhouse-client --database {database} --password {clickhouse_password} --query 'SELECT * FROM events LIMIT {batch_limit}'"
    error_contains: "ERROR"
```

Module variables:

- `variables` is optional and scoped to the module.
- Each variable must use the explicit `{type, value}` format.
- Supported types: `string`, `integer`, and `sensitive`.
- `string` and `sensitive` require a text value; `integer` accepts either a YAML integer or numeric text.
- Values in the `$ENV_NAME` format are resolved from the environment when the plugin is created.
- A missing environment variable fails before the command starts.
- Variable names and placeholders must follow `^[A-Za-z_][A-Za-z0-9_]*$`.
- Unknown placeholders fail before the command starts.
- Placeholders can be used in executable fields of the `command_line`, `clickhouse_client`, and `redis_client` plugins.
- `sensitive` values must never appear in logs, console output, SSE, persisted JSON, or active metadata; they must be masked as `****`.
- Masking is literal against the resolved sensitive value. Transformations done by external processes, such as hashing or encoding, are not inferred.

## Business Rules

- A batch runs sequentially, one plugin at a time.
- If a plugin fails, subsequent plugins are not run.
- The lock is per module, not global.
- A running module blocks another simultaneous run of the same module.
- Different modules can run in parallel.
- The internal scheduler fires modules with `schedule` through the same detached execution path used by the interface.
- `schedule_enabled: false` prevents automatic execution even when `schedule` is filled.
- The `schedule` cron uses the server local timezone.
- If the app is down or the module is busy at the scheduled time, the missed run is not replayed; the next cron time is calculated normally.
- Scheduled modules can be run manually when stopped.
- The main page lists modules in a table and includes a dedicated status column.
- Initial setup creates passwords for the fixed `admin` and `user` accounts.
- `admin` can create, edit, validate, and delete modules.
- `user` can open modules, run batches, watch status/logs, clear logs, request `Kill`, and view the module YAML/script in read-only mode with `sensitive` values masked.
- The main page lets only `admin` add modules and edit existing modules; for `user`, the script action opens the module screen in read-only mode.
- The module page shows configured plugins in order, per-step status, console, overall status, and actions.
- The module page indicates when the module has `schedule` and shows the calculated next run when available.
- During a scheduled run, the page must behave like a reopened manual run: console, per-step status, `Kill`, `Run` blocking, and `Clear` blocking use the same persisted state.
- The edit screen uses raw YAML and offers `Validate`, `Save`, and deletion for `admin`; for `user`, the same screen works only as a read-only YAML view.
- `Validate` checks YAML syntax, schema, plugin type, variables, placeholders, and plugin instantiation without executing commands and without normalizing/reformatting the submitted YAML.
- `Save` validates again and persists the submitted raw YAML in `modules/user/<module>.yaml`, preserving literal blocks, quotes, spacing, and order entered by the user. It is blocked while the module is running.
- Deletion removes `modules/user/<module>.yaml`, `temp/temp_<module>.jsonl`, `temp/active_<module>.json`, and `locks/<module>.lock`, and is blocked while the module is running.
- Per-step statuses:
  - `Not started`: initial screen state, before a run or after clearing logs.
  - `Queued`: future step in the current run.
  - `Running`: active step.
  - `Completed`: step finished successfully.
  - `Failed`: step that stopped the batch with an error.
  - `Interrupted`: active step when the batch was killed by the user.
- The console must show real-time logs and also recover the persisted latest-run output.
- If the user leaves the page and returns during a run, the console must keep showing already generated output and continue following new logs.
- `Clear` deletes the latest-run log only if the module is not running.
- `Kill` interrupts the currently running plugin and ends the batch as `killed`.
- Plugin timeout calls `kill()` on the active plugin; if all attempts expire, the step ends as `Failed` and the batch as `failed`.

## Plugins

Every plugin must inherit from `BasePlugin`.

Base contract:

- `run() -> Iterator[PluginEvent]`: public method provided by `BasePlugin`, applies `timeout`/`timeout_retries`, and emits log events.
- `_run_once() -> Iterator[PluginEvent]`: runs one plugin attempt and emits log events.
- `kill() -> None`: interrupts the active plugin execution.
- `set_runtime_context(...)`: receives module/run context and a callback for updating execution metadata.

Standard errors:

- `PluginExecutionError`: normal plugin failure.
- `PluginKillError`: failure while trying to interrupt a plugin.
- `PluginKilledError`: plugin interrupted by user request.

New plugin types must:

- Implement `_run_once()`.
- Implement `kill()`.
- Emit clear logs.
- Record useful metadata through `update_runtime_metadata()` when applicable.
- Close resources/processes in `finally`.

## Plugin `command_line`

The `command_line` type runs host shell commands using `subprocess`.

Rules:

- `command` can be a string or a list of strings.
- A string uses `shell=True`.
- A list uses direct execution.
- `pipeline` is optional and must be a non-empty string when provided.
- With `pipeline`, the main command is connected by pipe to the raw shell command defined in `pipeline` and executed through `/bin/bash -o pipefail -c`, with `shell=False`.
- In a pipeline, a list-form main command is converted with `shlex.join(...)`; a string main command is used as is.
- `pipeline` is controlled entirely by YAML: wrappers such as `redis_client` do not propagate `host`, password, or other fields automatically to the right side of the pipe.
- If the module has `variables`, placeholders like `{variable}` are substituted before `subprocess`.
- In string commands, substituted values are escaped with `shlex.quote`.
- In list commands, substituted values become text inside the corresponding argument, without shell quoting.
- In `pipeline`, substituted values are escaped with `shlex.quote`, because the field is raw shell.
- Started-command logs and metadata use the masked command version.
- When `pipeline` is configured, logs and metadata record the complete masked command in the `<command> | <pipeline>` format.
- stdout/stderr lines are masked before becoming `PluginEvent`.
- If `variables` is configured, literal braces in commands must be escaped as `{{` and `}}`.
- The process is started with `start_new_session=True`, creating its own group.
- PID and PGID are written to active metadata in `temp/active_<module>.json`.
- Kill is done by process group through the shell:

```sh
kill -KILL -<pgid>
```

- Defensive cleanup tries:

```sh
kill -TERM -<pgid>
```

and then:

```sh
kill -KILL -<pgid>
```

Validations:

- Non-zero exit code is an error.
- If `error_contains` appears in output, it is an error.
- If `success_contains` is configured and does not appear in output, it is an error.
- Output must be captured from stdout/stderr in real time.

## Plugin `clickhouse_client`

The `clickhouse_client` type runs `/usr/bin/clickhouse-client` with `subprocess` without shell, reusing the operational behavior of `command_line`.

Fields:

- `query`: required non-empty text.
- `user`: optional text.
- `password`: optional text.
- `database`: optional text.
- `pipeline`: optional text, raw shell command for the right side of the pipe.
- `error_contains`: optional text.
- `success_contains`: optional text.

Example:

```yaml
plugins:
  - id: query_clickhouse
    type: clickhouse_client
    user: "{clickhouse_user}"
    password: "{clickhouse_password}"
    database: "{clickhouse_database}"
    query: SELECT COUNT(*) FROM relat_base_avaliacao_resposta
    error_contains: ERROR
    success_contains: null
```

Internally assembled command:

```text
/usr/bin/clickhouse-client --user ... --password ... --database ... --query ...
```

Rules:

- Without `pipeline`, execution is direct, with an argument list and `shell=False`.
- With `pipeline`, it inherits `/bin/bash -o pipefail -c` execution from `CommandLinePlugin`.
- Placeholders are resolved in `query`, `user`, `password`, and `database`.
- Placeholders are also resolved in `pipeline`.
- `password` is always masked in logs and metadata, even if it does not come from a `sensitive` variable.
- The plugin inherits stdout/stderr capture, PID/PGID, `error_contains`, `success_contains`, exit code, and process-group kill from `CommandLinePlugin`.

## Plugin `redis_client`

The `redis_client` type runs `/usr/bin/redis-cli` with `subprocess` without shell, reusing the operational behavior of `command_line`.

Fields:

- `host`: optional text, assembled as `-h <host>`.
- `args`: required, as an argument list or non-empty string parsed with `shlex.split`.
- `pipeline`: optional text, raw shell command for the right side of the pipe.
- `error_contains`: optional text.
- `success_contains`: optional text.

Example:

```yaml
plugins:
  - id: scan_redis
    type: redis_client
    host: "{redis_host}"
    args:
      - --scan
      - --pattern
      - exp_superset_data_*
    pipeline: "xargs redis-cli -h {redis_host} del"
    error_contains: ERROR
    success_contains: null
```

Internally assembled command:

```text
/usr/bin/redis-cli -h <host> <args...>
```

Rules:

- Without `pipeline`, execution is direct, with an argument list and `shell=False`.
- With `pipeline`, it inherits `/bin/bash -o pipefail -c` execution from `CommandLinePlugin`.
- List-form `args` must contain only non-empty strings.
- String-form `args` is parsed with `shlex.split`.
- Placeholders are resolved in `host` and `args`.
- Placeholders are also resolved in `pipeline`.
- `host` is not propagated automatically to `pipeline`; provide it explicitly when needed.
- The plugin inherits stdout/stderr capture, PID/PGID, `error_contains`, `success_contains`, exit code, and process-group kill from `CommandLinePlugin`.

## Execution, Status, And Logs

Main routes:

- `GET /`: main page with module table.
- `GET /modules/new`: module creation screen, restricted to `admin`.
- `GET /modules/<module>/edit`: module edit screen for `admin`; for `user`, read-only YAML view with `sensitive` values masked.
- `GET /<module>`: module page.
- `GET /api/modules/status`: status of all modules.
- `GET /api/modules/<module>/status`: status of one module.
- `POST /api/modules/validate`: validates module YAML without persisting, restricted to `admin`.
- `POST /api/modules`: creates a module in `modules/user/<module>.yaml`, restricted to `admin`.
- `PUT /api/modules/<module>`: saves YAML for an existing module, restricted to `admin`.
- `DELETE /api/modules/<module>`: deletes a module and related temporary files, restricted to `admin`.
- `GET /api/modules/<module>/run`: starts execution and follows it through SSE; execution is detached from the client connection.
- `GET /api/modules/<module>/logs`: reads persisted logs incrementally.
- `POST /api/modules/<module>/logs/clear`: clears logs when stopped.
- `POST /api/modules/<module>/kill`: requests kill of the current plugin.
- `GET /api/events`: global SSE stream of minimal update signals.

Temporary persistence:

- Latest-run logs are stored in `temp/temp_<module>.jsonl`.
- Each line is a JSON event with `run_id`, `sequence`, `event`, `created_at`, `level`, `message`, optional per-step statuses in `plugin_statuses`, and optional metadata.
- Active execution is stored in `temp/active_<module>.json`.
- Active execution includes `plugin_statuses`, run origin (`manual` or `schedule`), optional scheduled time, current plugin, kill flag, and metadata such as PID/PGID.
- Files in `temp/*.jsonl` and `temp/active_*.json` must not be versioned.
- When the application starts, generated module artifacts in `temp/temp_*.jsonl` and `temp/active_*.json` are removed.

`run_id` identifies a run. `sequence` restarts for each new run. The frontend uses both to deduplicate events, detect when the console must reset, and rebuild per-step status from persisted events.

## Interface

Current patterns:

- Bootstrap via CDN.
- Shared visual style lives in `static/app.css`; avoid inline CSS in templates unless a specific exception is justified.
- The default visual style is inspired by GitHub light: light background, white surfaces, thin borders, dense tables, blue for primary actions, and red only for destructive actions.
- Simple HTML in Jinja templates.
- The public product name is `Latch`.
- Authenticated pages must show the global header with `Latch`, module navigation, logged-in user, and logout.
- Login and setup must show `Latch` with moderate visual prominence.
- All HTML pages must include the shared footer with the official page: `https://github.com/edgarrc/latch`.
- Logs are rendered with `textContent`, never `innerHTML`.
- SSE is used for execution started by the current page.
- Global SSE is used to signal backend changes without periodic polling.
- Status/log endpoints remain the source of data; global SSE only invalidates the screen, and the frontend performs `fetch` when it receives a signal.
- The interface language is English. Internal code identifiers remain in English and must not be translated into UI copy.

System modules:

- Modules in `modules/system/*.yaml` are reserved for automated tests and internal architecture.
- They must not appear in the listing, global status, or public interface/API routes.
- Agents can edit them freely to cover test behavior.
- Modules in `modules/user/*.yaml` are operational installation data and must not be treated as fixed test-suite contracts.

Module screen states:

- `Ready`: can run and clear; kill disabled.
- `Running`: run/clear disabled; kill enabled.
- `Interrupting`: kill already requested; kill disabled.
- `Completed`: execution finished successfully.
- `Failed`: execution finished with an error.
- `Interrupted`: execution was killed by the user.

Step list states:

- The initial screen shows all steps as `Not started`.
- When a batch starts, steps become `Queued`; the current step becomes `Running` on `plugin_start`.
- `plugin_done` marks the step as `Completed`.
- `done` with status `failed` marks the payload plugin as `Failed`.
- `done` with status `killed` marks the payload plugin as `Interrupted`.
- Future steps remain `Queued` if the batch stops before reaching them.

## Concurrency

- `filelock` is required for per-module locking.
- The lock is written to `locks/<module>.lock`.
- In-memory state (`ACTIVE_RUNS`) complements the lock so the active plugin can be accessed and killed.
- The `temp/active_<module>.json` file is observability/metadata, not the main source for calling `kill()`.
- Every exit path must release the lock, remove active execution, and clear active metadata.
- The embedded scheduler assumes one process is responsible for scheduling. In a multi-worker deployment, only one process may keep the scheduler enabled.
- The supported production deployment uses `uwsgi.ini` with `processes = 1`, `threads >= 16`, `enable-threads = true`, and `lazy-apps = true`.
- Do not increase `processes` without externalizing or redesigning the scheduler and active state used by `Kill`; multiple workers can create multiple schedulers and make the active plugin inaccessible to the interruption endpoint.
- `lazy-apps = true` avoids loading the application in the uWSGI master before fork, preserving internal threads started during import and execution.
- Long runs can last for days as long as the uWSGI process remains alive; do not configure `harakiri`, `max-requests`, or time-based automatic restarts for this model.
- The SSE connection is not the execution lifetime source: manual and scheduled batches run detached and can be followed later through persisted status/logs. SSEs must emit heartbeats to survive periods without output.
- `uwsgi.ini` must keep `ignore-sigpipe`, `ignore-write-errors`, and `disable-write-exception` enabled to suppress `Broken pipe` / `OSError: write error` when the browser or proxy closes an SSE connection before the next heartbeat.

## Expected Tests

Maintain coverage for:

- YAML module loading.
- Successful sequential execution.
- Failure by exit code.
- Failure by `error_contains`.
- Failure by missing `success_contains`.
- YAML `variables` validation.
- Placeholder substitution in string and list commands.
- Environment variable resolution.
- Failure by missing environment variable.
- Failure by unknown placeholder.
- Failure by invalid `integer`.
- `sensitive` masking in logs, persisted stdout/stderr, displayed command, and active metadata.
- Per-module locking.
- Independence between modules.
- Persisted and incremental log reading.
- Execution detached from the follow-up SSE connection, so browser disconnect/reload does not interrupt the batch.
- Console reset by new `run_id`.
- Per-step status in `plugin_statuses`, including success, failure, and queued future steps.
- `plugin_statuses` persistence in `temp/active_<module>.json` during execution.
- Clear allowed when stopped.
- Clear blocked during execution.
- Kill blocked when stopped.
- Kill calling `plugin.kill()` when active.
- `command_line` recording PID/PGID and ending as `killed`.
- `clickhouse_client` assembling fixed argv with `/usr/bin/clickhouse-client`, masking password, and inheriting validations/kill from `command_line`.
- `redis_client` accepting optional `host` and list/string `args`, assembling fixed argv with `/usr/bin/redis-cli`, and inheriting validations/kill from `command_line`.
- `clickhouse_client` and `redis_client` tests must not call the real CLIs; simulate execution with temporary binaries/scripts controlled by the test suite.
- Dynamic discovery of user modules in `modules/user/*.yaml`.
- Module YAML validation without persisting and without reformatting editor content.
- Module creation and editing persisting raw validated YAML in `modules/user/<module>.yaml`.
- Save blocked when the module is running.
- Module deletion removing YAML, temporary log, temporary active execution, and lock.
- Deletion blocked when the module is running.

When adding new plugin types, include specific tests for `run()` and `kill()`.

## Dependencies

Current dependencies in `requirements.txt`:

- Flask
- PyYAML
- filelock
- croniter
- pytest

Do not introduce new dependencies without a clear need. Prefer keeping the application simple and explicit.
