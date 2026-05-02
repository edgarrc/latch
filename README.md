# Latch

Latch is a Flask application for running, monitoring, and interrupting modular batches made of configurable plugins.

The project provides a simple web interface to start sequential commands by module, watch real-time logs, prevent concurrent runs of the same module, and interrupt running processes when needed.

Official page: https://github.com/edgarrc/latch

## Purpose

Latch was created to centralize local batch routines in an organized and extensible way.

Each module defines its own plugin sequence in YAML. The application loads that configuration, runs plugins in the configured order, and stops the batch if any plugin fails.

Typical use cases:

- orchestrating operational scripts;
- running analytical routines in sequence;
- watching long-running command logs in the browser;
- preventing accidental concurrent runs;
- manually interrupting processes when needed.

## Features

- User modules exposed by route, such as `/<module_id>`.
- YAML-based plugin configuration.
- Module creation and editing through the web interface.
- YAML validation before saving.
- Per-module variables with command placeholders.
- Optional per-module scheduling with 5-field cron expressions.
- Environment variable support and masked sensitive values.
- Sequential plugin execution.
- `command_line`, `clickhouse_client`, and `redis_client` plugins for running host commands.
- Real-time stdout/stderr capture.
- Validation by exit code, error string, and success string.
- Per-module locking with `filelock`.
- Persisted logs from the latest run in temporary project files.
- Temporary module log/metadata cleanup when the application starts.
- Console recovery after leaving and returning to a page during a run.
- Batch execution decoupled from the page SSE connection, so browser disconnects or reloads do not interrupt the batch.
- Per-step status on the module page, restored after page reload.
- Global SSE status/log invalidation, without periodic browser polling.
- Initial setup with two fixed users: `admin` and `user`.
- Only `admin` can create, edit, validate, and delete modules; `user` can view a module YAML/script in read-only mode.
- `Clear` button to clear logs when the module is stopped.
- `Kill` button to interrupt the active plugin.
- Simple Bootstrap-based UI via CDN.

## Stack

- Python
- Flask
- PyYAML
- filelock
- subprocess
- Server-Sent Events (SSE)
- uWSGI
- Bootstrap
- pytest

## Structure

```text
.
+-- app.py
+-- latch/
|   +-- auth.py
|   +-- config.py
|   +-- events.py
|   +-- modules.py
|   +-- plugin_registry.py
|   +-- runtime.py
|   +-- scheduler.py
|   +-- utils.py
|   +-- web.py
|   +-- plugins/
|       +-- base.py
|       +-- clickhouse_client.py
|       +-- command_line.py
|       +-- redis_client.py
|       +-- variables.py
+-- modules/
|   +-- user/
|   |   +-- *.yaml
|   +-- system/
|       +-- *.yaml
+-- plugins/
|   +-- compatibility wrappers for legacy imports
+-- templates/
|   +-- _app_footer.html
|   +-- _app_header.html
|   +-- login.html
|   +-- setup.html
|   +-- index.html
|   +-- module_edit.html
|   +-- module.html
+-- locks/
+-- temp/
+-- tests/
+-- uwsgi.ini
+-- requirements.txt
+-- AGENT.md
+-- README.md
```

`app.py` remains the Flask/uWSGI entrypoint and exports the application as `app`
for `module = app:app`. The application code lives under `latch/`: routes in
`latch/web.py`, module YAML handling in `latch/modules.py`, runtime execution
state in `latch/runtime.py`, global SSE signals in `latch/events.py`, scheduling
in `latch/scheduler.py`, and plugin implementations in `latch/plugins/`.

## Authentication

When `settings.yaml` does not exist, the application opens the initial setup and asks for one password for `admin` and one for `user`.

The `admin` user can operate batches and also create, edit, validate, and delete modules. The `user` user can open modules, run batches, watch status/logs, clear logs when allowed, request `Kill`, and view the module YAML/script in read-only mode with `sensitive` values masked.

The `settings.yaml` file stores only password hashes and the application session key.

## Module Configuration

Each user module is defined in `modules/user/<name>.yaml`. The filename without `.yaml` is the module ID and must contain only letters, numbers, `_`, or `-`.

Example:

```yaml
name: Analytics
description: Runs the analytical batch steps.
schedule_enabled: true
schedule: "0 * * * *"
plugins:
  - id: prepare_analytics
    type: command_line
    description: Prepares the analytics environment.
    command: "echo Preparing analytics module"
    error_contains: "ERROR"
    success_contains: "analytics"

  - id: process_analytics_with_sleep
    type: command_line
    description: Processes the analytics batch and validates completion.
    command: "sleep 30 && echo Analytics batch completed"
    error_contains: "ERROR"
    success_contains: "completed"
```

Use the module `description` to explain the overall purpose and each plugin `description` to explain what the step does.

Use `schedule` optionally to run the module automatically. The value must be a classic 5-field cron string, such as `"0 * * * *"` to run hourly. Use `schedule_enabled: false` to keep the cron configured without running it automatically. The cron is interpreted in the server local timezone. If Latch is down or the module is still running at the scheduled time, the missed run is not replayed; the scheduler waits for the next calculated time.

Scheduled modules still allow manual execution when stopped. During a scheduled run, the module page uses the same controls as a manual run: `Run` and `Clear` are disabled, `Kill` remains available, and the console can be reopened while following persisted logs.

Each plugin can define `timeout` and `timeout_retries`. `timeout` is optional, in positive integer seconds; when absent, the step waits indefinitely. If the timeout expires, Latch calls `kill()` on the active plugin. `timeout_retries` is optional, a non-negative integer, and only applies when `timeout` is set; `timeout_retries: 1` means the initial attempt plus one retry. If all attempts time out, the step fails and the batch is interrupted.

### Module Variables

A module can declare `variables:` to reuse values in commands. Commands use placeholders in the `{variable_name}` format.

Example:

```yaml
name: Analytics
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
  - id: query_clickhouse
    type: command_line
    command: "clickhouse-client --database {database} --password {clickhouse_password} --query 'SELECT * FROM events LIMIT {batch_limit}'"
    error_contains: "ERROR"
```

Example 2:

```yaml
name: Example module
description: Demonstrates string, integer, and sensitive variables in commands.
variables:
  routine_name:
    type: string
    value: analytics
  row_limit:
    type: integer
    value: 1000
  demo_password:
    type: sensitive
    value: demo_analytics_secret
plugins:
- id: echo_string_variable
  type: command_line
  description: Prints a substituted text variable.
  command: 'echo String variable: {routine_name}'
  error_contains: ERROR
  success_contains: analytics
- id: echo_integer_variable
  type: command_line
  description: Prints a substituted integer variable.
  command: 'echo Integer variable: {row_limit}'
  error_contains: ERROR
  success_contains: '1000'
- id: echo_sensitive_variable
  type: command_line
  description: Runs a command with a sensitive variable masked in logs.
  command: 'echo Sensitive variable: {demo_password}'
  error_contains: ERROR
  success_contains: demo_analytics_secret
- id: sleep
  type: command_line
  description: Simulates a long 10-second step.
  command: sleep 10
  error_contains: null
  success_contains: null
- id: sleep2
  type: command_line
  description: Simulates a second long step.
  command: sleep 10
  error_contains: null
  success_contains: null
```

Supported types:

- `string`: text value.
- `integer`: integer value, accepting either a YAML integer or numeric text.
- `sensitive`: text value that must not appear in logs, metadata, or the console.

When `value` uses the `$ENV_NAME` format, the application resolves the value from the `ENV_NAME` environment variable at execution time. If the environment variable does not exist, the plugin fails before starting the command.

Important rules:

- Variables are declared in module scope and can be used by that module's plugins.
- Variable names must start with a letter or `_` and contain only letters, numbers, and `_`.
- Placeholders without a configured variable make execution fail before the command starts.
- In string commands, substituted values are escaped with `shlex.quote` before execution with `shell=True`.
- In list commands, each item is executed as a direct argument, without shell interpretation.
- `sensitive` values are replaced with `****` in logs and metadata. Masking is literal: if an external process transforms a secret, for example by hashing or encoding it, that transformation is not inferred.
- To use literal braces in a command when `variables:` is configured, escape them as `{{` and `}}`.

### Plugins

#### `command_line`

Runs a direct or shell command depending on the `command` type. Optionally, `pipeline` connects the main command output to a raw shell command on the right side of the pipe, executed with Bash `pipefail`.

```yaml
plugins:
  - id: run_script
    type: command_line
    command:
      - /usr/bin/python3
      - /opt/scripts/routine.py
    pipeline: "grep completed"
    error_contains: ERROR
    success_contains: completed
```

#### `clickhouse_client`

Runs `/usr/bin/clickhouse-client` with arguments assembled by the application. The `query` field is required. `user`, `password`, `database`, and `pipeline` are optional. The password is always masked in logs and metadata, even when it does not come from a `sensitive` variable.

```yaml
plugins:
  - id: query_clickhouse
    type: clickhouse_client
    user: "{clickhouse_user}"
    password: "{clickhouse_password}"
    database: "{clickhouse_database}"
    query: SELECT COUNT(*) FROM relat_base_avaliacao_resposta
    pipeline: "grep -v '^0$'"
    error_contains: ERROR
    success_contains: null
```

Without `pipeline`, the final command runs without a shell:

```text
/usr/bin/clickhouse-client --user ... --password ... --database ... --query ...
```

With `pipeline`, the command is connected to the configured right side and executed through `/bin/bash -o pipefail -c`, failing if any pipeline step fails.

#### `redis_client`

Runs `/usr/bin/redis-cli` with host and arguments defined by the configuration. The `host` field is optional. The `args` field is required and can be an argument list or a string parsed with `shlex.split`. The `pipeline` field is optional and represents the raw shell command on the right side of the pipe.

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

Without `pipeline`, the final command runs without a shell:

```text
/usr/bin/redis-cli -h <host> <args...>
```

With `pipeline`, the assembled command is converted to shell text with `shlex.join(...)` and connected to `pipeline`. The `redis_client` `host` is not propagated automatically to that right side; provide it explicitly in `pipeline` when needed.

## Running Locally

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the application:

```bash
flask --app app run

# OR

flask --app app run --host 0.0.0.0 --port 5000
```

Open:

```text
http://127.0.0.1:5000
```

## Running In Production With uWSGI

Install dependencies in the virtual environment:

```bash
pip install -r requirements.txt
```

Start the application with uWSGI, with the virtual environment activated:

```bash
uwsgi --ini uwsgi.ini
```

Without activating the virtual environment, use:

```bash
venv/bin/uwsgi --ini uwsgi.ini
```

The `uwsgi.ini` file exposes Latch on `0.0.0.0:5000` over direct HTTP and uses a single process with threads enabled. This configuration is intentional: the embedded scheduler must exist in only one process, and `Kill` depends on the in-memory state of the process that started the active plugin. Because SSE connections remain open while the page is active, the thread pool must have room for long-lived SSEs and short requests at the same time.

Long runs do not depend on the browser connection staying open. The batch runs in a detached thread, writes status/logs to `temp/`, and can be followed later by reopening the module page. SSE connections send heartbeats to avoid idle termination when a plugin spends a long time without emitting output, and `uwsgi.ini` uses 7-day timeouts to support long follow-up sessions.

When a tab is reloaded, closed, or loses the connection during `/api/events`, uWSGI may try to write to the socket after the client disconnects. `uwsgi.ini` keeps `ignore-sigpipe`, `ignore-write-errors`, and `disable-write-exception` enabled to prevent this expected SSE termination from polluting logs with `Broken pipe` / `OSError: write error`.

For multi-day work, keep the uWSGI process stable: do not use time-based automatic restarts, `max-requests`, `harakiri`, or multi-process deployments while a run is active. If the uWSGI process is stopped or reloaded, Latch loses the in-memory state needed to monitor and interrupt the active plugin.

For this deployment model, do not increase `processes`. If Nginx or another proxy is placed in front, keep uWSGI with `processes = 1`, `enable-threads = true`, and adjust only the HTTP/socket exposure.

## Tests

```bash
pytest
```

## Extensibility

New plugins must inherit from `BasePlugin` and implement:

- `_run_once()`: runs one plugin attempt and emits log events.
- `kill()`: interrupts the active plugin execution.

The public `run()` method is provided by `BasePlugin` and applies the shared `timeout` and `timeout_retries` logic.

The full contract and architecture rules are documented in `AGENT.md`.

## Module Editing

The main page has an `Add module` button, and each row has `Edit` only for the `admin` user. For `user`, the row shows `View script`, opening the module YAML in read-only mode.

The edit screen works with raw YAML and offers:

- `Validate`: validates YAML syntax, required fields, plugin types, variables, and placeholders without persisting and without reformatting the content.
- `Save`: validates again and writes the raw YAML to `modules/user/<name>.yaml`, preserving literal blocks, quotes, spacing, and order entered in the editor.
- `Delete`: removes the module YAML and related temporary files.

Creation, validation, saving, and deletion are exclusive to `admin`. The `user` account can view YAML but cannot validate or change configuration. Saving and deleting a running module are blocked to avoid changing configuration during a batch.

## Step Statuses

On the module page, each plugin appears with an individual status:

- `Not started`: before a run or after clearing logs.
- `Queued`: future step in the current batch.
- `Running`: active step.
- `Completed`: step finished successfully.
- `Failed`: step that interrupted the batch with an error.
- `Interrupted`: active step when the user requested `Kill`.

An exhausted timeout is handled as an operational failure: the step appears as `Failed`.

These states are reconstructed from persisted run events, so they continue to appear correctly after reloading the page during or after a batch.

## Security Notes

Execution plugins run commands on the host. YAML files must therefore be treated as trusted configuration.

Use `sensitive` for passwords, tokens, and secrets. Do not put secrets directly in `string` or `integer`, because those values may appear in logs.

Do not expose this application publicly without appropriate authentication, authorization, and security review.
