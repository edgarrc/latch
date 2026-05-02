from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

try:
    from croniter import croniter
except ImportError:  # pragma: no cover - production installs requirements.txt.
    croniter = None


def local_now() -> datetime:
    return datetime.now().astimezone()


def format_datetime_for_display(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%d/%m/%Y %H:%M")


def validate_schedule_expression(value: Any, context: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{context} 'schedule' deve ser texto.")

    schedule = value.strip()
    if not schedule:
        return ""

    next_schedule_time(schedule, local_now())
    return schedule


def validate_schedule_enabled(value: Any, schedule: str, context: str) -> bool:
    if value is None:
        return bool(schedule)
    if not isinstance(value, bool):
        raise ValueError(f"{context} 'schedule_enabled' deve ser booleano.")
    return bool(schedule and value)


def next_schedule_time(expression: str, base_time: datetime) -> datetime:
    if len(expression.split()) != 5:
        raise ValueError(f"Invalid schedule: {expression!r}.")

    if croniter is not None:
        try:
            next_time = croniter(expression, base_time).get_next(datetime)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Invalid schedule: {expression!r}.") from exc
        if next_time.tzinfo is None and base_time.tzinfo is not None:
            next_time = next_time.replace(tzinfo=base_time.tzinfo)
        return next_time

    return next_schedule_time_fallback(expression, base_time)


def next_schedule_time_fallback(expression: str, base_time: datetime) -> datetime:
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(f"Invalid schedule: {expression!r}.")

    try:
        minutes, _minute_wildcard = parse_cron_field(fields[0], 0, 59)
        hours, _hour_wildcard = parse_cron_field(fields[1], 0, 23)
        days, day_wildcard = parse_cron_field(fields[2], 1, 31)
        months, _month_wildcard = parse_cron_field(fields[3], 1, 12)
        weekdays, weekday_wildcard = parse_cron_field(fields[4], 0, 7)
    except ValueError as exc:
        raise ValueError(f"Invalid schedule: {expression!r}.") from exc
    if 7 in weekdays:
        weekdays.add(0)
        weekdays.discard(7)

    candidate = base_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    deadline = candidate + timedelta(days=366)
    while candidate <= deadline:
        cron_weekday = (candidate.weekday() + 1) % 7
        day_matches = candidate.day in days
        weekday_matches = cron_weekday in weekdays
        if not day_wildcard and not weekday_wildcard:
            calendar_matches = day_matches or weekday_matches
        else:
            calendar_matches = day_matches and weekday_matches

        if (
            candidate.minute in minutes
            and candidate.hour in hours
            and candidate.month in months
            and calendar_matches
        ):
            return candidate
        candidate += timedelta(minutes=1)

    raise ValueError(f"Invalid schedule: {expression!r}.")


def parse_cron_field(field: str, minimum: int, maximum: int) -> tuple[set[int], bool]:
    if not field:
        raise ValueError("Empty cron field.")

    values: set[int] = set()
    wildcard = False
    for raw_part in field.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError("Empty cron field.")

        range_part = part
        step = 1
        if "/" in part:
            range_part, step_part = part.split("/", 1)
            if not step_part.isdigit():
                raise ValueError("Invalid cron step.")
            step = int(step_part)
            if step <= 0:
                raise ValueError("Invalid cron step.")

        if range_part == "*":
            start = minimum
            end = maximum
            wildcard = wildcard or step == 1
        elif "-" in range_part:
            start_part, end_part = range_part.split("-", 1)
            if not start_part.isdigit() or not end_part.isdigit():
                raise ValueError("Invalid cron range.")
            start = int(start_part)
            end = int(end_part)
        elif range_part.isdigit():
            start = end = int(range_part)
        else:
            raise ValueError("Invalid cron field.")

        if start < minimum or end > maximum or start > end:
            raise ValueError("Cron value is outside the allowed range.")
        values.update(range(start, end + 1, step))

    return values, wildcard


def scheduled_time_is_future(value: str, now: datetime) -> bool:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    if parsed.tzinfo is None and now.tzinfo is not None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    return parsed > now


def sse(event: str, payload: dict[str, Any], event_id: int | None = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"
