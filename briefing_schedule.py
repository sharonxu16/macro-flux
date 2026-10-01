"""Select the report window for GitHub Actions triggers."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone


HKT = timezone(timedelta(hours=8))
MORNING_SCHEDULES = {
    "5 0 * * *",
    "15 0 * * *",
    "25 0 * * *",
    "35 0 * * *",
    "5 1 * * *",
    "17 1 * * *",
    "31 1 * * *",
    "47 1 * * *",
    "13 2 * * *",
    "43 2 * * *",
    "13 3 * * *",
}
AFTERNOON_SCHEDULES = {
    "35 9 * * *",
    "45 9 * * *",
    "55 9 * * *",
    "5 10 * * *",
    "20 10 * * *",
    "30 11 * * *",
}


def _target_date(label: str, now_hkt: datetime) -> date:
    if label == "morning":
        return now_hkt.date() if now_hkt.hour >= 6 else now_hkt.date() - timedelta(days=1)
    if label == "afternoon":
        return now_hkt.date() if now_hkt.hour >= 12 else now_hkt.date() - timedelta(days=1)
    raise ValueError(f"Unsupported briefing_type: {label}")


def _infer_scheduled_type(now_hkt: datetime) -> str:
    if now_hkt.hour < 6:
        return "afternoon"
    if now_hkt.hour < 12:
        return "morning"
    return "afternoon"


def select_briefing(
    *,
    event_name: str,
    schedule: str = "",
    manual_type: str = "",
    manual_date: str = "",
    dispatch_type: str = "",
    dispatch_date: str = "",
    now_hkt: datetime | None = None,
) -> dict[str, str]:
    now_hkt = now_hkt or datetime.now(HKT)
    event_name = event_name.strip()
    schedule = schedule.strip()

    if event_name == "schedule":
        if schedule in MORNING_SCHEDULES:
            briefing_type = "morning"
            selection_reason = "known_morning_schedule"
        elif schedule in AFTERNOON_SCHEDULES:
            briefing_type = "afternoon"
            selection_reason = "known_afternoon_schedule"
        else:
            briefing_type = _infer_scheduled_type(now_hkt)
            selection_reason = "hkt_time_fallback"
        target_date = _target_date(briefing_type, now_hkt)
    elif event_name == "repository_dispatch":
        briefing_type = dispatch_type.strip() or "morning"
        target_date = date.fromisoformat(dispatch_date) if dispatch_date.strip() else now_hkt.date()
        selection_reason = "repository_dispatch"
    elif event_name == "workflow_dispatch":
        briefing_type = manual_type.strip() or "morning"
        target_date = date.fromisoformat(manual_date) if manual_date.strip() else now_hkt.date()
        selection_reason = "workflow_dispatch"
    else:
        raise ValueError(f"Unsupported GitHub event: {event_name!r}")

    if briefing_type == "morning":
        start_date = target_date - timedelta(days=3 if target_date.weekday() == 0 else 1)
        start = f"{start_date} 17:30"
        end = f"{target_date} 08:00"
        flag = "--morning"
    elif briefing_type == "afternoon":
        start = f"{target_date} 08:00"
        end = f"{target_date} 17:30"
        flag = "--afternoon"
    else:
        raise ValueError(f"Unsupported briefing_type: {briefing_type}")

    return {
        "flag": flag,
        "label": briefing_type,
        "window_from": start,
        "window_to": end,
        "target_date": target_date.isoformat(),
        "selection_reason": selection_reason,
    }


def main() -> None:
    result = select_briefing(
        event_name=os.environ.get("EVENT_NAME") or os.environ.get("GITHUB_EVENT_NAME", ""),
        schedule=os.environ.get("EVENT_SCHEDULE", ""),
        manual_type=os.environ.get("INPUT_BRIEFING_TYPE", ""),
        manual_date=os.environ.get("INPUT_RUN_DATE_HKT", ""),
        dispatch_type=os.environ.get("DISPATCH_BRIEFING_TYPE", ""),
        dispatch_date=os.environ.get("DISPATCH_RUN_DATE_HKT", ""),
    )

    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key in ("flag", "label", "window_from", "window_to", "target_date"):
            print(f"{key}={result[key]}", file=output)

    print(
        "Selected "
        f"{result['label']}: {result['window_from']} -> {result['window_to']} HKT "
        f"(event={os.environ.get('EVENT_NAME') or os.environ.get('GITHUB_EVENT_NAME', '')!r}, "
        f"schedule={os.environ.get('EVENT_SCHEDULE', '')!r}, reason={result['selection_reason']})"
    )


if __name__ == "__main__":
    main()
