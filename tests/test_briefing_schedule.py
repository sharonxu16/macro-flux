import unittest
from datetime import datetime, timezone, timedelta

from briefing_schedule import select_briefing


HKT = timezone(timedelta(hours=8))


class BriefingScheduleTests(unittest.TestCase):
    def test_blank_schedule_uses_afternoon_after_noon_hkt(self):
        result = select_briefing(
            event_name="schedule",
            schedule="",
            now_hkt=datetime(2026, 9, 30, 17, 35, tzinfo=HKT),
        )

        self.assertEqual(result["label"], "afternoon")
        self.assertEqual(result["target_date"], "2026-09-30")
        self.assertEqual(result["selection_reason"], "hkt_time_fallback")

    def test_blank_schedule_uses_morning_before_noon_hkt(self):
        result = select_briefing(
            event_name="schedule",
            schedule="",
            now_hkt=datetime(2026, 9, 30, 8, 5, tzinfo=HKT),
        )

        self.assertEqual(result["label"], "morning")
        self.assertEqual(result["target_date"], "2026-09-30")

    def test_known_afternoon_schedule_is_explicit(self):
        result = select_briefing(
            event_name="schedule",
            schedule="35 9 * * *",
            now_hkt=datetime(2026, 9, 30, 17, 35, tzinfo=HKT),
        )

        self.assertEqual(result["label"], "afternoon")
        self.assertEqual(result["selection_reason"], "known_afternoon_schedule")

    def test_repository_dispatch_keeps_explicit_report_id(self):
        result = select_briefing(
            event_name="repository_dispatch",
            dispatch_type="morning",
            dispatch_date="2026-09-29",
            now_hkt=datetime(2026, 9, 30, 17, 35, tzinfo=HKT),
        )

        self.assertEqual(result["label"], "morning")
        self.assertEqual(result["target_date"], "2026-09-29")

    def test_workflow_dispatch_keeps_explicit_report_id(self):
        result = select_briefing(
            event_name="workflow_dispatch",
            manual_type="afternoon",
            manual_date="2026-09-28",
            now_hkt=datetime(2026, 9, 30, 17, 35, tzinfo=HKT),
        )

        self.assertEqual(result["label"], "afternoon")
        self.assertEqual(result["target_date"], "2026-09-28")


if __name__ == "__main__":
    unittest.main()
