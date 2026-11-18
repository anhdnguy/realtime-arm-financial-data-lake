"""
The watcher's decision rules, with no Kafka in them. Feed in what happened
(broker checks, heartbeats, the clock); get back the log lines to write,
as (level, message) pairs. All times are Unix seconds.

  broker unreachable  list_topics failed
  receiver down       broker reachable, and no heartbeat for SILENCE_LIMIT. Counted from
                      the later of: the last heartbeat, or the moment the broker became
                      reachable (after an outage, or at watcher startup). The receiver
                      can't send while the broker is down, so that time doesn't count.
  restart             a heartbeat with a new started_at (WARNING)
  crash loop          CRASH_LOOP_STARTS or more different started_at within CRASH_LOOP_WINDOW

Alerts start with "ALERT" and are logged once when they start; "CLEARED" when they end.
grep ALERT finds every problem.
"""
import logging
from datetime import datetime, timezone

SILENCE_LIMIT = 180         # 3 missed beats: a restart (~1 min) plus a dropped beat stay under it
CRASH_LOOP_WINDOW = 15 * 60  # fits 3 runs that each die when retry() gives up (~3.5 min)
CRASH_LOOP_STARTS = 3

INFO, WARNING, ERROR = logging.INFO, logging.WARNING, logging.ERROR


def utc_text(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WatcherState:
    def __init__(self):
        self.broker_ok = None      # None until the first check
        self.clock_start = None    # where the silence clock counts from
        self.last_beat = None      # (time, beat)
        self.started_at = None     # the receiver's current start time
        self.start_times = []      # [(time first seen, started_at)] inside the window
        self.receiver_down = False
        self.crash_loop = False

    def on_broker_check(self, ok, now):
        events = []
        if ok and self.broker_ok is not True:
            # First success at startup, or back after an outage: restart the silence clock.
            self.clock_start = now
            if self.broker_ok is False:
                events.append((INFO, "CLEARED broker unreachable: list_topics works again"))
        elif not ok and self.broker_ok is not False:
            events.append((ERROR, "ALERT broker unreachable: list_topics failed; no heartbeats can arrive"))
        self.broker_ok = ok
        return events

    def on_heartbeat(self, beat, at, live):
        """
        at: when the beat counts as seen. Live: arrival (watcher's clock).
        Replayed at startup: the record's CreateTime, so old restarts keep their own time.
        """
        events = []
        self.clock_start = at if self.clock_start is None else max(self.clock_start, at)
        self.last_beat = (at, beat)

        started = beat["started_at"]
        if started != self.started_at:
            self.start_times.append((at, started))
            if self.started_at is None:
                events.append((INFO, f"receiver seen, started at {started}"))
            else:
                events.append((WARNING, f"receiver restarted at {started} (previous start {self.started_at})"))
            self.started_at = started

        if self.receiver_down:
            self.receiver_down = False
            events.append((INFO, f"CLEARED receiver down: heartbeat at {utc_text(at)}"))

        if live:
            events.append((INFO, f"receiver alive, started at {started}, "
                                 f"last Alpaca message {beat.get('last_alpaca_message_at')}"))
        return events

    def on_tick(self, now):
        """Run every check interval, after the broker check."""
        events = []

        if self.broker_ok and self.clock_start is not None and not self.receiver_down:
            silent = now - self.clock_start
            if silent > SILENCE_LIMIT:
                self.receiver_down = True
                events.append((ERROR, f"ALERT receiver down: no heartbeat for {silent:.0f}s "
                                      f"while the broker is reachable; {self._last_beat_text()}"))

        self.start_times = [(t, s) for t, s in self.start_times if now - t <= CRASH_LOOP_WINDOW]
        starts = len(self.start_times)
        if starts >= CRASH_LOOP_STARTS and not self.crash_loop:
            self.crash_loop = True
            listed = ", ".join(s for _, s in self.start_times)
            events.append((ERROR, f"ALERT crash loop: {starts} start times in "
                                  f"{CRASH_LOOP_WINDOW // 60} min: {listed}"))
        elif starts < CRASH_LOOP_STARTS and self.crash_loop:
            self.crash_loop = False
            events.append((INFO, f"CLEARED crash loop: {starts} start time(s) in the last "
                                 f"{CRASH_LOOP_WINDOW // 60} min"))
        return events

    def _last_beat_text(self):
        if self.last_beat is None:
            return "no heartbeat seen since the watcher started"
        at, beat = self.last_beat
        return (f"last heartbeat {utc_text(at)}, receiver started at {beat['started_at']}, "
                f"last Alpaca message {beat.get('last_alpaca_message_at')}")
