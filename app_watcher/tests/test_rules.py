import logging

import pytest

from rules import CRASH_LOOP_WINDOW, SILENCE_LIMIT, WatcherState

T0 = 1_800_000_000.0  # any fixed Unix time; tests count seconds from here


def beat(started="2026-11-18T02:00:00Z", alpaca="2026-11-18T02:00:05Z"):
    return {"started_at": started, "sent_at": "x", "last_alpaca_message_at": alpaca}


def messages(events, level=None):
    return [m for lvl, m in events if level is None or lvl == level]


def alerts(events):
    return [m for m in messages(events) if m.startswith("ALERT")]


def run_ticks(state, start, end, every=30):
    """Tick every 30s like the watcher loop; return all events."""
    events = []
    t = start
    while t <= end:
        events += state.on_tick(t)
        t += every
    return events


@pytest.fixture
def state():
    """Watcher just started and reached the broker at T0."""
    s = WatcherState()
    s.on_broker_check(True, T0)
    return s


# --- receiver down -----------------------------------------------------------

def test_steady_beats_raise_nothing(state):
    events = []
    for n in range(10):
        events += state.on_heartbeat(beat(), T0 + 60 * n, live=True)
        events += state.on_tick(T0 + 60 * n + 30)

    assert alerts(events) == []


def test_silence_after_the_last_beat_is_receiver_down_logged_once(state):
    state.on_heartbeat(beat(), T0 + 10, live=True)

    early = run_ticks(state, T0 + 30, T0 + 10 + SILENCE_LIMIT)
    late = run_ticks(state, T0 + 10 + SILENCE_LIMIT + 1, T0 + 600)

    assert alerts(early) == []
    [alert] = alerts(late)  # once, not on every tick
    assert alert.startswith("ALERT receiver down")
    assert "2026-11-18T02:00:05Z" in alert  # shows the last Alpaca message time


def test_beat_after_receiver_down_clears_it(state):
    run_ticks(state, T0, T0 + SILENCE_LIMIT + 30)

    events = state.on_heartbeat(beat(), T0 + 300, live=True)

    assert any(m.startswith("CLEARED receiver down") for m in messages(events))
    assert alerts(run_ticks(state, T0 + 330, T0 + 400)) == []


def test_watcher_start_with_no_beats_counts_from_the_first_broker_check(state):
    # A receiver that is already dead when the watcher starts is still reported.
    events = run_ticks(state, T0, T0 + SILENCE_LIMIT + 30)

    assert [a.split(":")[0] for a in alerts(events)] == ["ALERT receiver down"]


# --- broker unreachable ------------------------------------------------------

def test_broker_unreachable_logged_once_then_cleared(state):
    events = []
    for n in range(1, 5):
        events += state.on_broker_check(False, T0 + 30 * n)
    events += state.on_broker_check(True, T0 + 150)

    assert [a.split(":")[0] for a in alerts(events)] == ["ALERT broker unreachable"]
    assert any(m.startswith("CLEARED broker unreachable") for m in messages(events))


def test_no_receiver_down_while_the_broker_is_unreachable(state):
    events = []
    for t in range(30, 1200, 30):
        events += state.on_broker_check(False, T0 + t)
        events += state.on_tick(T0 + t)

    assert not any(a.startswith("ALERT receiver down") for a in alerts(events))


def test_probe_outage_is_not_receiver_down(state):
    # The step 4 probe, in seconds after 02:20:00: last beat 02:21:40 (100), broker down
    # from 02:21:43, first good check after it returned at 255, beats again from
    # 02:24:50 (290) every 60s. With the clock counting through the outage, this
    # was a false "receiver down" between 02:24:40 and 02:24:50: the tick at 285
    # lands in that gap.
    events = state.on_heartbeat(beat(), T0 + 100, live=True)
    next_beat = 290
    for t in range(135, 1000, 30):
        events += state.on_broker_check(t >= 250, T0 + t)
        while next_beat <= t:
            events += state.on_heartbeat(beat(), T0 + next_beat, live=True)
            next_beat += 60
        events += state.on_tick(T0 + t)

    assert [a.split(":")[0] for a in alerts(events)] == ["ALERT broker unreachable"]


def test_clock_restarts_when_the_broker_comes_back(state):
    state.on_heartbeat(beat(), T0 + 100, live=True)
    state.on_broker_check(False, T0 + 130)
    state.on_broker_check(True, T0 + 250)  # back

    quiet = run_ticks(state, T0 + 260, T0 + 250 + SILENCE_LIMIT)
    loud = run_ticks(state, T0 + 250 + SILENCE_LIMIT + 1, T0 + 250 + SILENCE_LIMIT + 31)

    # A receiver that died during the outage is still caught, 3 min after the broker returns.
    assert alerts(quiet) == []
    assert [a.split(":")[0] for a in alerts(loud)] == ["ALERT receiver down"]


# --- restarts and crash loops -------------------------------------------------

def test_first_start_time_is_info_a_new_one_is_a_restart_warning(state):
    first = state.on_heartbeat(beat("2026-11-18T02:00:00Z"), T0 + 10, live=True)
    same = state.on_heartbeat(beat("2026-11-18T02:00:00Z"), T0 + 70, live=True)
    new = state.on_heartbeat(beat("2026-11-18T02:05:00Z"), T0 + 130, live=True)

    assert messages(first, logging.WARNING) == []
    assert messages(same, logging.WARNING) == []
    [warning] = messages(new, logging.WARNING)
    assert "restarted" in warning and "2026-11-18T02:05:00Z" in warning


def test_three_start_times_within_the_window_is_a_crash_loop_logged_once(state):
    events = []
    for n, minute in enumerate([0, 4, 8]):
        events += state.on_heartbeat(beat(f"2026-11-18T02:0{minute}:00Z"), T0 + 240 * n, live=True)
        events += state.on_tick(T0 + 240 * n + 1)
    events += run_ticks(state, T0 + 500, T0 + 640)  # last beat at 480: stays under SILENCE_LIMIT

    assert [a.split(":")[0] for a in alerts(events)] == ["ALERT crash loop"]


def test_start_times_spread_wider_than_the_window_are_not_a_crash_loop(state):
    events = []
    gap = CRASH_LOOP_WINDOW / 2 + 60
    for n in range(3):
        t = T0 + gap * n
        events += state.on_heartbeat(beat(f"start-{n}"), t, live=True)
        events += state.on_tick(t + 1)

    assert not any(a.startswith("ALERT crash loop") for a in alerts(events))


def test_crash_loop_clears_once_old_starts_leave_the_window(state):
    for n in range(3):
        state.on_heartbeat(beat(f"start-{n}"), T0 + 60 * n, live=True)
    state.on_tick(T0 + 121)

    events = []
    for t in range(150, int(CRASH_LOOP_WINDOW) + 200, 30):
        events += state.on_heartbeat(beat("start-2"), T0 + t, live=True)
        events += state.on_tick(T0 + t)

    assert any(m.startswith("CLEARED crash loop") for m in messages(events))


def test_replayed_restarts_are_placed_at_their_own_time():
    # Watcher restarts at T0. Its replay holds 3 start times from 14, 13 and 12 min ago
    # (placed by CreateTime). It is a crash loop, but it ends when those leave the
    # window (~3 min later), not 15 min after the watcher started.
    state = WatcherState()
    state.on_broker_check(True, T0)
    for n, minutes_ago in enumerate([14, 13, 12]):
        state.on_heartbeat(beat(f"start-{n}"), T0 - 60 * minutes_ago, live=False)

    start = state.on_tick(T0)
    events = []
    for t in range(30, 300, 30):
        events += state.on_heartbeat(beat("start-2"), T0 + t, live=True)
        events += state.on_tick(T0 + t)

    assert [a.split(":")[0] for a in alerts(start)] == ["ALERT crash loop"]
    assert any(m.startswith("CLEARED crash loop") for m in messages(events))


def test_replayed_beats_are_not_logged_one_by_one(state):
    events = state.on_heartbeat(beat(), T0 - 120, live=False)
    events += state.on_heartbeat(beat(), T0 - 60, live=False)

    assert messages(events, logging.INFO) == ["receiver seen, started at 2026-11-18T02:00:00Z"]


def test_live_beat_logs_one_status_line(state):
    events = state.on_heartbeat(beat(), T0 + 10, live=True)
    events += state.on_heartbeat(beat(), T0 + 70, live=True)

    status = [m for m in messages(events, logging.INFO) if m.startswith("receiver alive")]
    assert len(status) == 2
    assert "last Alpaca message 2026-11-18T02:00:05Z" in status[-1]
