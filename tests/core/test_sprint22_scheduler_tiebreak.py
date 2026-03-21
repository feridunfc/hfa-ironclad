from types import SimpleNamespace

import pytest

from hfa_control.scheduler_types import SchedulingDecisionScore


def make_worker(
    worker_id,
    worker_group="group-default",
    region="region-default",
    capacity=10,
    inflight=0,
    load_factor=0.0,
    schedulable=True,
):
    return SimpleNamespace(
        worker_id=worker_id,
        worker_group=worker_group,
        region=region,
        capacity=capacity,
        inflight=inflight,
        load_factor=load_factor,
        schedulable=schedulable,
    )


def test_score_sort_key_prefers_lower_primary_score():
    a = SchedulingDecisionScore(
        primary_score=200,
        load_factor=0.1,
        inflight=1,
        capacity=10,
        worker_group="g1",
        region="r1",
        worker_id="w-1",
    )
    b = SchedulingDecisionScore(
        primary_score=100,
        load_factor=0.9,
        inflight=9,
        capacity=1,
        worker_group="g9",
        region="r9",
        worker_id="w-9",
    )
    assert b.as_sort_key() < a.as_sort_key()


def test_score_sort_key_prefers_lower_load_factor_when_primary_equal():
    a = SchedulingDecisionScore(100, 0.8, 8, 10, "g1", "r1", "w-1")
    b = SchedulingDecisionScore(100, 0.2, 2, 10, "g1", "r1", "w-2")
    assert b.as_sort_key() < a.as_sort_key()


def test_score_sort_key_prefers_lower_inflight_when_primary_and_load_equal():
    a = SchedulingDecisionScore(100, 0.5, 6, 12, "g1", "r1", "w-1")
    b = SchedulingDecisionScore(100, 0.5, 4, 8, "g1", "r1", "w-2")
    assert b.as_sort_key() < a.as_sort_key()


def test_score_sort_key_prefers_higher_capacity_when_primary_load_inflight_equal():
    a = SchedulingDecisionScore(100, 0.5, 5, 10, "g1", "r1", "w-1")
    b = SchedulingDecisionScore(100, 0.5, 5, 20, "g1", "r1", "w-2")
    assert b.as_sort_key() < a.as_sort_key()


def test_score_sort_key_prefers_topology_then_worker_id_as_last_fallback():
    a = SchedulingDecisionScore(100, 0.5, 5, 10, "group-b", "eu-west", "w-1")
    b = SchedulingDecisionScore(100, 0.5, 5, 10, "group-a", "us-east", "w-2")
    assert b.as_sort_key() < a.as_sort_key()


class FakeScheduler:
    def _build_worker_decision_score(
        self,
        worker,
        primary_score: int,
    ) -> SchedulingDecisionScore:
        return SchedulingDecisionScore(
            primary_score=int(primary_score),
            load_factor=float(getattr(worker, "load_factor", 1.0)),
            inflight=int(getattr(worker, "inflight", 0)),
            capacity=int(getattr(worker, "capacity", 0)),
            worker_group=str(getattr(worker, "worker_group", "")),
            region=str(getattr(worker, "region", "")),
            worker_id=str(getattr(worker, "worker_id", "")),
        )

    def _pick_best_worker(
        self,
        workers: list,
        primary_scores_by_worker_id: dict[str, int],
    ):
        candidates = []

        for worker in workers:
            if not getattr(worker, "schedulable", False):
                continue

            worker_id = str(getattr(worker, "worker_id", "")).strip()
            if not worker_id:
                continue

            if worker_id not in primary_scores_by_worker_id:
                continue

            score_obj = self._build_worker_decision_score(
                worker=worker,
                primary_score=primary_scores_by_worker_id[worker_id],
            )
            candidates.append((score_obj, worker))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0].as_sort_key())
        return candidates[0][1]


def test_pick_best_worker_ignores_unschedulable_workers():
    scheduler = FakeScheduler()

    w1 = make_worker("w-1", schedulable=False, load_factor=0.0)
    w2 = make_worker("w-2", schedulable=True, load_factor=1.0)

    scores = {"w-1": 0, "w-2": 1000}
    best = scheduler._pick_best_worker([w1, w2], scores)

    assert best is not None
    assert best.worker_id == "w-2"


def test_pick_best_worker_ignores_empty_worker_id():
    scheduler = FakeScheduler()

    bad = make_worker("", schedulable=True, load_factor=0.0)
    good = make_worker("w-2", schedulable=True, load_factor=1.0)

    scores = {"": 0, "w-2": 100}
    best = scheduler._pick_best_worker([bad, good], scores)

    assert best is not None
    assert best.worker_id == "w-2"


def test_pick_best_worker_ignores_workers_missing_primary_score():
    scheduler = FakeScheduler()

    scored = make_worker("w-1", schedulable=True, load_factor=0.5)
    unscored = make_worker("w-2", schedulable=True, load_factor=0.0)

    scores = {"w-1": 50}
    best = scheduler._pick_best_worker([unscored, scored], scores)

    assert best is not None
    assert best.worker_id == "w-1"


def test_pick_best_worker_is_input_order_independent():
    scheduler = FakeScheduler()

    w_a = make_worker("w-a", "group-1", "eu", load_factor=0.5)
    w_b = make_worker("w-b", "group-1", "eu", load_factor=0.5)
    w_c = make_worker("w-c", "group-1", "eu", load_factor=0.5)

    scores = {"w-a": 50, "w-b": 50, "w-c": 50}

    best_1 = scheduler._pick_best_worker([w_c, w_b, w_a], scores)
    best_2 = scheduler._pick_best_worker([w_b, w_a, w_c], scores)

    assert best_1 is not None
    assert best_2 is not None
    assert best_1.worker_id == "w-a"
    assert best_2.worker_id == "w-a"


def test_pick_best_worker_prefers_capacity_only_after_earlier_tiebreaks_match():
    scheduler = FakeScheduler()

    small = make_worker(
        "w-small",
        worker_group="group-a",
        region="eu",
        capacity=10,
        inflight=2,
        load_factor=0.2,
        schedulable=True,
    )
    large = make_worker(
        "w-large",
        worker_group="group-a",
        region="eu",
        capacity=20,
        inflight=2,
        load_factor=0.2,
        schedulable=True,
    )

    scores = {"w-small": 100, "w-large": 100}
    best = scheduler._pick_best_worker([small, large], scores)

    assert best is not None
    assert best.worker_id == "w-large"