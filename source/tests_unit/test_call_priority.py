"""Tests de CallPriority (weight + envejecimiento)."""
import os
import sys

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from services.routing.call_priority import compute_call_priority  # noqa: E402


def test_weight2_wait5_beats_weight1_wait10():
    """Escenario Jira: B(w=2,5s) gana a A(w=1,10s)."""
    pri_a = compute_call_priority(
        1, 10.0, tau=40.0, weight_norm_max=10.0, w_weight=0.6, w_age=0.4
    )
    pri_b = compute_call_priority(
        2, 5.0, tau=40.0, weight_norm_max=10.0, w_weight=0.6, w_age=0.4
    )
    assert pri_b > pri_a


def test_long_wait_low_weight_can_overtake():
    """A con wait largo puede superar a B recién llegada."""
    pri_a = compute_call_priority(
        1, 180.0, tau=40.0, weight_norm_max=10.0, w_weight=0.6, w_age=0.4
    )
    pri_b = compute_call_priority(
        2, 1.0, tau=40.0, weight_norm_max=10.0, w_weight=0.6, w_age=0.4
    )
    assert pri_a > pri_b


def test_priority_in_unit_interval_range():
    p = compute_call_priority(5, 40.0, tau=40.0, weight_norm_max=10.0, w_weight=0.6, w_age=0.4)
    assert 0.0 <= p <= 1.0
