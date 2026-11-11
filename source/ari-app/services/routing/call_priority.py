"""CallPriority: weight de cola + envejecimiento por espera."""

from __future__ import annotations

import math
from typing import Optional

from config import settings


def compute_call_priority(
    weight: float,
    wait_sec: float,
    *,
    tau: Optional[float] = None,
    weight_norm_max: Optional[float] = None,
    w_weight: Optional[float] = None,
    w_age: Optional[float] = None,
) -> float:
    """
    CallPriority = w_weight * weight_norm + w_age * age

    age = 1 - exp(-wait_sec / tau)
    weight_norm = clamp(weight, 0, W_MAX) / W_MAX
    """
    tau_v = float(tau if tau is not None else settings.ACD_CALL_PRIORITY_TAU_SEC)
    w_max = float(
        weight_norm_max if weight_norm_max is not None else settings.ACD_WEIGHT_NORM_MAX
    )
    ww = float(
        w_weight if w_weight is not None else settings.ACD_CALL_PRIORITY_W_WEIGHT
    )
    wa = float(w_age if w_age is not None else settings.ACD_CALL_PRIORITY_W_AGE)

    if tau_v <= 0:
        tau_v = 40.0
    if w_max <= 0:
        w_max = 10.0

    wait = max(0.0, float(wait_sec or 0.0))
    age = 1.0 - math.exp(-wait / tau_v)

    w = float(weight or 0.0)
    if w < 0:
        w = 0.0
    if w > w_max:
        w = w_max
    weight_norm = w / w_max

    return ww * weight_norm + wa * age
