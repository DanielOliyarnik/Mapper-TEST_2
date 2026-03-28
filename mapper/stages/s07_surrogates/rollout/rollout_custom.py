from __future__ import annotations

from .zone_rollout import coordinate_zone_rollout


def run_rollout_custom(zone_root, **kwargs):
    return coordinate_zone_rollout(zone_root, **kwargs)
