from __future__ import annotations

import pandas as pd
from typing import Any, Callable, Iterable

from mapper.features.criteria_generator import generate_configs_for_keys
from mapper.features.match_fields import coerce_match_fields, normalize_feature_specs

from ..trainer_base import TrainerBase


class Trainer(TrainerBase):
    def fit(
        self,
        _read_fn: Callable[[str], pd.Series | None],
        keys: Iterable[str],
        meta_lookup: dict[str, dict[str, Any]],
        prev_state: dict[str, Any] | None = None,
        prev_hparams: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        cfg = self.cfg or {}
        dataset = str(cfg.get("dataset") or "").strip()
        if not dataset:
            raise ValueError("-=== (features_constructor.trainer): cfg.dataset is required ===-")
        feature_specs = normalize_feature_specs(cfg.get("features"))
        use_template_features = bool(cfg.get("use_template_features", False))
        if not feature_specs and not use_template_features:
            raise ValueError("-=== (features_constructor.trainer): provide cfg.features or set use_template_features=true ===-")
        match_fields = coerce_match_fields(cfg.get("match_fields"))
        if not match_fields:
            raise ValueError("-=== (features_constructor.trainer): cfg.match_fields is required by new standard ===-")
        read_fn = self.build_read_fn("features_constructor", _read_fn)
        learned_cfgs = generate_configs_for_keys(
            dataset=dataset,
            keys=list(keys),
            _read_fn=read_fn,
            meta_lookup=meta_lookup,
            trainer_cfg=cfg,
            force=bool(cfg.get("force_retrain", False)),
        )
        return {
            "dataset": dataset,
            "feature_names": list(learned_cfgs.keys()),
            "match_fields": match_fields,
            "use_template_features": use_template_features,
            "trainer_cfg": cfg,
            "learned_cfgs": learned_cfgs,
        }
