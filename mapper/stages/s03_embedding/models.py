from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AutoEncoderModel:
    encoder_name: str = "cnn1d_encoder"
    latent_dim: int = 64
