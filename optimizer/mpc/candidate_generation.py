from __future__ import annotations


def generate_candidates(request, num_candidates: int = 4):
    return [request for _ in range(num_candidates)]
