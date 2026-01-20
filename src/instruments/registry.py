# instruments/registry.py
"""
Banxico connector registrations.

This module is intentionally IMPORT-SAFE:
- no registry mutations
- no Constant lookups
- no side effects

Call `register_all()` explicitly from your bootstrap code.
"""

from __future__ import annotations

from threading import RLock
from typing import Any, Callable


_LOCK = RLock()
_REGISTERED = False


def register_all(*, override: bool = False) -> None:
    """
    Register everything Banxico provides:
      1) ETL builders (curves + fixings)
      2) Pricing-model index specs (QuantLib IndexSpec wiring)

    Idempotent per process.
    """
    global _REGISTERED
    with _LOCK:
        if _REGISTERED:
            return
        register_etl_builders(override=override)
        register_pricing_indices(override=override)
        _REGISTERED = True


def register_etl_builders(*, override: bool = False) -> None:
    """
    Wire Banxico ETL builders into mainsequence registries.

    IMPORTANT: No imports at module import time; we import inside the function.
    """
    from mainsequence.instruments.interest_rates.etl.registry import (
        DISCOUNT_CURVE_BUILDERS,
        FIXING_RATE_BUILDERS,
    )
    from .rates_to_curves import (
        build_banxico_mbonos_otr_zero_curve,
        update_tiie_fixings,
        update_cete_fixing,
    )

    _safe_register(
        DISCOUNT_CURVE_BUILDERS,
        "ZERO_CURVE__BANXICO_M_BONOS_OTR",
        build_banxico_mbonos_otr_zero_curve,
        override=override,
    )

    # TIIE fixings (same callable; pipeline passes the resolved UID)
    for k in (
        "REFERENCE_RATE__TIIE_OVERNIGHT",
        "REFERENCE_RATE__TIIE_28",
        "REFERENCE_RATE__TIIE_91",
        "REFERENCE_RATE__TIIE_182",
    ):
        _safe_register(FIXING_RATE_BUILDERS, k, update_tiie_fixings, override=override)

    # CETE fixings
    for k in (
        "REFERENCE_RATE__CETE_28",
        "REFERENCE_RATE__CETE_91",
        "REFERENCE_RATE__CETE_182",
    ):
        _safe_register(FIXING_RATE_BUILDERS, k, update_cete_fixing, override=override)


def register_pricing_indices(*, override: bool = False) -> None:
    """
    Wire Banxico IndexSpec definitions into the new mainsequence index registry.

    This is where banxico “owns” constants/curve uids and registers IndexSpec in the base library.
    """
    import QuantLib as ql
    from mainsequence.client import Constant as _C
    from mainsequence.instruments.pricing_models.indices import register_index_spec
    from mainsequence.instruments.pricing_models.indices_builders import (
        mx_calendar,
        mx_currency,
        tiie_spec,
        cete_spec,
        mx_gov_overnight_spec,
    )

    # Resolve curve uid from Banxico constants (done at registration time, NOT import time)
    curve_uid = _C.get_value(name="ZERO_CURVE__BANXICO_M_BONOS_OTR")

    cal = mx_calendar()
    ccy = mx_currency()
    dc = ql.Actual360()

    # ---- TIIE ----
    for const_name, days in (
        ("REFERENCE_RATE__TIIE_OVERNIGHT", 1),
        ("REFERENCE_RATE__TIIE_28", 28),
        ("REFERENCE_RATE__TIIE_91", 91),
        ("REFERENCE_RATE__TIIE_182", 182),
    ):
        index_uid = _C.get_value(name=const_name)
        register_index_spec(
            index_uid,
            lambda d=days: tiie_spec(
                curve_uid=curve_uid,
                period_days=d,
                calendar=cal,
                day_counter=dc,
                currency=ccy,
                settlement_days=1,
                bdc=ql.ModifiedFollowing,
                end_of_month=False,
            ),
            override=override,
        )

    # ---- CETE ----
    for const_name, days in (
        ("REFERENCE_RATE__CETE_28", 28),
        ("REFERENCE_RATE__CETE_91", 91),
        ("REFERENCE_RATE__CETE_182", 182),
    ):
        index_uid = _C.get_value(name=const_name)
        register_index_spec(
            index_uid,
            lambda d=days: cete_spec(
                curve_uid=curve_uid,
                period_days=d,
                calendar=cal,
                day_counter=dc,
                currency=ccy,
                settlement_days=1,
                bdc=ql.Following,
                end_of_month=False,
            ),
            override=override,
        )

    # ---- Optional: BONDES overnight index uid (only if constant exists) ----
    try:
        bondes_uid = _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT_BONDES")
    except Exception:
        bondes_uid = None

    if bondes_uid:
        register_index_spec(
            bondes_uid,
            lambda: mx_gov_overnight_spec(
                curve_uid=curve_uid,
                period_days=1,
                calendar=cal,
                day_counter=dc,
                currency=ccy,
                settlement_days=1,
                bdc=ql.ModifiedFollowing,
                end_of_month=False,
            ),
            override=override,
        )


def _safe_register(registry: Any, key: str, fn: Callable[..., Any], *, override: bool) -> None:
    """
    Registry helper that:
    - prefers registry.register(key, fn, override=...)
    - falls back to registry.register(key, fn)
    - never runs at import time (only called from register_* functions)
    """
    # Try newer signature first
    try:
        registry.register(key, fn, override=override)
        return
    except TypeError:
        pass  # old signature
    except Exception:
        # if override requested but registry can't handle it, don't hard-fail
        if override:
            return
        raise

    # Fallback signature
    try:
        registry.register(key, fn)
    except Exception:
        if override:
            return
        raise
