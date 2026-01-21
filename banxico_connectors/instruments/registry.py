# banxico_connectors/instruments/registry.py
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


from functools import partial

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
    """Wire Banxico IndexSpec definitions into the mainsequence index registry.

    Notes
    -----
    - Banxico "owns" constants/curve uids and registers IndexSpec in the base library.
    - Constant resolution is done at *registration time* (not import time).
    - Banxico/MX-specific conventions live here (not in `indices_builders`).
    """

    import QuantLib as ql

    from mainsequence.client import Constant as _C
    from mainsequence.instruments.pricing_models.indices import register_index_spec
    from mainsequence.instruments.pricing_models.indices_builders import IndexSpec

    def _const(name: str) -> str:
        """Resolve a Constant value by name (kept inside the function on purpose)."""
        return _C.get_value(name=name)

    # ----------------------------- Banxico-local helpers --------------------------------- #

    def _mx_calendar() -> ql.Calendar:
        # Keep a safe fallback for older QuantLib builds.
        return ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()

    def _mx_currency() -> ql.Currency:
        return ql.MXNCurrency() if hasattr(ql, "MXNCurrency") else ql.USDCurrency()

    cal = _mx_calendar()
    ccy = _mx_currency()
    dc = ql.Actual360()

    # Resolve curve uids from constants (done at registration time, NOT import time)
    curve_uid_mbonos = _const("ZERO_CURVE__BANXICO_M_BONOS_OTR")
    curve_uid_tiie = _const("ZERO_CURVE__VALMER_TIIE_28")

    def _build_ibor_days_spec(
        *,
        curve_uid: str,
        period_days: int,
        settlement_days: int = 1,
        bdc: int = ql.ModifiedFollowing,
        end_of_month: bool = False,
        fixings_uid: str | None = None,
    ) -> IndexSpec:
        """Build an IndexSpec for a days-tenor ibor-like index under MX conventions."""
        return IndexSpec(
            curve_uid=curve_uid,
            calendar=cal,
            day_counter=dc,
            currency=ccy,
            period=ql.Period(int(period_days), ql.Days),
            settlement_days=int(settlement_days),
            bdc=int(bdc),
            end_of_month=bool(end_of_month),
            fixings_uid=fixings_uid,
        )

    def _register_tenors(
        *,
        curve_uid: str,
        tenors: tuple[tuple[str, int], ...],
        bdc: int,
        settlement_days: int = 1,
    ) -> None:
        """Register multiple tenors that share the same curve + conventions."""
        for const_name, days in tenors:
            index_uid = _const(const_name)
            register_index_spec(
                index_uid,
                partial(
                    _build_ibor_days_spec,
                    curve_uid=curve_uid,
                    period_days=days,
                    settlement_days=settlement_days,
                    bdc=bdc,
                    end_of_month=False,
                ),
                override=override,
            )

    # ---- TIIE ----
    _register_tenors(
        curve_uid=curve_uid_tiie,
        tenors=(
            ("REFERENCE_RATE__TIIE_OVERNIGHT", 1),
            ("REFERENCE_RATE__TIIE_28", 28),
            ("REFERENCE_RATE__TIIE_91", 91),
            ("REFERENCE_RATE__TIIE_182", 182),
        ),
        bdc=ql.ModifiedFollowing,
        settlement_days=1,
    )

    # ---- CETE ----
    _register_tenors(
        curve_uid=curve_uid_mbonos,
        tenors=(
            ("REFERENCE_RATE__CETE_28", 28),
            ("REFERENCE_RATE__CETE_91", 91),
            ("REFERENCE_RATE__CETE_182", 182),
        ),
        bdc=ql.Following,
        settlement_days=1,
    )

    # ---- Optional: BONDES overnight index uid (only if constant exists) ----
    try:
        bondes_uid = _const("REFERENCE_RATE__TIIE_OVERNIGHT_BONDES")
    except Exception:
        bondes_uid = None

    if bondes_uid:
        register_index_spec(
            bondes_uid,
            partial(
                _build_ibor_days_spec,
                curve_uid=curve_uid_mbonos,
                period_days=1,
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
