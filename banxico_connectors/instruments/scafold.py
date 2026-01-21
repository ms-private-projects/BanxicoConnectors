# banxico_connectors/instruments/boostrap.py
from __future__ import annotations

from mainsequence.client import Constant as _C


DEFAULT_CONSTANTS = dict(
    # -----------------------
    # Reference rates (UIDs)
    # -----------------------
    REFERENCE_RATE__TIIE_28="TIIE_28",
    REFERENCE_RATE__TIIE_91="TIIE_91",
    REFERENCE_RATE__TIIE_182="TIIE_182",
    REFERENCE_RATE__TIIE_OVERNIGHT="TIIE_OVERNIGHT",

    REFERENCE_RATE__CETE_28="CETE_28",
    REFERENCE_RATE__CETE_91="CETE_91",
    REFERENCE_RATE__CETE_182="CETE_182",

    REFERENCE_RATE__TIIE_OVERNIGHT_BONDES="TIIE_OVERNIGHT_BONDES",

    # -----------------------
    # Curves (UIDs)
    # -----------------------
    ZERO_CURVE__BANXICO_M_BONOS_OTR="BANXICO_M_BONOS_OTR",




)


def seed_defaults() -> None:
    _C.create_constants_if_not_exist(DEFAULT_CONSTANTS)
