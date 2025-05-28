from datetime import datetime

import pandas as pd


def round_time(t: datetime, freq: str | None) -> datetime:
    if freq is None:
        return t
    return pd.Timestamp(t).round(freq).to_pydatetime()


def ceil_time(t: datetime, freq: str | None) -> datetime:
    if freq is None:
        return t
    return pd.Timestamp(t).ceil(freq).to_pydatetime()
