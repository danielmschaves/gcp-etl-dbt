def fmt_currency(value: float, decimals: int = 2) -> str:
    return f"${value:,.{decimals}f}"


def fmt_pct(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def fmt_number(value: float) -> str:
    return f"{value:,.0f}"
