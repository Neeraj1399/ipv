
import pandas as pd

def format_big_number(x):
    try:
        x = float(x)
    except Exception:
        return x
    if x >= 1e9:
        return f"{x/1e9:.2f}B"
    if x >= 1e6:
        return f"{x/1e6:.2f}M"
    if x >= 1e3:
        return f"{x/1e3:.1f}K"
    return f"{int(x)}"

def safe_pct(x):
    try:
        if x is None:
            return "N/A"
        if pd.isna(x):
            return "N/A"
        return f"{x:.2f}%"
    except Exception:
        return "N/A"
