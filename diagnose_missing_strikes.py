"""
diagnose_missing_strikes.py
------------------------------
Answers the question: "are my missing far ITM/OTM strikes a data gap, or
are they genuinely untraded?" -- using your OWN already-parsed data, no
new downloads needed.

How it works, for one (underlying, date, expiry):
  1. Load your parsed Track A data for that day/expiry.
  2. Infer the strike step from the strikes that ARE present (e.g. NIFTY
     steps by 50, BANKNIFTY by 100) by taking the most common gap between
     consecutive sorted strikes.
  3. Reconstruct the full theoretical strike ladder spanning the range you
     specify (e.g. spot +/- 20%), using that step.
  4. Diff against what's actually present -> tells you exactly which
     strikes are missing.
  5. Split everything into UPSIDE (strikes > spot) and DOWNSIDE
     (strikes < spot) and analyze each side independently, using only the
     economically relevant option type for that side (CE for upside, PE
     for downside). Calls and puts trade as separate books with
     independently-tapering liquidity -- it's completely normal for puts
     to extend further on the downside than calls do upside, or vice
     versa. An earlier version of this script pooled both sides into one
     undirected "distance from spot" comparison, which produced false-
     positive "gap" verdicts on perfectly ordinary asymmetric chains
     (caught via an actual Antigravity run against real NIFTY data, where
     downside puts traded out to ~10% while upside calls thinned out by
     ~7% -- a totally normal skew, not a gap).
  6. On each side, check whether volume/OI among PRESENT strikes tapers to
     near-zero before reaching the MISSING strikes on that same side. If
     so: genuinely untraded, not a bug. If missing strikes are closer to
     spot than where that side's own liquidity tapered off: real gap,
     worth investigating that date's download/parse.

Usage:
    python diagnose_missing_strikes.py --underlying NIFTY --date 2024-08-01 \
        --expiry 2024-08-29 --spot 24550 --range-pct 20
"""

import argparse
from datetime import date

import pandas as pd

from confignew import PARSED_HIST_DIR, FYERS_INTRADAY_DIR


def infer_strike_step(strikes: list) -> float:
    s = sorted(set(strikes))
    if len(s) < 2:
        return float("nan")
    gaps = [round(b - a, 2) for a, b in zip(s[:-1], s[1:])]
    # most common gap -- robust to the occasional irregular interval some
    # underlyings have near very high/low strikes
    return pd.Series(gaps).mode().iloc[0]


def run(underlying: str, day: date, expiry: str, spot: float, range_pct: float):
    daily_path = PARSED_HIST_DIR / underlying
    intraday_path = FYERS_INTRADAY_DIR / underlying

    frames = []
    for base, label in [(daily_path, "Track A (daily)"), (intraday_path, "Track B (intraday)")]:
        f = base / f"{underlying}_{day.year}.parquet"
        if f.exists():
            df = pd.read_parquet(f)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df[(df["date"] == day) & (df["expiry"] == expiry)]
            if not df.empty:
                df["_source_label"] = label
                frames.append(df)

    if not frames:
        print(f"No parsed data found for {underlying} on {day} expiry {expiry}. "
              f"Run download_nse_bhavcopy.py + parse_nse_bhavcopy.py for this "
              f"date first.")
        return

    data = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(data)} rows from: {data['_source_label'].unique().tolist()}\n")

    present_strikes = sorted(data["strike"].dropna().unique())
    step = infer_strike_step(present_strikes)
    print(f"Strikes present: {len(present_strikes)} "
          f"(range {min(present_strikes):.0f} - {max(present_strikes):.0f})")
    print(f"Inferred strike step: {step}\n")

    if pd.isna(step) or step <= 0:
        print("Could not infer a sane strike step (too few strikes present "
              "to detect a pattern). Try a more liquid date/underlying, or "
              "inspect present_strikes manually.")
        return

    lo = spot * (1 - range_pct / 100)
    hi = spot * (1 + range_pct / 100)
    # snap to the inferred step grid
    start = round(lo / step) * step
    theoretical = []
    s = start
    while s <= hi:
        theoretical.append(round(s, 2))
        s += step

    present_set = set(present_strikes)
    missing = [s for s in theoretical if s not in present_set]

    print(f"Theoretical ladder ({spot} +/- {range_pct}%, step {step}): "
          f"{len(theoretical)} strikes")
    print(f"Missing from your parsed data: {len(missing)} strikes\n")

    if not missing:
        print("Nothing missing in this range -- the gap you're seeing must "
              "be outside +/-{}% of spot. Re-run with a larger --range-pct."
              .format(range_pct))
        return

    # IMPORTANT: calls and puts trade as genuinely separate books and their
    # liquidity tapers off independently and asymmetrically -- it is normal
    # for puts to extend further on the downside than calls do, or vice
    # versa. Pooling "closest missing strike from either side" against
    # "farthest present strike from either side" mixes two unrelated tails
    # and produces false-positive "gap" verdicts on perfectly ordinary
    # asymmetric chains. So every comparison below is done PER SIDE
    # (upside/downside) and the liquidity taper is computed PER OPTION TYPE
    # (CE/PE), since even at the same strike the two can differ sharply.

    def side_of(strike):
        return "upside" if strike > spot else ("downside" if strike < spot else "atm")

    results_by_side = {}
    for side in ("upside", "downside"):
        side_present = [s for s in present_strikes if side_of(s) == side]
        side_missing = [s for s in missing if side_of(s) == side]

        if not side_present:
            results_by_side[side] = {
                "present": [], "missing": side_missing, "taper_dist": 0.0,
                "verdict": "real_gap" if side_missing else "n/a",
                "liquidity": pd.DataFrame(),
            }
            continue

        # liquidity per relevant option type: CE is what's traded for
        # upside strikes, PE for downside strikes -- using only the
        # economically relevant side avoids a thinly-traded OTM put, say,
        # masking real call liquidity at the same strike.
        relevant_type = "CE" if side == "upside" else "PE"
        side_data = data[data["option_type"] == relevant_type]

        liq = (
            side_data.groupby("strike")[["volume", "oi"]]
            .sum(numeric_only=True)
            .reindex(side_present)
            .fillna(0)
        )
        liq["dist_from_spot_pct"] = (
            (pd.Series(side_present, index=side_present) - spot).abs() / spot * 100
        )
        liq = liq.sort_values("dist_from_spot_pct")

        near_zero = liq[(liq["volume"] <= 1) & (liq["oi"] <= 1)]
        if near_zero.empty:
            taper_dist = liq["dist_from_spot_pct"].max()
            has_taper_signal = False
        else:
            taper_dist = near_zero["dist_from_spot_pct"].min()
            has_taper_signal = True

        # Interior-gap check: a taper point alone misses a "hole" -- e.g.
        # strikes at 3% and 5% from spot both present and liquid, but 4%
        # is missing. That's a real gap regardless of where the OVERALL
        # taper point sits. So: for each missing strike on this side, also
        # check whether any PRESENT, LIQUID strike on this side sits
        # farther from spot than it does -- if so, this missing strike is
        # sandwiched, which liquidity decay alone cannot explain.
        liquid_present = liq[(liq["volume"] > 1) | (liq["oi"] > 1)]
        max_liquid_dist = liquid_present["dist_from_spot_pct"].max() if not liquid_present.empty else -1

        if side_missing:
            missing_dists = {m: abs(m - spot) / spot * 100 for m in side_missing}
            sandwiched = [m for m, d in missing_dists.items() if d < max_liquid_dist]
            min_missing_dist = min(missing_dists.values())

            if sandwiched:
                # Definitive: these specific strikes have liquid strikes
                # further out on the same side, so "ran out of liquidity"
                # cannot explain their absence.
                verdict = "real_gap"
            elif not has_taper_signal:
                verdict = "likely_untraded"
            elif min_missing_dist >= taper_dist - 0.5:
                verdict = "likely_untraded"
            else:
                verdict = "real_gap"
        else:
            min_missing_dist = None
            sandwiched = []
            verdict = "n/a"

        results_by_side[side] = {
            "present": side_present, "missing": side_missing,
            "taper_dist": taper_dist, "min_missing_dist": min_missing_dist,
            "verdict": verdict, "liquidity": liq, "option_type": relevant_type,
            "sandwiched": sandwiched,
        }

    for side in ("upside", "downside"):
        r = results_by_side[side]
        print(f"--- {side.upper()} ({r.get('option_type', '')}) ---")
        if not r["liquidity"].empty:
            print(r["liquidity"].to_string(float_format=lambda x: f"{x:,.0f}"))
        print(f"Present: {len(r['present'])}  Missing: {len(r['missing'])}")
        if r["missing"]:
            print(f"Closest missing strike on this side: "
                  f"{r['min_missing_dist']:.1f}% from spot")
            print(f"Liquidity taper point on this side: "
                  f"{r['taper_dist']:.1f}% from spot")
            if r.get("sandwiched"):
                print(f">>> This side: REAL GAP. {len(r['sandwiched'])} missing "
                      f"strike(s) have a PRESENT, LIQUID strike farther from "
                      f"spot on the same side -- liquidity decay cannot "
                      f"explain their absence: {sorted(r['sandwiched'])}")
            elif r["verdict"] == "likely_untraded":
                print(">>> This side: likely genuinely untraded.")
            else:
                print(">>> This side: looks like a REAL GAP -- investigate "
                      "raw download/parse for this date.")
        print()

    any_real_gap = any(results_by_side[s]["verdict"] == "real_gap" for s in ("upside", "downside"))
    print("=" * 60)
    if any_real_gap:
        print(">>> OVERALL VERDICT: Real data gap detected on at least one "
              "side. Re-check the raw downloaded file and parser logs for "
              "this date.")
    else:
        print(">>> OVERALL VERDICT: Missing strikes are likely genuinely "
              "untraded on both sides (this is expected NSE behavior, not "
              "a pipeline bug). Use synthetic Black-Scholes pricing if your "
              "backtester needs a price at these strikes.")
    print("Note: upside and downside are evaluated independently -- it is "
          "normal and expected for one side's liquidity to extend further "
          "than the other's.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Diagnose missing far ITM/OTM strikes")
    ap.add_argument("--underlying", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--expiry", required=True, help="YYYY-MM-DD")
    ap.add_argument("--spot", type=float, required=True,
                     help="Underlying spot/close price on that date")
    ap.add_argument("--range-pct", type=float, default=20.0,
                     help="How far from spot (%%) to check for missing strikes")
    args = ap.parse_args()

    run(args.underlying, date.fromisoformat(args.date), args.expiry,
        args.spot, args.range_pct)