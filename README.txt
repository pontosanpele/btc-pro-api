BTC PRO V16 MODULAR

Felépítés:
- btc_pro_config.py      -> konstansok, utilok
- btc_pro_market.py      -> API / market data réteg
- btc_pro_history.py     -> history kezelés és diff
- btc_pro_signals.py     -> nyers jelzések
- btc_pro_levels.py      -> szintek / triggerek / retest v2
- btc_pro_validation.py  -> breakout / rejection / no-trade / trap / execution / orderflow
- btc_pro_bias.py        -> score, bias, readiness, arbitration, consensus, conflict
- btc_pro_state.py       -> regime, setup, lifecycle, state machine, summary, final recommendation
- btc_pro_strategy.py    -> orchestration pipeline
- btc_pro_runner.py      -> belépési pont

V16 újítások:
- retest detector v2 (winner-side logika)
- rejection detector
- orderflow consistency score
- direction consensus engine
- signal conflict detector
- entry timing score
- state machine v2
- final recommendation engine

Futtatás:
python3 btc_pro_runner.py


V17 újítások:
- confidence decomposition
- execution feasibility score
- expected value score
- decision drivers positive / negative
- hierarchical decision engine
- final recommendation v2


V18 újítások:
- context penalty score
- invalidation quality
- decision hysteresis v2
- final judgment tiers
- final recommendation v3


V19 újítások:
- robust normalization (percentile rank, robust z-score, noise floor)
- rolling trend comparison helpers
- interaction scores / confirmation clusters
- regime-adaptive weighting
- recalibrated breakout quality
- recalibrated setup readiness
- v2 composite scoring and v4 final recommendation
- designed for easy future tuning in btc_pro_metrics.py and config
