# indicators.py

import pandas_ta as ta

def add_indicators(df, tf_params):
    """
    df: 원본 DataFrame (열 이름: open, high, low, close, volume)
    tf_params: 보조지표에 사용할 파라미터(딕셔너리)
    """
    # (1) 컬럼명 변경(pandas_ta 표준)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # ADX
    df.ta.adx(high="High", low="Low", close="Close",
              length=tf_params.get('adx_length', 14),
              append=True)

    # CHOP
    df.ta.chop(high="High", low="Low", close="Close",
               length=tf_params.get('chop_length', 14),
               append=True)

    # Bollinger Bands
    df.ta.bbands(close="Close",
                 length=tf_params.get('bb_length', 20),
                 std=tf_params.get('bb_std', 2.0),
                 append=True)

    # MACD
    df.ta.macd(close="Close",
               fast=tf_params.get('macd_fast', 12),
               slow=tf_params.get('macd_slow', 26),
               signal=tf_params.get('macd_signal', 9),
               append=True)

    # Ichimoku
    df.ta.ichimoku(high="High", low="Low", close="Close",
                   tenkan=tf_params.get('ichimoku_tenkan', 9),
                   kijun=tf_params.get('ichimoku_kijun', 26),
                   senkou=tf_params.get('ichimoku_senkou', 52),
                   append=True)

    # Keltner Channel
    df.ta.kc(high="High", low="Low", close="Close",
             length=tf_params.get('kc_length', 20),
             scalar=tf_params.get('kc_scalar', 1.5),
             append=True)

    # TTM Trend
    df.ta.ttm_trend(high="High", low="Low", close="Close",
                    length=tf_params.get('ttm_length', 15),
                    append=True)

    # Squeeze Pro & Squeeze
    df.ta.squeeze_pro(bb_length=tf_params.get('bb_length', 20),
                      bb_std=tf_params.get('bb_std', 2.0),
                      kc_length=tf_params.get('kc_length', 20),
                      kc_mult=tf_params.get('kc_scalar', 1.5),
                      append=True)

    df.ta.squeeze(bb_length=tf_params.get('bb_length', 20),
                  bb_std=tf_params.get('bb_std', 2.0),
                  kc_length=tf_params.get('kc_length', 20),
                  kc_mult=tf_params.get('kc_scalar', 1.5),
                  append=True)

    # Aroon
    df.ta.aroon(high="High", low="Low",
                length=tf_params.get('aroon_length', 14),
                append=True)

    # Supertrend
    df.ta.supertrend(high="High", low="Low", close="Close",
                     length=tf_params.get('supertrend_length', 14),
                     multiplier=tf_params.get('supertrend_multiplier', 3.0),
                     append=True)

    # Parabolic SAR
    df.ta.psar(high="High", low="Low", close="Close",
               step=tf_params.get('psar_step', 0.02),
               max_step=tf_params.get('psar_max_step', 0.2),
               append=True)

    # Schaff Trend Cycle
    df.ta.stc(close="Close",
              fast=tf_params.get('stc_fast', 14),
              slow=tf_params.get('stc_slow', 28),
              factor=tf_params.get('stc_factor', 0.5),
              append=True)

    # KST Oscillator
    df.ta.kst(close="Close",
              roc1=tf_params.get('kst_roc1', 9),
              roc2=tf_params.get('kst_roc2', 13),
              roc3=tf_params.get('kst_roc3', 15),
              roc4=tf_params.get('kst_roc4', 20),
              smroc1=tf_params.get('kst_smroc1', 6),
              smroc2=tf_params.get('kst_smroc2', 6),
              smroc3=tf_params.get('kst_smroc3', 6),
              smroc4=tf_params.get('kst_smroc4', 6),
              signal=tf_params.get('kst_signal', 9),
              append=True)

    # VHF
    df.ta.vhf(close="Close",
              length=tf_params.get('vhf_length', 14),
              append=True)

    # Efficiency Ratio
    df.ta.er(close="Close",
             length=tf_params.get('er_length', 10),
             append=True)

    # Inertia
    df.ta.inertia(close="Close",
                  r=tf_params.get('inertia_r', 14),
                  append=True)

    # Chande Kroll Stop
    df.ta.cksp(high="High", low="Low", close="Close",
               len1=tf_params.get('cksp_len1', 10),
               len2=tf_params.get('cksp_len2', 20),
               append=True)

    # Vortex
    df.ta.vortex(high="High", low="Low", close="Close",
                 length=tf_params.get('vortex_length', 14),
                 append=True)

    # AMAT
    df.ta.amat(close="Close",
               fast=tf_params.get('amat_fast', 10),
               slow=tf_params.get('amat_slow', 20),
               append=True)

    # Trend Signals
    df.ta.tsignals(close="Close",
                   length=tf_params.get('tsignals_length', 14),
                   append=True)

    # KAMA
    df.ta.kama(close="Close",
               length=tf_params.get('kama_length', 10),
               fast=tf_params.get('kama_fast', 2),
               slow=tf_params.get('kama_slow', 30),
               append=True)

    # Donchian Channel
    df.ta.donchian(high="High", low="Low", close="Close",
                   lower_length=tf_params.get('donchian_lower', 20),
                   upper_length=tf_params.get('donchian_upper', 20),
                   append=True)

    # Slope
    df.ta.slope(close="Close",
                length=tf_params.get('slope_length', 10),
                append=True)

    # ATR
    df.ta.atr(high="High", low="Low", close="Close",
              length=tf_params.get('atr_length', 14),
              append=True)

    # RSI
    df.ta.rsi(close="Close",
              length=tf_params.get('rsi_length', 14),
              append=True)

    # Stochastic
    df.ta.stoch(high="High", low="Low", close="Close",
                k=tf_params.get('stoch_k', 14),
                d=tf_params.get('stoch_d', 3),
                smooth=tf_params.get('stoch_smooth', 3),
                append=True)

    # OBV
    df.ta.obv(close="Close", volume="Volume", append=True)

    # CCI
    df.ta.cci(high="High", low="Low", close="Close",
              length=tf_params.get('cci_length', 20),
              c=tf_params.get('cci_constant', 0.015),
              append=True)

    # PVO
    df.ta.pvo(volume="Volume",
              fast=tf_params.get('pvo_fast', 12),
              slow=tf_params.get('pvo_slow', 26),
              signal=tf_params.get('pvo_signal', 9),
              append=True)

    # MFI
    df.ta.mfi(high="High", low="Low", close="Close",
              volume="Volume",
              length=tf_params.get('mfi_length', 14),
              append=True)

    # (마무리) 컬럼명 원복
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    return df
