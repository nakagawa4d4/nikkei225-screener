import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import stock_screener as ss

st.set_page_config(
    page_title="Nikkei225 スクリーニング",
    page_icon="📈",
    layout="wide"
)

st.title("Nikkei225 スクリーニング")
st.markdown("""
このダッシュボードは、日経225構成銘柄を対象に、直近の株価モメンタムや出来高急増、テクニカル指標（RSI, ボリンジャーバンド, MACD）を総合的に分析・可視化したものです。
グラフ上のバブルや右側の凡例（業種名）をクリックすることで、気になる銘柄やセクターを直感的に絞り込むことができます。
""")

# Sidebar
st.sidebar.header("設定")
if st.sidebar.button("データの再取得（キャッシュクリア）"):
    st.cache_data.clear()
    st.rerun()
lookback_days = st.sidebar.slider("データ取得期間（過去日数）", min_value=90, max_value=360, value=ss.LOOKBACK_DAYS, step=30)

# ==========================================
# データの取得とキャッシュ
# ==========================================
# Streamlit CloudでのAPI制限や毎回のローディング待ちを避けるため、1日1回などのキャッシュを効かせる
@st.cache_data(ttl=3600*12) # 12時間キャッシュ
def load_and_screen_data(days):
    tickers = list(ss.TICKERS.keys())
    # データを取得
    data_dict = ss.fetch_stock_data(tickers, days)
    if not data_dict:
        return pd.DataFrame()
        
    # スクリーニングと指標計算
    df_results = ss.screen_stocks(data_dict)
    
    # ティッカーから会社名、業種をマッピング
    df_results['Company'] = df_results['Ticker'].map(ss.TICKERS)
    df_results['Sector'] = df_results['Ticker'].map(ss.SECTORS).fillna("Unknown")
    
    return df_results

with st.spinner('株価データを取得・分析しています（初回・更新時は1〜2分かかります）...'):
    df_results = load_and_screen_data(lookback_days)

if df_results.empty:
    st.error("データの取得に失敗しました。時間をおいて再試行してください。")
    st.stop()

# 古いキャッシュが読み込まれたときのKeyError対策
for col in ['Daily_Return_%']:
    if col not in df_results.columns:
        df_results[col] = np.nan

# ==========================================
# ダッシュボードとサマリーの表示
# ==========================================
# 最終更新日時の表示
st.caption(f"データ最終取得日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (12時間キャッシュ有効)")

# メインのグラフ描画
with st.spinner('ダッシュボードを生成中...'):
    # stock_screener.py に実装した関数を使ってPlotly Figureを取得
    fig = ss.generate_dashboard(df_results)
    
    if fig:
        # Streamlit用のPlotly描画（幅を100%にする）
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("表示できるデータがありません。")

# ==========================================
# サマリー表示タブ
# ==========================================
st.markdown("---")
st.subheader("📋 分析サマリー")
st.markdown("全体相場の動向や特定のテーマに対する資金シフト、モメンタムをリアルタイムに分類して表示します。")

tab1, tab2, tab3, tab4 = st.tabs([
    "🌍 地政学・逆行高テーマ", 
    "📈 強いモメンタム (買われている)", 
    "📉 リバウンド期待 (売られすぎ)", 
    "🔥 出来高急増 (注目)"
])

with tab1:
    st.markdown("**【有事セクター・資金シフト監視】**")
    st.caption("防衛・原油・海運など、地政学リスク発生時に資金が向かいやすいセクターの今日の動向一覧です。（出来高変化率が高い順）")
    
    # 登録されているテーマ株をすべて表示
    theme_tickers = list(ss.GEOPOLITICAL_THEMES.keys())
    theme_df = df_results[df_results['Ticker'].isin(theme_tickers)].copy()
    if not theme_df.empty:
        st.dataframe(theme_df[['Company', 'Sector', 'Close', 'Daily_Return_%', 'Volume_Surge', 'Reasons']].sort_values(by='Volume_Surge', ascending=False), use_container_width=True)
        
    st.markdown("**【市場全体の資金流入トップ10】**")
    st.caption("日経225全体の中で、今日最も通常より多くの資金（出来高）が集まり、株価が上昇している銘柄です。")
    fund_shift_df = df_results[df_results['Daily_Return_%'] > 0].sort_values(by='Volume_Surge', ascending=False).head(10)
    st.dataframe(fund_shift_df[['Company', 'Sector', 'Close', 'Daily_Return_%', 'Volume_Surge', 'Reasons']], use_container_width=True)

with tab2:
    st.markdown("**【モメンタム上位トップ10】**")
    st.caption("MACDがプラス圏（または直近ゴールデンクロス）など、勢いが強い銘柄の中で出来高が伴っているランキングです。")
    trending = df_results[df_results['MACD'] > 0].sort_values(by='Volume_Surge', ascending=False).head(10)
    if not trending.empty:
        st.dataframe(trending[['Company', 'Sector', 'Close', 'Daily_Return_%', 'Volume_Surge', 'RSI_14', 'MACD', 'Reasons']], use_container_width=True)
    else:
        st.info("現在該当する銘柄はありません。（市場全体が軟調な可能性があります）")

with tab3:
    st.markdown("**【売られすぎ（リバウンド期待）トップ10】**")
    st.caption("RSIが低く、短期的に売られすぎている水準（買い時を探る）の銘柄ランキングです。")
    oversold = df_results.sort_values(by='RSI_14', ascending=True).head(10)
    st.dataframe(oversold[['Company', 'Sector', 'Close', 'Daily_Return_%', 'RSI_14', 'BB_Lower_2', 'Volume_Surge', 'Reasons']], use_container_width=True)

with tab4:
    st.markdown("**【出来高急変トップ10】**")
    st.caption("過去20日間の平均出来高に対して、本日の出来高が最も急変（急増）している銘柄ランキングです。")
    spiking = df_results.sort_values(by='Volume_Surge', ascending=False).head(10)
    st.dataframe(spiking[['Company', 'Sector', 'Close', 'Daily_Return_%', 'Volume_Surge', 'Volume', 'RSI_14', 'Reasons']], use_container_width=True)
