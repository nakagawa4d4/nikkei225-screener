import yfinance as yf
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# 設定
# ==========================================
# スクリーニング対象とする銘柄のティッカーリスト (Yahoo Finance形式: 証券コード.T) と会社名のマッピング
# ここでは日経225構成銘柄を設定します。
TICKERS = {
    "1332.T": "ニッスイ", "1333.T": "マルハニチロ", "1605.T": "INPEX", "1721.T": "コムシスHD", 
    "1801.T": "大成建設", "1802.T": "大林組", "1803.T": "清水建設", "1808.T": "長谷工コーポ",
    "1812.T": "鹿島建設", "1925.T": "大和ハウス", "1928.T": "積水ハウス", "2002.T": "日清製粉G",
    "2269.T": "明治HD", "2282.T": "日本ハム", "2413.T": "エムスリー", "2432.T": "DeNA",
    "2501.T": "サッポロHD", "2502.T": "アサヒGHD", "2503.T": "キリンHD", "2531.T": "宝HD",
    "2768.T": "双日", "2801.T": "キッコーマン", "2802.T": "味の素", "2871.T": "ニチレイ",
    "2914.T": "JT", "3086.T": "J.フロント", "3099.T": "三越伊勢丹", "3141.T": "ウエルシア",
    "3289.T": "東急不動産HD", "3382.T": "セブン&アイHD", "3401.T": "帝人", "3402.T": "東レ",
    "3405.T": "クラレ", "3407.T": "旭化成", "3436.T": "SUMCO", "3659.T": "ネクソン",
    "3861.T": "王子HD", "3863.T": "日本製紙", "4004.T": "レゾナックHD", "4005.T": "住友化学",
    "4021.T": "日産化学", "4041.T": "日本曹達", "4042.T": "東ソー", "4061.T": "デンカ",
    "4063.T": "信越化学", "4151.T": "協和キリン", "4183.T": "三井化学", "4188.T": "三菱ケミカルG",
    "4208.T": "UBE", "4324.T": "電通グループ", "4385.T": "メルカリ", "4452.T": "花王",
    "4502.T": "武田薬品", "4503.T": "アステラス製薬", "4506.T": "住友ファーマ", "4507.T": "塩野義製薬",
    "4519.T": "中外製薬", "4523.T": "エーザイ", "4543.T": "テルモ", "4568.T": "第一三共",
    "4578.T": "大塚HD", "4631.T": "DIC", "4661.T": "オリエンタルランド", "4689.T": "LINEヤフー",
    "4704.T": "トレンドマイクロ", "4736.T": "日本新薬", "4751.T": "サイバーエージェント", "4755.T": "楽天G",
    "4901.T": "富士フイルム", "4911.T": "資生堂", "4912.T": "ライオン", "5019.T": "出光興産",
    "5020.T": "ENEOS", "5101.T": "横浜ゴム", "5108.T": "ブリヂストン", "5201.T": "AGC",
    "5202.T": "日本板硝子", "5214.T": "日本電気硝子", "5232.T": "住友大阪セメント", "5233.T": "太平洋セメント",
    "5301.T": "東海カーボン", "5332.T": "TOTO", "5333.T": "日本ガイシ", "5401.T": "日本製鉄",
    "5406.T": "神戸製鋼所", "5411.T": "JFE HD", "5541.T": "大平洋金属", "5631.T": "日本製鋼所",
    "5703.T": "日軽金HD", "5706.T": "三井金属", "5711.T": "三菱マテリアル", "5713.T": "住友金属鉱山",
    "5714.T": "DOWA HD", "5801.T": "古河電工", "5802.T": "住友電工", "5803.T": "フジクラ",
    "5831.T": "しずおかFG", "6098.T": "リクルートHD", "6103.T": "オークマ", "6118.T": "アマダ",
    "6146.T": "ディスコ", "6273.T": "SMC", "6301.T": "コマツ", "6302.T": "住友重機械",
    "6305.T": "日立建機", "6326.T": "クボタ", "6361.T": "荏原製作所", "6367.T": "ダイキン工業",
    "6432.T": "竹内製作所", "6471.T": "日本精工", "6472.T": "NTN", "6473.T": "ジェイテクト",
    "6501.T": "日立製作所", "6503.T": "三菱電機", "6504.T": "富士電機", "6506.T": "安川電機",
    "6526.T": "ソシオネクスト", "6594.T": "ニデック", "6645.T": "オムロン", "6701.T": "NEC",
    "6702.T": "富士通", "6723.T": "ルネサス", "6724.T": "エプソン", "6752.T": "パナソニック",
    "6758.T": "ソニーG", "6762.T": "TDK", "6770.T": "アルプスアルパイン", "6841.T": "横河電機",
    "6857.T": "アドバンテスト", "6861.T": "キーエンス", "6902.T": "デンソー", "6920.T": "レーザーテック",
    "6952.T": "カシオ計算機", "6954.T": "ファナック", "6971.T": "京セラ", "6976.T": "太陽誘電",
    "6981.T": "村田製作所", "6988.T": "日東電工", "7011.T": "三菱重工", "7012.T": "川崎重工",
    "7013.T": "IHI", "7186.T": "コンコルディア", "7201.T": "日産自動車", "7203.T": "トヨタ自動車",
    "7205.T": "日野自動車", "7211.T": "三菱自動車", "7259.T": "アイシン", "7261.T": "マツダ",
    "7267.T": "ホンダ", "7269.T": "スズキ", "7270.T": "SUBARU", "7272.T": "ヤマハ発動機",
    "7731.T": "ニコン", "7733.T": "オリンパス", "7735.T": "SCREEN HD", "7741.T": "HOYA",
    "7751.T": "キヤノン", "7752.T": "リコー", "7762.T": "シチズン時計", "7832.T": "バンダイナムコ",
    "7911.T": "TOPPAN HD", "7912.T": "大日本印刷", "7951.T": "ヤマハ", "7974.T": "任天堂",
    "8001.T": "伊藤忠商事", "8002.T": "丸紅", "8015.T": "豊田通商", "8031.T": "三井物産",
    "8035.T": "東京エレクトロン", "8053.T": "住友商事", "8058.T": "三菱商事", "8233.T": "高島屋",
    "8252.T": "丸井G", "8253.T": "クレディセゾン", "8267.T": "イオン", "8304.T": "あおぞら銀行",
    "8306.T": "三菱UFJ FG", "8308.T": "りそなHD", "8309.T": "三井住友トラスト", "8316.T": "三井住友FG",
    "8331.T": "千葉銀行", "8354.T": "ふくおかFG", "8411.T": "みずほFG", "8473.T": "SBI HD",
    "8591.T": "オリックス", "8601.T": "大和証券G", "8604.T": "野村HD", "8628.T": "松井証券",
    "8630.T": "SOMPO HD", "8697.T": "日本取引所G", "8725.T": "MS&AD", "8750.T": "第一生命HD",
    "8766.T": "東京海上HD", "8795.T": "T&D HD", "8801.T": "三井不動産", "8802.T": "三菱地所",
    "8804.T": "東京建物", "8830.T": "住友不動産", "8905.T": "イオンモール", "9001.T": "東武鉄道",
    "9005.T": "東急", "9007.T": "小田急電鉄", "9008.T": "京王電鉄", "9009.T": "京成電鉄",
    "9020.T": "JR東日本", "9021.T": "JR西日本", "9022.T": "JR東海", "9064.T": "ヤマトHD",
    "9101.T": "日本郵船", "9104.T": "商船三井", "9107.T": "川崎汽船", "9147.T": "NIPPON EXPRESS",
    "9201.T": "日本航空", "9202.T": "ANA HD", "9301.T": "三菱倉庫", "9432.T": "NTT",
    "9433.T": "KDDI", "9434.T": "ソフトバンク", "9501.T": "東京電力HD", "9502.T": "中部電力",
    "9503.T": "関西電力", "9531.T": "東京ガス", "9532.T": "大阪ガス", "9602.T": "東宝",
    "9613.T": "NTTデータG", "9735.T": "セコム", "9766.T": "コナミG", "9843.T": "ニトリHD",
    "9983.T": "ファーストリテイリング", "9984.T": "ソフトバンクG"
}

# 業種マッピング


# 業種マッピング
SECTORS = {
    "1332.T": "水産・農林業", "1333.T": "水産・農林業", "1605.T": "鉱業", "1721.T": "建設業",
    "1801.T": "建設業", "1802.T": "建設業", "1803.T": "建設業", "1808.T": "建設業",
    "1812.T": "建設業", "1925.T": "建設業", "1928.T": "建設業", "2002.T": "食料品",
    "2269.T": "食料品", "2282.T": "食料品", "2413.T": "サービス業", "2432.T": "サービス業",
    "2501.T": "食料品", "2502.T": "食料品", "2503.T": "食料品", "2531.T": "食料品",
    "2768.T": "卸売業", "2801.T": "食料品", "2802.T": "食料品", "2871.T": "食料品",
    "2914.T": "食料品", "3086.T": "小売業", "3099.T": "小売業", "3141.T": "小売業",
    "3289.T": "不動産業", "3382.T": "小売業", "3401.T": "繊維製品", "3402.T": "繊維製品",
    "3405.T": "化学", "3407.T": "化学", "3436.T": "金属製品", "3659.T": "情報・通信業",
    "3861.T": "パルプ・紙", "3863.T": "パルプ・紙", "4004.T": "化学", "4005.T": "化学",
    "4021.T": "化学", "4041.T": "化学", "4042.T": "化学", "4061.T": "化学",
    "4063.T": "化学", "4151.T": "医薬品", "4183.T": "化学", "4188.T": "化学",
    "4208.T": "化学", "4324.T": "サービス業", "4385.T": "情報・通信業", "4452.T": "化学",
    "4502.T": "医薬品", "4503.T": "医薬品", "4506.T": "医薬品", "4507.T": "医薬品",
    "4519.T": "医薬品", "4523.T": "医薬品", "4543.T": "精密機器", "4568.T": "医薬品",
    "4578.T": "医薬品", "4631.T": "化学", "4661.T": "サービス業", "4689.T": "情報・通信業",
    "4704.T": "情報・通信業", "4736.T": "医薬品", "4751.T": "サービス業", "4755.T": "サービス業",
    "4901.T": "化学", "4911.T": "化学", "4912.T": "化学", "5019.T": "石油・石炭製品",
    "5020.T": "石油・石炭製品", "5101.T": "ゴム製品", "5108.T": "ゴム製品", "5201.T": "ガラス・土石製品",
    "5202.T": "ガラス・土石製品", "5214.T": "ガラス・土石製品", "5232.T": "ガラス・土石製品", "5233.T": "ガラス・土石製品",
    "5301.T": "ガラス・土石製品", "5332.T": "ガラス・土石製品", "5333.T": "ガラス・土石製品", "5401.T": "鉄鋼",
    "5406.T": "鉄鋼", "5411.T": "鉄鋼", "5541.T": "鉄鋼", "5631.T": "鉄鋼",
    "5703.T": "非鉄金属", "5706.T": "非鉄金属", "5711.T": "非鉄金属", "5713.T": "非鉄金属",
    "5714.T": "非鉄金属", "5801.T": "非鉄金属", "5802.T": "非鉄金属", "5803.T": "非鉄金属",
    "5831.T": "銀行業", "6098.T": "サービス業", "6103.T": "機械", "6118.T": "機械",
    "6146.T": "機械", "6273.T": "機械", "6301.T": "機械", "6302.T": "機械",
    "6305.T": "機械", "6326.T": "機械", "6361.T": "機械", "6367.T": "機械",
    "6432.T": "機械", "6471.T": "機械", "6472.T": "機械", "6473.T": "機械",
    "6501.T": "電気機器", "6503.T": "電気機器", "6504.T": "電気機器", "6506.T": "電気機器",
    "6526.T": "電気機器", "6594.T": "電気機器", "6645.T": "電気機器", "6701.T": "電気機器",
    "6702.T": "電気機器", "6723.T": "電気機器", "6724.T": "電気機器", "6752.T": "電気機器",
    "6758.T": "電気機器", "6762.T": "電気機器", "6770.T": "電気機器", "6841.T": "電気機器",
    "6857.T": "電気機器", "6861.T": "電気機器", "6902.T": "輸送用機器", "6920.T": "電気機器",
    "6952.T": "電気機器", "6954.T": "電気機器", "6971.T": "電気機器", "6976.T": "電気機器",
    "6981.T": "電気機器", "6988.T": "化学", "7011.T": "機械", "7012.T": "輸送用機器",
    "7013.T": "機械", "7186.T": "銀行業", "7201.T": "輸送用機器", "7203.T": "輸送用機器",
    "7205.T": "輸送用機器", "7211.T": "輸送用機器", "7259.T": "輸送用機器", "7261.T": "輸送用機器",
    "7267.T": "輸送用機器", "7269.T": "輸送用機器", "7270.T": "輸送用機器", "7272.T": "輸送用機器",
    "7731.T": "精密機器", "7733.T": "精密機器", "7735.T": "電気機器", "7741.T": "精密機器",
    "7751.T": "電気機器", "7752.T": "電気機器", "7762.T": "精密機器", "7832.T": "その他製品",
    "7911.T": "その他製品", "7912.T": "その他製品", "7951.T": "その他製品", "7974.T": "その他製品",
    "8001.T": "卸売業", "8002.T": "卸売業", "8015.T": "卸売業", "8031.T": "卸売業",
    "8035.T": "電気機器", "8053.T": "卸売業", "8058.T": "卸売業", "8233.T": "小売業",
    "8252.T": "小売業", "8253.T": "その他金融業", "8267.T": "小売業", "8304.T": "銀行業",
    "8306.T": "銀行業", "8308.T": "銀行業", "8309.T": "銀行業", "8316.T": "銀行業",
    "8331.T": "銀行業", "8354.T": "銀行業", "8411.T": "銀行業", "8473.T": "証券、商品先物取引業",
    "8591.T": "その他金融業", "8601.T": "証券、商品先物取引業", "8604.T": "証券、商品先物取引業", "8628.T": "証券、商品先物取引業",
    "8630.T": "保険業", "8697.T": "その他金融業", "8725.T": "保険業", "8750.T": "保険業",
    "8766.T": "保険業", "8795.T": "保険業", "8801.T": "不動産業", "8802.T": "不動産業",
    "8804.T": "不動産業", "8830.T": "不動産業", "8905.T": "不動産業", "9001.T": "陸運業",
    "9005.T": "陸運業", "9007.T": "陸運業", "9008.T": "陸運業", "9009.T": "陸運業",
    "9020.T": "陸運業", "9021.T": "陸運業", "9022.T": "陸運業", "9064.T": "陸運業",
    "9101.T": "海運業", "9104.T": "海運業", "9107.T": "海運業", "9147.T": "陸運業",
    "9201.T": "空運業", "9202.T": "空運業", "9301.T": "倉庫・運輸関連業", "9432.T": "情報・通信業",
    "9433.T": "情報・通信業", "9434.T": "情報・通信業", "9501.T": "電気・ガス業", "9502.T": "電気・ガス業",
    "9503.T": "電気・ガス業", "9531.T": "電気・ガス業", "9532.T": "電気・ガス業", "9602.T": "情報・通信業",
    "9613.T": "情報・通信業", "9735.T": "サービス業", "9766.T": "情報・通信業", "9843.T": "小売業",
    "9983.T": "小売業", "9984.T": "情報・通信業"
}

# 地政学リスク・テーマ関連銘柄マッピング（有事の際に資金が向かいやすいセクター）
GEOPOLITICAL_THEMES = {
    "1605.T": "原油・資源", "5019.T": "原油・資源", "5020.T": "原油・資源",
    "9101.T": "海運(運賃高騰)", "9104.T": "海運(運賃高騰)", "9107.T": "海運(運賃高騰)",
    "7011.T": "防衛関連", "7012.T": "防衛関連", "7013.T": "防衛関連"
}

# データ取得期間（日数）
LOOKBACK_DAYS = 180

# キャッシュディレクトリ
CACHE_DIR = "screener_cache"

# ==========================================
# データ取得 (リトライ・キャッシュ機構付き)
# ==========================================
def fetch_fundamentals(tickers: list[str]) -> dict:
    """
    指定されたティッカーリストに対するファンダメンタル指標（PER, PBR, ROE, 配当利回りなど）を並列取得する。
    """
    fund_data = {}
    print(f"ファンダメンタル指標（PER, PBR, ROEなど）を取得します...")

    def fetch_single(ticker):
        try:
            info = yf.Ticker(ticker).info
            return ticker, {
                "PER": info.get("trailingPE"),
                "PBR": info.get("priceToBook"),
                "ROE": info.get("returnOnEquity"),
                "DivYield_Pct": info.get("dividendYield", 0) * 100 if info.get("dividendYield") else None
            }
        except Exception:
            return ticker, {"PER": None, "PBR": None, "ROE": None, "DivYield_Pct": None}

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_single, tickers)
        for ticker, data in results:
            fund_data[ticker] = data

    return fund_data

def fetch_stock_data(tickers: list[str], lookback_days: int) -> dict:
    """
    指定されたティッカーの株価データを取得する。
    HTTP 429エラー等を回避するため、Exponential Backoffによるリトライを実装。
    戻り値は { 'ticker': {'df': DataFrame, 'fundamentals': dict} } の形式。
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=lookback_days)
    
    fetch_list = tickers.copy()
    if "^N225" not in fetch_list:
        fetch_list.append("^N225")
        
    # yfinanceのdownloadメソッドで一括取得（スレッド処理されるため効率的だが制限に注意）
    print(f"{len(fetch_list)} 銘柄のデータを取得します ({start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')})...")
    
    max_retries = 3
    base_wait = 2
    
    for attempt in range(max_retries):
        try:
            # group_by='ticker' でティッカーごとにデータを分ける
            df_all = yf.download(tickers, start=start_date, end=end_date, group_by='ticker', threads=True)
            
            data_dict = {}
            if len(fetch_list) == 1:
                # 1銘柄の場合の特別処理（マルチインデックスにならないため）
                data_dict[fetch_list[0]] = df_all
            else:
                for ticker in fetch_list:
                    if ticker in df_all:
                        df_ticker = df_all[ticker].dropna(how='all')
                        if not df_ticker.empty:
                            data_dict[ticker] = df_ticker
                        else:
                            print(f"Warning: {ticker} のデータが空です。")
                    else:
                        print(f"Warning: {ticker} のデータが取得できませんでした。")
            
            
            # 成功したらファンダメンタルデータも取得して結合
            fundamentals = fetch_fundamentals(list(data_dict.keys()))
            
            final_data = {}
            for t, df in data_dict.items():
                final_data[t] = {
                    'df': df,
                    'fundamentals': fundamentals.get(t, {"PER": None, "PBR": None, "ROE": None, "DivYield_Pct": None})
                }
            
            print(f"データ取得完了 ({len(final_data)}/{len(tickers)} 銘柄)")
            return final_data
            
        except Exception as e:
            wait_time = base_wait * (2 ** attempt)
            print(f"データ取得エラー: {e}")
            if attempt < max_retries - 1:
                print(f"{wait_time}秒後にリトライします... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print("最大リトライ回数に達しました。")
                raise

    return {}

# ==========================================
# 指標計算
# ==========================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    株価データに対してテクニカル指標を計算し、カラムを追加する。
    """
    df = df.copy()
    
    # 欠損値の前方補完（休場日などの影響を軽減）
    df = df.ffill()
    
    # 前日比騰落率 (%)
    df['Daily_Return_%'] = df['Close'].pct_change() * 100
    
    # 1. 20日単純移動平均 (SMA 20)
    # 終値の過去20日間の平均値
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    
    # 2. 価格と20日SMAの乖離率 (%)
    # (価格 - SMA20) / SMA20 * 100
    df['SMA_20_Deviation'] = (df['Close'] - df['SMA_20']) / df['SMA_20'] * 100
    
    # 3. ボリンジャーバンド (20日, ±2σ)
    # 価格の変動幅（標準偏差）を利用した指標
    std_20 = df['Close'].rolling(window=20).std()
    df['BB_Upper_2'] = df['SMA_20'] + (std_20 * 2)
    df['BB_Lower_2'] = df['SMA_20'] - (std_20 * 2)
    
    # 4. RSI (14日) Relative Strength Index
    # 値上がり幅と値下がり幅から買われすぎ・売られすぎを判断するオシレーター
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    
    rs = avg_gain / avg_loss
    df['RSI_14'] = 100 - (100 / (1 + rs))
    
    # 5. 出来高20日単純移動平均
    df['Volume_SMA_20'] = df['Volume'].rolling(window=20).mean()
    
    # 6. 出来高変化率 (Volume Surge)
    # 当日の出来高が過去20日の平均に対して何倍になっているか
    df['Volume_Surge_Ratio'] = df['Volume'] / df['Volume_SMA_20']
    
    # 7. MACD (Moving Average Convergence Divergence)
    # トレンドの方向性や転換点を測る指標 (12日EMA - 26日EMA)
    ema_12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema_12 - ema_26
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    
    # MACDヒストグラム（MACDとシグナルの差）
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
    
    return df

# ==========================================
# スクリーニング実行
# ==========================================
def screen_stocks(data_dict: dict) -> pd.DataFrame:
    """
    計算済みの指標から最新日のデータを抽出し、スクリーニング条件に合致するか判定する。
    """
    latest_data = []
    
    # 日経平均のデータを取り出して地合いを判定
    nikkei_df_info = data_dict.get('^N225')
    is_market_down = False
    if nikkei_df_info is not None and 'df' in nikkei_df_info:
        nikkei_df = nikkei_df_info['df']
        if not nikkei_df.empty and len(nikkei_df) >= 2:
            n_prev = nikkei_df['Close'].iloc[-2]
            n_curr = nikkei_df['Close'].iloc[-1]
            nikkei_return = (n_curr - n_prev) / n_prev * 100
            is_market_down = (nikkei_return < -0.5) # 0.5%以上の下落を地合い悪化とみなす
    
    for ticker, info_dict in data_dict.items():
        if ticker == '^N225':
            continue # 日経平均自体はスルー表示しない
            
        df = info_dict['df']
        funds = info_dict['fundamentals']
        if df.empty or len(df) < 20: # 少なくとも20日分のデータがないと指標計算不可
            continue
            
        # 指標計算
        df_ind = calculate_indicators(df)
        
        # 最新の1営業日（直近の取引日）のデータを取得
        latest_row = df_ind.iloc[-1].copy()
        
        # NaN が含まれる場合はスキップ（上場直後の銘柄など）
        if pd.isna(latest_row['RSI_14']) or pd.isna(latest_row['Volume_Surge_Ratio']):
            continue
            
        # ファンダメンタルデータの取得
        per = funds.get('PER')
        pbr = funds.get('PBR')
        roe_dec = funds.get('ROE')
        roe = roe_dec * 100 if roe_dec is not None else None
        div_yield = funds.get('DivYield_Pct')
        
        record = {
            'Ticker': ticker,
            'Date': df_ind.index[-1].strftime('%Y-%m-%d'),
            'Close': latest_row['Close'],
            'Daily_Return_%': np.round(latest_row['Daily_Return_%'], 2),
            'Volume': latest_row['Volume'],
            'SMA_20': latest_row['SMA_20'],
            'SMA_20_Dev_%': np.round(latest_row['SMA_20_Deviation'], 2),
            'RSI_14': np.round(latest_row['RSI_14'], 2),
            'Volume_Surge': np.round(latest_row['Volume_Surge_Ratio'], 2),
            'BB_Lower_2': np.round(latest_row['BB_Lower_2'], 2),
            'BB_Upper_2': np.round(latest_row['BB_Upper_2'], 2),
            'MACD': np.round(latest_row['MACD'], 2),
            'MACD_Hist': np.round(latest_row['MACD_Hist'], 2),
            'PER': per,
            'PBR': pbr,
            'ROE_%': roe,
            'Div_Yield_%': div_yield,
            'Reasons': []
        }
        
        # --- スクリーニング条件の判定フラグと理由付与 ---
        reasons = []
        
        # 1. 最近買われている銘柄 (Trending / Buying Pressure)
        # 条件：終値が20日SMAより上 かつ MACDがプラス圏（または上向き） かつ 出来高急増
        is_trending_up = (
            record['Close'] > record['SMA_20'] and 
            record['MACD'] > 0 and
            record['Volume_Surge'] >= 1.5
        )
        if is_trending_up:
            reasons.append("上昇トレンド＋出来高増")
            
        if record['MACD_Hist'] > 0 and df_ind['MACD_Hist'].iloc[-2] <= 0:
            reasons.append("MACDゴールデンクロス")
            is_trending_up = True
            
        record['Is_Buying_Pressure'] = is_trending_up
        
        # 2. 売られすぎの銘柄 (Oversold)
        is_oversold = False
        if record['RSI_14'] <= 30:
            reasons.append(f"RSI売られすぎ ({record['RSI_14']})")
            is_oversold = True
        if record['Close'] <= record['BB_Lower_2']:
            reasons.append("ボリンジャーバンド-2σ到達")
            is_oversold = True
            
        record['Is_Oversold'] = is_oversold
        
        # 3. 出来高急増（注目銘柄）
        is_volume_spike = record['Volume_Surge'] >= 2.5
        if is_volume_spike:
            reasons.append(f"出来高急変 ({record['Volume_Surge']}倍)")
            
        record['Is_Volume_Spike'] = is_volume_spike
        
        # 4. 追加: 買われすぎ (Overbought)
        if record['RSI_14'] >= 70:
            reasons.append(f"RSI買われすぎ ({record['RSI_14']})")
            
        # 5. 地政学リスク・テーマ株の資金シフト（防衛・海運・原油など）
        theme = GEOPOLITICAL_THEMES.get(ticker)
        if theme:
            if is_market_down and record['Daily_Return_%'] > 0 and record['Volume_Surge'] >= 1.2:
                reasons.append(f"有事テーマ買い({theme}逆行高)")
                is_trending_up = True
            elif record['Daily_Return_%'] >= 2.0 and record['Volume_Surge'] >= 1.5:
                reasons.append(f"テーマ資金流入({theme})")
                is_trending_up = True
                
        # 6. 一般的な逆行高（地合いが悪いのに強い銘柄）
        if is_market_down and record['Daily_Return_%'] >= 2.0 and record['Volume_Surge'] >= 1.5 and not theme:
            reasons.append(f"相場逆行高({record['Daily_Return_%']}%)")
            is_trending_up = True
            
        record['Is_Buying_Pressure'] = is_trending_up
        
        # 7. ファンダメンタルズ条件（バリュー/高収益）
        if pbr is not None and pbr < 1.0:
            reasons.append(f"割安(PBR {pbr:.2f}倍)")
        if per is not None and per > 0 and per < 15:
            reasons.append(f"低PER({per:.1f}倍)")
        if roe is not None and roe > 10:
            reasons.append(f"高ROE({roe:.1f}%)")
        if div_yield is not None and div_yield > 4.0:
            reasons.append(f"高配当({div_yield:.1f}%)")
        
        record['Reasons'] = ", ".join(reasons) if reasons else "特記事項なし"
        
        latest_data.append(record)
        
    df_results = pd.DataFrame(latest_data)
    return df_results

# ==========================================
# 可視化・ダッシュボード生成
# ==========================================
def generate_dashboard(df_results: pd.DataFrame):
    """
    Plotlyを用いて、スクリーニング結果の可視化ダッシュボード（Figureオブジェクト）を生成・返却する。
    散布図と注目銘柄の表を組み合わせて直感的にわかりやすく表示する。
    業種（Sector）ごとに色分けを行い、凡例のクリックで表示/非表示を切り替え可能にする。
    """
    if df_results.empty:
        return None
        
    # 乖離率の絶対値をバブルサイズ用に計算
    df_results['Bubble_Size'] = df_results['SMA_20_Dev_%'].abs() + 1 
    
    # 全体サマリー用のカテゴリ分け（状態ベース。色は業種に譲るため文字列だけ記録）
    def assign_state(row):
        if row['Is_Oversold']: return '売られすぎ (Oversold)'
        if row['Is_Buying_Pressure']: return '強いモメンタム (Trending)'
        if row['Is_Volume_Spike']: return '出来高急拡大 (Vol Spike)'
        if row['RSI_14'] >= 70: return '買われすぎ (Overbought)'
        return '通常 (Normal)'
        
    df_results['State'] = df_results.apply(assign_state, axis=1)
    
    # 注目銘柄のみを抽出
    df_highlighted = df_results[df_results['Reasons'] != "特記事項なし"].copy()
    
    # Subplotの作成 (上が散布図、下がテーブル構成)
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("ポジションマップ (業種別・RSI vs 出来高変化率)", "注目銘柄リストと選定理由"),
        vertical_spacing=0.1,
        specs=[[{"type": "scatter"}],
               [{"type": "table"}]]
    )
    
    # --- 上段: 散布図 (業種ごとにTraceを分けることで凡例トグル機能を実現) ---
    sectors = sorted(df_results['Sector'].unique())
    # 業種ごとに色を割り振る（Plotlyのデフォルトカラーシーケンスを利用）
    colors = px.colors.qualitative.Alphabet + px.colors.qualitative.Dark24
    
    for i, sector in enumerate(sectors):
        df_sec = df_results[df_results['Sector'] == sector]
        if df_sec.empty:
            continue
            
        fig.add_trace(go.Scatter(
            x=df_sec['Volume_Surge'],
            y=df_sec['RSI_14'],
            mode='markers+text',
            marker=dict(
                size=df_sec['Bubble_Size'] * 4,
                color=colors[i % len(colors)],
                line=dict(width=1, color='DarkSlateGrey')
            ),
            name=sector,  # 凡例に表示される業種名
            text=df_sec['Company'], # バブルの横に会社名を表示
            textposition="top center",
            hovertext=df_sec['Reasons'],
            customdata=np.column_stack((
                df_sec['State'], 
                df_sec['SMA_20_Dev_%'], 
                df_sec['Ticker']
            )),
            hovertemplate="<b>%{text}</b> (%{customdata[2]})<br>" +
                          "業種: %{name}<br>" +
                          "状態: %{customdata[0]}<br>" +
                          "出来高変化: %{x}倍<br>" +
                          "RSI: %{y}<br>" +
                          "乖離率: %{customdata[1]}%<br>" +
                          "フラグ理由: %{hovertext}<extra></extra>"
        ), row=1, col=1)

    # 基準線の追加
    fig.add_hline(y=30, line_dash="dash", line_color="blue", annotation_text="売られすぎ(30)", row=1, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="買われすぎ(70)", row=1, col=1)
    fig.add_vline(x=1.0, line_dash="dash", line_color="green", annotation_text="平均出来高(1x)", row=1, col=1)
    fig.add_vline(x=2.0, line_dash="dash", line_color="orange", annotation_text="出来高急拡大(2x)", row=1, col=1)

    # --- 下段: データテーブル ---
    fig.add_trace(
        go.Table(
            header=dict(
                values=["会社名", "業種", "終値", "RSI", "出来高変化", "選定の理由"],
                fill_color='paleturquoise',
                align='left',
                font=dict(size=12, color='black')
            ),
            cells=dict(
                values=[
                    df_highlighted['Company'],
                    df_highlighted['Sector'],
                    df_highlighted['Close'],
                    df_highlighted['RSI_14'],
                    df_highlighted['Volume_Surge'].astype(str) + "倍",
                    df_highlighted['Reasons']
                ],
                fill_color='lavender',
                align='left',
                font=dict(size=11, color='darkslategray')
            )
        ),
        row=2, col=1
    )

    # レイアウトの調整
    fig.update_layout(
        height=900,
        title_text="Nikkei225 スクリーニング",
        showlegend=True,
        template='plotly_white',
        hovermode="closest"
    )
    
    # 軸ラベルの設定
    fig.update_xaxes(title_text="出来高変化率 (対20日平均)", row=1, col=1)
    fig.update_yaxes(title_text="RSI (14日)", row=1, col=1)
    
    return fig

# ==========================================
# メイン処理
# ==========================================
def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    
    os.makedirs(CACHE_DIR, exist_ok=True)
    csv_output_path = os.path.join(CACHE_DIR, f"screener_results_{timestamp}.csv")
    html_output_path = f"screener_dashboard_{timestamp}.html"
    
    print("=== 日本株スクリーニングツール ===")
    
    # 1. データ取得
    data_dict = fetch_stock_data(list(TICKERS.keys()), LOOKBACK_DAYS)
    
    if not data_dict:
        print("データが取得できませんでした。終了します。")
        sys.exit(1)
        
    # 2. 指標計算とスクリーニング
    print("指標計算およびスクリーニングを実行中...")
    df_results = screen_stocks(data_dict)
    
    # ティッカーから会社名、業種をマッピング
    df_results['Company'] = df_results['Ticker'].map(TICKERS)
    df_results['Sector'] = df_results['Ticker'].map(SECTORS).fillna("Unknown")
    
    # 3. 結果の保存 (上書き防止のためタイムスタンプを付与)
    df_results.to_csv(csv_output_path, index=False)
    print(f"結果をCSVに保存しました: {csv_output_path}")
    
    # ターミナルへサマリーを出力
    print("\n--- スクリーニングサマリー ---")
    print(f"最近買われている（強いモメンタム）銘柄: \n {df_results[df_results['Is_Buying_Pressure']]['Company'].tolist()}")
    print(f"売られすぎの（リバウンド期待）銘柄: \n {df_results[df_results['Is_Oversold']]['Company'].tolist()}")
    print(f"出来高急増（注目）銘柄: \n {df_results[df_results['Is_Volume_Spike']]['Company'].tolist()}\n")
    
    # 4. 可視化・ダッシュボード出力 (CLI版としてローカル保存)
    fig = generate_dashboard(df_results)
    if fig:
        fig.write_html(html_output_path)
        print("ダッシュボードの出力が完了しました。")
        print("\nすべての処理が完了しました。")
        print(f"ブラウザで {html_output_path} を開いて可視化結果をご確認ください。")

if __name__ == "__main__":
    main()
