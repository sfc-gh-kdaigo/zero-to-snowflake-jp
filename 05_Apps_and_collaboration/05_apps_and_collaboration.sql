/***************************************************************************************************       
アセット:      Zero to Snowflake - アプリとコラボレーション
バージョン:    v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

アプリとコラボレーション
1. Snowflake Marketplaceからの天気データの取得
2. アカウントデータとWeather Sourceデータの統合
3. Safegraph POIデータの探索
4. SnowflakeでのStreamlitの紹介

****************************************************************************************************/

-- まず、セッションのクエリタグを設定します
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "apps_and_collaboration"}}';

-- 次に、ワークシートのコンテキストを設定します
USE DATABASE tb_101;
USE ROLE accountadmin;
USE WAREHOUSE tb_de_wh;

/*  1. Snowflake Marketplaceからの天気データの取得
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-sharing-intro
    ***********************************************************
    ジュニアアナリストの1人であるBenは、天気が米国のフードトラックの売上にどのように影響するかについて、
    より良いインサイトを得たいと考えています。これを行うために、Snowflake Marketplaceを使用して
    天気データをアカウントに追加し、独自のTastyBytesデータに対してクエリして、まったく新しい
    インサイトを発見します。
    
    Snowflake Marketplaceは、さまざまなサードパーティのデータ、アプリケーション、AI製品を
    発見してアクセスできる集中ハブを提供します。この安全なデータ共有により、複製なしで
    ライブですぐにクエリ可能なデータにアクセスできます。
    
    Weather Sourceデータを取得する手順:
    1. アカウントレベルからaccountadminを使用していることを確認します（左下隅を確認）
    2. ナビゲーションメニューから「Marketplace」ページに移動します。必要に応じて、このページを新しいブラウザタブで開くことができます
    3. 検索バーに「Weather Source frostbyte」と入力します
    4. 「Weather Source LLC: frostbyte」リストを選択して「取得」をクリックします
    5. 「Options」をクリックしてオプションセクションを展開します
    6. データベース名を「 ZTS_WEATHERSOURCE 」に変更します
    7. 「PUBLIC」にアクセスを許可します
    8. 「取得」を押します
    
    このプロセスにより、Weather Sourceデータにほぼ即座にアクセスできます。従来のデータ複製と
    パイプラインの必要性を排除することで、アナリストはビジネス上の質問から実用的な分析に
    直接移行できます。
    
    天気データがアカウントに追加されたので、TastyBytesアナリストは既存のロケーションデータと
    即座に結合を開始できます。
*/

-- アナリストロールに切り替えます
USE ROLE tb_analyst;

/*  2. アカウントデータとWeather Sourceデータの統合

    生のロケーションデータをWeather Source共有のデータと調和させ始める前に、データ共有を直接クエリして、
    作業しているデータをより深く理解できます。まず、天気データで利用可能なすべての個別の都市のリストと、
    その特定の都市の天気メトリクスを取得することから始めます。
*/
SELECT 
    DISTINCT city_name,
    AVG(max_wind_speed_100m_mph) AS avg_wind_speed_mph,
    AVG(avg_temperature_air_2m_f) AS avg_temp_f,
    AVG(tot_precipitation_in) AS avg_precipitation_in,
    MAX(tot_snowfall_in) AS max_snowfall_in
FROM zts_weathersource.onpoint_id.history_day
WHERE country = 'US'
GROUP BY city_name;

-- 次に、生の国データをWeather Sourceデータ共有の履歴日次天気データと結合するビューを作成しましょう
CREATE OR REPLACE VIEW harmonized.daily_weather_v
COMMENT = 'Tasty Bytesがサポートする都市にフィルタリングされたWeather Source日次履歴'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM zts_weathersource.onpoint_id.history_day hd
JOIN zts_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;

/*
    日次天気履歴ビューを使用して、Benは2022年2月のハンブルクの平均日次天気温度を見つけ、
    それを折れ線グラフとして可視化したいと考えています。

    結果ペインで「チャート」をクリックして、結果をグラフィカルに視覚化します。チャートビューで、
    左側の「チャート型」と書かれたセクションで、次の設定を構成します:
    
        チャート型: 折れ線グラフ | X軸: DATE_VALID_STD | Y軸: AVERAGE_TEMP_F
*/
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS average_temp_f
FROM harmonized.daily_weather_v dw
WHERE dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = 2022
    AND MONTH(date_valid_std) = 2 -- 2月
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;

/*
    日次天気ビューは素晴らしい働きをしています！さらに一歩進めて、注文ビューと日次天気ビューを
    天気別の日次売上ビューと組み合わせましょう。これにより、売上と天気条件の間の傾向と関係を
    発見できます。
*/
CREATE OR REPLACE VIEW analytics.daily_sales_by_weather_v
COMMENT = '日次天気メトリクスと注文データ'
AS
WITH daily_orders_aggregated AS (
    SELECT
        DATE(o.order_ts) AS order_date,
        o.primary_city,
        o.country,
        o.menu_item_name,
        SUM(o.price) AS total_sales
    FROM
        harmonized.orders_v o
    GROUP BY ALL
)
SELECT
    dw.date_valid_std AS date,
    dw.city_name,
    dw.country_desc,
    ZEROIFNULL(doa.total_sales) AS daily_sales,
    doa.menu_item_name,
    ROUND(dw.avg_temperature_air_2m_f, 2) AS avg_temp_fahrenheit,
    ROUND(dw.tot_precipitation_in, 2) AS avg_precipitation_inches,
    ROUND(dw.tot_snowdepth_in, 2) AS avg_snowdepth_inches,
    dw.max_wind_speed_100m_mph AS max_wind_speed_mph
FROM
    harmonized.daily_weather_v dw
LEFT JOIN
    daily_orders_aggregated doa
    ON dw.date_valid_std = doa.order_date
    AND dw.city_name = doa.primary_city
    AND dw.country_desc = doa.country
ORDER BY 
    date ASC;

/*
    Benは、天気別の日次売上ビューを使用して、天気が売上にどのように影響するか（これまで探索されていなかった関係）を
    明らかにし、次のような質問に答え始めることができます: シアトル市場で大雨が売上高にどのように影響するか？

    チャート型: 棒グラフ | X軸: MENU_ITEM_NAME | Y軸: DAILY_SALES
*/
SELECT * EXCLUDE (city_name, country_desc, avg_snowdepth_inches, max_wind_speed_mph)
FROM analytics.daily_sales_by_weather_v
WHERE 
    country_desc = 'United States'
    AND city_name = 'Seattle'
    AND avg_precipitation_inches >= 1.0
ORDER BY date ASC;

/*  3. Safegraph POIデータの探索

    Benは、フードトラックのロケーションでの天気条件についてさらに詳しく知りたいと考えています。
    幸いなことに、SafegraphはSnowflake Marketplaceで無料のPoint-of-Interestデータを提供しています。
    
    このデータリストを使用するには、先ほどの天気データと同様の手順に従います:
        1. アカウントレベルからaccountadminを使用していることを確認します（左下隅を確認）
        2. ナビゲーションメニューから「Marketplace」ページに移動します。必要に応じて、このページを新しいブラウザタブで開くことができます
        3. 検索バーに「safegraph frostbyte」と入力します
        4. 「Safegraph: frostbyte」リストを選択して「取得」をクリックします
        5. 「Options」をクリックしてオプションセクションを展開します
        6. データベース名を「 ZTS_SAFEGRAPH 」に変更します
        7. 「PUBLIC」にアクセスを許可します
        8. 「取得」を押します
    
    SafegraphのPOIデータをFrostbyteのような天気データセットと独自の内部`orders_v`テーブルと結合することで、
    高リスクのロケーションを特定し、外部要因からの財務的影響を定量化できます。
*/
CREATE OR REPLACE VIEW harmonized.tastybytes_poi_v
AS 
SELECT 
    l.location_id,
    sg.postal_code,
    sg.country,
    sg.city,
    sg.iso_country_code,
    sg.location_name,
    sg.top_category,
    sg.category_tags,
    sg.includes_parking_lot,
    sg.open_hours
FROM raw_pos.location l
JOIN zts_safegraph.public.frostbyte_tb_safegraph_s sg 
    ON l.location_id = sg.location_id
    AND l.iso_country_code = sg.iso_country_code;

-- 次に、POIデータを天気データに対してクエリして、2022年の米国で平均して最も風の強い上位3つのロケーションを見つけましょう
SELECT TOP 3
    p.location_id,
    p.city,
    p.postal_code,
    AVG(hd.max_wind_speed_100m_mph) AS average_wind_speed
FROM harmonized.tastybytes_poi_v AS p
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON p.postal_code = hd.postal_code
WHERE
    p.country = 'United States'
    AND YEAR(hd.date_valid_std) = 2022
GROUP BY p.location_id, p.city, p.postal_code
ORDER BY average_wind_speed DESC;

/*
    前のクエリのlocation_idを使用して、異なる天気条件下での売上パフォーマンスを直接比較したいと考えています。
    共通テーブル式（CTE）を使用すると、上記のクエリをサブクエリとして使用して、平均風速が最も高い
    上位3つのロケーションを見つけ、それらの特定のロケーションの売上データを分析できます。
    共通テーブル式は、複雑なクエリをさまざまな小さなクエリに分割して、可読性とパフォーマンスを向上させるのに役立ちます。
    
    各トラックブランドの売上データを2つのバケットに分割します: 1日の最大風速が20 mph以下だった「穏やかな」日と、
    20 mphを超えた「風の強い」日です。

    これのビジネスへの影響は、ブランドの天候耐性を特定することです。これらの売上高を並べて見ることで、
    どのブランドが「天候に強い」か、どのブランドが強風で売上が大幅に減少するかを即座に見つけることができます。
    これにより、脆弱なブランドのための「Wind Day」プロモーションの実施、在庫の調整、または
    ブランドのメニューをロケーションの典型的な天候により適合させるための将来のトラック配備戦略の
    情報提供など、より適切な情報に基づいた運用上の決定が可能になります。
*/
WITH TopWindiestLocations AS (
    SELECT TOP 3
        p.location_id
    FROM harmonized.tastybytes_poi_v AS p
    JOIN
        zts_weathersource.onpoint_id.history_day AS hd
        ON p.postal_code = hd.postal_code
    WHERE
        p.country = 'United States'
        AND YEAR(hd.date_valid_std) = 2022
    GROUP BY p.location_id, p.city, p.postal_code
    ORDER BY AVG(hd.max_wind_speed_100m_mph) DESC
)
SELECT
    o.truck_brand_name,
    ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph <= 20 THEN o.order_total END),
    2) AS avg_sales_calm_days,
    ZEROIFNULL(ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph > 20 THEN o.order_total END),
    2)) AS avg_sales_windy_days
FROM analytics.orders_v AS o
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON o.primary_city = hd.city_name
    AND DATE(o.order_ts) = hd.date_valid_std
WHERE o.location_id IN (SELECT location_id FROM TopWindiestLocations)
GROUP BY o.truck_brand_name
ORDER BY o.truck_brand_name;

/*----------------------------------------------------------------------------------
 リセットスクリプト
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- ビューを削除
DROP VIEW IF EXISTS harmonized.daily_weather_v;
DROP VIEW IF EXISTS analytics.daily_sales_by_weather_v;
DROP VIEW IF EXISTS harmonized.tastybytes_poi_v;

-- クエリタグの設定を解除
ALTER SESSION UNSET query_tag;
-- ウェアハウスを一時停止
ALTER WAREHOUSE tb_de_wh SUSPEND;