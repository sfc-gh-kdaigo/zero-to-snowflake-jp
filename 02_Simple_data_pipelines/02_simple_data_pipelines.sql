/***************************************************************************************************       
アセット:      Zero to Snowflake - シンプルなデータパイプライン
バージョン:    v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

シンプルなデータパイプライン
1. 外部ステージからの取り込み
2. 半構造化データとVARIANTデータ型
3. Dynamic Tables
4. Dynamic Tablesを使用したシンプルなパイプライン
5. 有向非巡回グラフ（DAG）によるパイプラインの可視化

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "data_pipeline"}}';

/*
    生のメニューデータでデータパイプラインを作成する意図でTastyBytesデータエンジニアの役割を担うので、
    コンテキストを適切に設定しましょう。
*/
USE DATABASE tb_101;
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;

/*  1. 外部ステージからの取り込み
    ***************************************************************
    SQLリファレンス:
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    ***************************************************************

    現在、データはAmazon S3バケットにCSV形式で存在しています。この生のCSVデータをステージに
    ロードして、作業用のステージングテーブルにCOPYできるようにする必要があります。
    
    Snowflakeでは、ステージはデータファイルが保存されている場所を指定する名前付きデータベース
    オブジェクトであり、テーブルへのデータのロードやアンロードを可能にします。

    ステージを作成する際に指定するもの:
    - データを取得するS3バケット
    - データを解析するファイル形式（この場合はCSV）
*/

-- メニューステージを作成
CREATE OR REPLACE STAGE raw_pos.menu_stage
COMMENT = 'メニューデータ用ステージ'
URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_pos/menu/'
FILE_FORMAT = public.csv_ff;

CREATE OR REPLACE TABLE raw_pos.menu_staging
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- ステージとテーブルが配置されたので、ステージから新しいmenu_stagingテーブルにデータをロードしましょう
COPY INTO raw_pos.menu_staging
FROM @raw_pos.menu_stage;

-- オプション: ロードが成功したことを確認
SELECT * FROM raw_pos.menu_staging;

/*  2. Snowflakeの半構造化データ
    *********************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured
    *********************************************************************
    
    SnowflakeはVARIANTデータ型を使用してJSONのような半構造化データの処理に優れています。
    このデータを自動的に解析、最適化、インデックス化し、ユーザーが標準SQLと特殊な関数を使用して
    簡単に抽出と分析を行えるようにします。Snowflakeは、JSON、Avro、ORC、Parquet、XMLなどの
    半構造化データ型をサポートしています。
    
    menu_item_health_metrics_obj列のVARIANTオブジェクトには、2つの主要なキーと値のペアが含まれています:
        - menu_item_id: アイテムの一意の識別子を表す数値
        - menu_item_health_metrics: 健康情報を詳述するオブジェクトを保持する配列
        
    menu_item_health_metrics配列内の各オブジェクトには:
        - 文字列の成分配列
        - 'Y'と'N'の文字列値を持ついくつかの食事フラグ
*/
SELECT menu_item_health_metrics_obj FROM raw_pos.menu_staging;

/*
    このクエリは、データの内部のJSON的な構造をナビゲートするための特別な構文を使用します。
    コロン演算子（:）はキー名でデータにアクセスし、角括弧（[]）は数値位置で配列から要素を選択します。
    これらの演算子を連鎖させて、ネストされたオブジェクトから成分リストを抽出できます。
    
    VARIANTオブジェクトから取得された要素は、VARIANT型のままです。
    これらの要素を既知のデータ型にキャストすると、クエリのパフォーマンスが向上し、データ品質が向上します。
    キャストを実現する2つの異なる方法があります:
        - CAST関数
        - 省略構文の使用: <source_expr> :: <target_data_type>

    以下は、メニューアイテム名、メニューアイテムID、必要な成分のリストを取得するために
    これらすべてのトピックを組み合わせたクエリです。
*/
SELECT
    menu_item_name,
    CAST(menu_item_health_metrics_obj:menu_item_id AS INTEGER) AS menu_item_id, -- 'AS'を使用したキャスト
    menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY AS ingredients -- 二重コロン（::）構文を使用したキャスト
FROM raw_pos.menu_staging;

/*
    半構造化データを操作する際に活用できるもう1つの強力な関数はFLATTENです。
    FLATTENを使用すると、JSONや配列などの半構造化データをアンラップし、
    指定されたオブジェクト内のすべての要素に対して行を生成できます。

    これを使用して、トラックで使用されているすべてのメニューからすべての成分のリストを取得できます。
*/
SELECT
    i.value::STRING AS ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i;

/*  3. Dynamic Tables
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/dynamic-tables-about
    **************************************************************
    
    すべての成分を構造化された形式で保存して、個別に簡単にクエリ、フィルタリング、分析できると
    便利です。しかし、フードトラックフランチャイズは常に新しくエキサイティングなメニューアイテムを
    メニューに追加しており、その多くはデータベースにまだない独自の成分を使用しています。
    
    これには、データ変換パイプラインを簡素化するために設計された強力なツールであるDynamic Tablesを
    使用できます。Dynamic Tablesは、いくつかの理由から、このユースケースに最適です:
        - 宣言的な構文を使用して作成され、データは指定されたクエリによって定義されます
        - 自動データ更新により、手動更新やカスタムスケジューリングを必要とせずにデータが新鮮に保たれます
        - Snowflake Dynamic Tablesによって管理されるデータの鮮度は、Dynamic Tableそのものだけでなく、
          それに依存する下流のデータオブジェクトにも拡張されます

    これらの機能を実際に確認するために、シンプルなDynamic Tableパイプラインを作成し、
    ステージングテーブルに新しいメニューアイテムを追加して自動更新を実証します。

    まず、成分用のDynamic Tableを作成します。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'
AS
    SELECT
    ingredient_name,
    menu_ids
FROM (
    SELECT DISTINCT
        i.value::STRING AS ingredient_name, -- 個別の成分値
        ARRAY_AGG(m.menu_item_id) AS menu_ids -- 成分が使用されているメニューIDの配列
    FROM
        raw_pos.menu_staging m,
        LATERAL FLATTEN(INPUT => menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i
    GROUP BY i.value::STRING
);

-- 成分Dynamic Tableが正常に作成されたことを確認しましょう
SELECT * FROM harmonized.ingredient;

/*
    サンドイッチトラックの1つであるBetter Off Breadが、新しいメニューアイテムであるバインミーサンドイッチを
    導入しました。このメニューアイテムには、いくつかの成分が含まれています: フレンチバゲット、マヨネーズ、
    ピクルス大根。
    
    Dynamic Tableの自動更新により、menu_stagingテーブルにこの新しいメニューアイテムを追加すると、
    ingredientテーブルに自動的に反映されます。
*/
INSERT INTO raw_pos.menu_staging 
SELECT 
    10101,
    15, -- トラックID
    'Sandwiches',
    'Better Off Bread', -- トラックブランド名
    157, -- メニューアイテムID
    'Banh Mi', -- メニューアイテム名
    'Main',
    'Cold Option',
    9.0,
    12.0,
    PARSE_JSON('{
      "menu_item_health_metrics": [
        {
          "ingredients": [
            "French Baguette",
            "Mayonnaise",
            "Pickled Daikon",
            "Cucumber",
            "Pork Belly"
          ],
          "is_dairy_free_flag": "N",
          "is_gluten_free_flag": "N",
          "is_healthy_flag": "Y",
          "is_nut_free_flag": "Y"
        }
      ],
      "menu_item_id": 157
    }'
);

/*
    フレンチバゲット、ピクルス大根が成分テーブルに表示されていることを確認します。
    「Query produced no results」と表示される場合があります。これは、Dynamic Tableがまだ更新されていないことを意味します。
    Dynamic TableのLAG設定が追いつくまで、最大1分待ちます。
*/

SELECT * FROM harmonized.ingredient 
WHERE ingredient_name IN ('French Baguette', 'Pickled Daikon');

/* 4. Dynamic Tablesを使用したシンプルなパイプライン

    それでは、成分からメニューへのルックアップDynamic Tableを作成しましょう。これにより、
    どのメニューアイテムが特定の成分を使用しているかを確認できます。その後、どのトラックが
    どの成分をどのくらい必要とするかを判断できます。
    このテーブルもDynamic Tableであるため、メニューステージングテーブルに追加された
    メニューアイテムで新しい成分が使用された場合、自動的に更新されます。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_to_menu_lookup
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'    
AS
SELECT
    i.ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) f
JOIN harmonized.ingredient i ON f.value::STRING = i.ingredient_name;

-- 成分からメニューへのルックアップが正常に作成されたことを確認
SELECT * 
FROM harmonized.ingredient_to_menu_lookup
ORDER BY menu_item_id;

/*
    次の2つのインサートクエリを実行して、2022年1月27日にトラック#15で2つのバインミーサンドイッチの
    注文をシミュレートします。その後、トラック別の成分使用量を示す別の下流Dynamic Tableを作成します。
*/
INSERT INTO raw_pos.order_header
SELECT 
    459520441, -- order_id
    15, -- truck_id
    1030, -- location id
    101565,
    null,
    200322900,
    TO_TIMESTAMP_NTZ('08:00:00', 'hh:mi:ss'),
    TO_TIMESTAMP_NTZ('14:00:00', 'hh:mi:ss'),
    null,
    TO_TIMESTAMP_NTZ('2022-01-27 08:21:08.000'), -- 注文タイムスタンプ
    null,
    'USD',
    14.00,
    null,
    null,
    14.00;
    
INSERT INTO raw_pos.order_detail
SELECT
    904745311, -- order_detail_id
    459520441, -- order_id
    157, -- menu_item_id
    null,
    0,
    2, -- 注文数量
    14.00,
    28.00,
    null;

/*
    次に、米国の個々のフードトラックによる各成分の月間使用量を要約する別のDynamic Tableを作成します。
    これにより、ビジネスは成分の消費を追跡できます。これは、在庫の最適化、コストの管理、
    メニュー計画とサプライヤー関係に関する情報に基づいた決定を行うために重要です。
    
    注文タイムスタンプから日付の一部を抽出するために使用される2つの異なる方法に注意してください:
      -> EXTRACT(<date part> FROM <datetime>)は、指定されたタイムスタンプから指定された日付部分を分離します。
      EXTRACT関数で使用できる日付と時刻の部分はいくつかあり、最も一般的なものは
      YEAR、MONTH、DAY、HOUR、MINUTE、SECONDです。
      -> MONTH(<datetime>)は1〜12の月のインデックスを返します。YEAR(<datetime>)とDAY(<datetime>)は、
      それぞれ年と日に対して同じことを行います。
*/

-- 次にテーブルを作成
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_usage_by_truck 
    LAG = '2 minute'
    WAREHOUSE = 'TB_DE_WH'  
    AS 
    SELECT
        oh.truck_id,
        EXTRACT(YEAR FROM oh.order_ts) AS order_year,
        MONTH(oh.order_ts) AS order_month,
        i.ingredient_name,
        SUM(od.quantity) AS total_ingredients_used
    FROM
        raw_pos.order_detail od
        JOIN raw_pos.order_header oh ON od.order_id = oh.order_id
        JOIN harmonized.ingredient_to_menu_lookup iml ON od.menu_item_id = iml.menu_item_id
        JOIN harmonized.ingredient i ON iml.ingredient_name = i.ingredient_name
        JOIN raw_pos.location l ON l.location_id = oh.location_id
    WHERE l.country = 'United States'
    GROUP BY
        oh.truck_id,
        order_year,
        order_month,
        i.ingredient_name
    ORDER BY
        oh.truck_id,
        total_ingredients_used DESC;
/*
    それでは、新しく作成したingredient_usage_by_truckビューを使用して、
    2022年1月のトラック#15の成分使用量を表示しましょう。
*/
SELECT
    truck_id,
    ingredient_name,
    SUM(total_ingredients_used) AS total_ingredients_used,
FROM
    harmonized.ingredient_usage_by_truck
WHERE
    order_month = 1 -- 月は数値1〜12で表されます
    AND truck_id = 15
GROUP BY truck_id, ingredient_name
ORDER BY total_ingredients_used DESC;

/*  5. 有向非巡回グラフ（DAG）によるパイプラインの可視化

    最後に、パイプラインの有向非巡回グラフ（DAG）を理解しましょう。
    DAGは、データパイプラインの視覚化として機能します。これを使用して、複雑なデータワークフローを
    視覚的にオーケストレーションし、タスクが正しい順序で実行されることを確認できます。
    パイプライン内の各Dynamic TableのLAGメトリクスと構成を表示したり、必要に応じてテーブルを
    手動で更新したりすることもできます。

    DAGにアクセスするには:
    - ナビゲーションメニューの「カタログ」ボタンをクリックしてデータベース画面を開く
    - 「TB_101」の横にある矢印「>」をクリックしてデータベースを展開
    - 「HARMONIZED」を展開し、次に「ダイナミックテーブル」を展開
    - 「INGREDIENT」テーブルをクリック
    - 「グラフ」タブをクリック
*/

-------------------------------------------------------------------------
--リセット--
-------------------------------------------------------------------------
USE ROLE accountadmin;
-- Dynamic Tablesを削除
DROP TABLE IF EXISTS raw_pos.menu_staging;
DROP TABLE IF EXISTS harmonized.ingredient;
DROP TABLE IF EXISTS harmonized.ingredient_to_menu_lookup;
DROP TABLE IF EXISTS harmonized.ingredient_usage_by_truck;

-- インサートを削除
DELETE FROM raw_pos.order_detail
WHERE order_detail_id = 904745311;
DELETE FROM raw_pos.order_header
WHERE order_id = 459520441;

-- クエリタグの設定を解除
ALTER SESSION UNSET query_tag;
-- ウェアハウスを一時停止
ALTER WAREHOUSE tb_de_wh SUSPEND;