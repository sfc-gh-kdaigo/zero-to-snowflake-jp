/***************************************************************************************************       
アセット:      Zero to Snowflake - Snowflakeを始める
バージョン:    v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Snowflakeを始める
1. 仮想ウェアハウスと設定
2. 永続化されたクエリ結果の使用
3. 基本的なデータ変換テクニック
4. UNDROPによるデータ復旧
5. リソースモニター
6. 予算
7. ユニバーサル検索

****************************************************************************************************/

-- 開始する前に、このクエリを実行してセッションのクエリタグを設定します
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "getting_started_with_snowflake"}}';

-- ワークシートのコンテキストを設定することから始めます。データベース、スキーマ、ロールを設定します

USE DATABASE tb_101;
USE ROLE accountadmin;

/*   1. 仮想ウェアハウスと設定
    **************************************************************
     ユーザーガイド:
     https://docs.snowflake.com/en/user-guide/warehouses-overview
    **************************************************************
    
    仮想ウェアハウスは、Snowflakeデータの分析を実行できる、動的でスケーラブル、かつコスト効率の高い
    コンピューティングパワーです。その目的は、基盤となる技術的な詳細を気にすることなく、
    すべてのデータ処理ニーズを処理することです。

    ウェアハウスのパラメータ:
      > WAREHOUSE_SIZE: 
            サイズは、ウェアハウス内のクラスターごとに利用可能なコンピュートリソースの量を指定します
            利用可能なサイズはX-SmallからX6-Largeまでの範囲です
            デフォルト: 'XSmall'
      > WAREHOUSE_TYPE:
            仮想ウェアハウスのタイプを定義し、そのアーキテクチャと動作を決定します
            タイプ:
                'STANDARD' - 汎用ワークロード用
                'SNOWPARK_OPTIMIZED' - メモリ集約型ワークロード用
            デフォルト: 'STANDARD'
      > AUTO_SUSPEND:
            ウェアハウスが自動的に一時停止する前の非アクティブ期間を指定します
            デフォルト: 600秒
      > INITIALLY_SUSPENDED:
            ウェアハウスが作成直後に一時停止状態で開始するかどうかを決定します
            デフォルト: TRUE
      > AUTO_RESUME:
            クエリが送信されたときにウェアハウスが一時停止状態から自動的に再開するかどうかを決定します
            デフォルト: TRUE

        それでは、最初のウェアハウスを作成しましょう！
*/

-- まず、アカウント上に既に存在し、アクセス権限があるウェアハウスを見てみましょう
SHOW WAREHOUSES;

/*
    これは、ウェアハウスのリストとその属性（名前、状態（実行中または一時停止中）、タイプ、サイズなど）を
    返します。
    
    Snowsightですべてのウェアハウスを表示および管理することもできます。ウェアハウスページにアクセスするには、
    ナビゲーションメニューのAdminボタンをクリックし、展開されたAdminカテゴリの「Warehouses」リンクを
    クリックします。
    
    ウェアハウスページに戻ると、このアカウントのウェアハウスのリストとその属性が表示されます。
*/

-- シンプルなSQLコマンドでウェアハウスを簡単に作成できます
CREATE OR REPLACE WAREHOUSE my_wh
    COMMENT = 'My TastyBytes warehouse'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = false;

/*
    ウェアハウスができたので、このワークシートがこのウェアハウスを使用することを指定する必要があります。
    これはSQLコマンドまたはUIのいずれかで実行できます。
*/

-- ウェアハウスを使用
USE WAREHOUSE my_wh;

/*
    シンプルなクエリを実行してみることができますが、結果ペインにエラーメッセージが表示され、
    ウェアハウスMY_WHが一時停止していることが通知されます。今すぐ試してみてください。
*/
SELECT * FROM raw_pos.truck_details;

/*    
    クエリを実行するには、すべてのDML操作と同様に、アクティブなウェアハウスが必要です。
    データからインサイトを得たい場合は、ウェアハウスを再開する必要があります。
    
    エラーメッセージには、SQLコマンド「ALTER warehouse MY_WH resume」を実行する提案も
    含まれていました。それを実行しましょう！
*/
ALTER WAREHOUSE my_wh RESUME;

/* 
    また、ウェアハウスが再び一時停止した場合に手動で再開する必要がないように、
    AUTO_RESUMEをTRUEに設定します。
 */
ALTER WAREHOUSE my_wh SET AUTO_RESUME = TRUE;

-- ウェアハウスが実行中になったので、先ほどのクエリを実行してみましょう
SELECT * FROM raw_pos.truck_details;

-- これでデータに対してクエリを実行できるようになりました

/* 
    次に、Snowflakeのウェアハウスのスケーラビリティの力を見てみましょう。
    
    Snowflakeのウェアハウスはスケーラビリティと弾力性を備えて設計されており、
    ワークロードのニーズに基づいてコンピュートリソースを上下に調整する力を提供します。
    
    シンプルなALTER WAREHOUSEステートメントで、ウェアハウスをその場で簡単にスケールアップできます。
*/
ALTER WAREHOUSE my_wh SET warehouse_size = 'XLarge';


-- テーブルの統計的な情報を見てみましょう　（メタメタデータキャッシュ）
SELECT COUNT(*) FROM raw_pos.truck_details;
SELECT MAX(truck_opening_date) AS newest_truck FROM raw_pos.truck_details;
SELECT MIN(truck_opening_date) AS oldest_truck FROM raw_pos.truck_details;

-- それでは、トラックごとの売上を見てみましょう
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;

-- それでは、もう一度トラックごとの売上を見てみましょう　（リザルトキャッシュ）
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;

-- リザルトキャッシュを回避する条件を追加して、もう一度クエリを実行してみましょう (ウェアハウスキャッシュ)
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(o.price) AS total_sales
FROM analytics.orders_v o
WHERE 1=1  -- リザルトキャッシュを回避する条件
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;
/*
    結果パネルを開いた状態で、右上のツールバーをすばやく見てみましょう。ここには、検索、列の選択、
    クエリの詳細と期間の統計の表示、列の統計の表示、結果のダウンロードなどのオプションが表示されます。
    
    検索 - 検索用語を使用して結果をフィルタリング
    列の選択 - 結果に表示する列を有効/無効にする
    クエリの詳細 - SQLテキスト、返された行数、クエリID、実行されたロールとウェアハウスなど、
                   クエリに関連する情報が含まれます
    クエリの期間 - コンパイル、プロビジョニング、実行時間ごとにクエリの実行にかかった時間を分類
    列の統計 - 結果パネルの列の分布に関するデータを表示
    結果のダウンロード - 結果をCSVとしてエクスポートおよびダウンロード
*/

/*  2. 永続化されたクエリ結果の使用
    *******************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/querying-persisted-results
    *******************************************************************
    
    先に進む前に、Snowflakeのもう1つの強力な機能であるクエリ結果キャッシュを実演するのに
    最適な場所です。
    
    上記のクエリを最初に実行したとき、XLウェアハウスを使用していても、完了するまでに
    数秒かかりました。

    上記の同じ「トラックごとの売上」クエリを実行し、クエリ期間ペインで合計実行時間を確認してください。
    最初に実行したときは数秒かかったのが、次回はわずか数百ミリ秒になったことに気付くでしょう。
    これがクエリ結果キャッシュの動作です。

    クエリ履歴パネルを開いて、クエリが最初に実行されたときと2回目の実行時間を比較してください。
    
    クエリ結果キャッシュの概要:
    - 結果はどのクエリでも24時間保持されますが、クエリが実行されるたびにタイマーがリセットされます
    - 結果キャッシュのヒットにはほとんどコンピュートリソースが必要ないため、頻繁に実行される
      レポートやダッシュボード、およびクレジット消費の管理に最適です
    - キャッシュはCloud Servicesレイヤーに存在するため、個々のウェアハウスから論理的に分離されています
      これにより、同じアカウント内のすべての仮想ウェアハウスとユーザーがグローバルにアクセスできます
*/

-- これからより小さなデータセットで作業するので、ウェアハウスを縮小できます
ALTER WAREHOUSE my_wh SET warehouse_size = 'XSmall';

/*  3. 基本的なデータ変換テクニック

    ウェアハウスが構成され実行されているので、トラックの製造元の分布を理解することが計画ですが、
    この情報は、年、メーカー、モデルに関する情報をVARIANTデータ型で格納する別の列
    「truck_build」に埋め込まれています。

    VARIANTデータ型は半構造化データの例です。OBJECT、ARRAY、その他のVARIANT値を含む
    あらゆる種類のデータを格納できます。この場合、truck_buildは、年、メーカー、モデルの
    3つの異なるVARCHAR値を含む単一のOBJECTを格納します。
    
    これから、より簡単で容易な分析を可能にするために、3つのプロパティすべてをそれぞれの列に分離します。
*/
SELECT truck_build FROM raw_pos.truck_details;

/*  ゼロコピークローニング

    truck_build列のデータは一貫して同じ形式に従っています。品質分析をより簡単に実行するために、
    「make」用の別の列が必要になります。計画は、truckテーブルの開発コピーを作成し、年、メーカー、
    モデルの新しい列を追加し、truck build VARIANTオブジェクトから各プロパティを抽出して
    これらの新しい列に格納することです。
 
    Snowflakeの強力なゼロコピークローニングにより、追加のストレージスペースを使用せずに、
    データベースオブジェクトの同一で完全に機能する別のコピーを即座に作成できます。

    ゼロコピークローニングは、Snowflakeの独自のマイクロパーティションアーキテクチャを活用して、
    クローンされたオブジェクトと元のコピーの間でデータを共有します。いずれかのテーブルへの変更は、
    変更されたデータのみに対して新しいマイクロパーティションが作成されます。これらの新しい
    マイクロパーティションは、クローンまたは元のクローンされたオブジェクトのいずれであっても、
    所有者が独占的に所有するようになります。基本的に、一方のテーブルに加えられた変更は、
    元のコピーまたはクローンコピーのいずれにも影響しません。
*/

-- truckテーブルのゼロコピークローンとしてtruck_devテーブルを作成
CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck_details;

-- truckテーブルのtruck_devへのクローンが成功したことを確認
SELECT TOP 15 * 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    truckテーブルの開発コピーができたので、新しい列を追加することから始めます。
    注意: 3つのステートメントを一度に実行するには、それらを選択して画面右上の青い「Run」ボタンを
    クリックするか、キーボードを使用します。
    
        Mac: command + return
        Windows: Ctrl + Enter
*/

ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS year NUMBER;
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS make VARCHAR(255);
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS model VARCHAR(255);

/*
    それでは、truck_build列から抽出したデータで新しい列を更新しましょう。
    コロン（:）演算子を使用してtruck_build列の各キーの値にアクセスし、
    その値をそれぞれの列に設定します。
*/
UPDATE raw_pos.truck_dev
SET 
    year = truck_build:year::NUMBER,
    make = truck_build:make::VARCHAR,
    model = truck_build:model::VARCHAR;

-- 3つの列がテーブルに正常に追加され、truck_buildから抽出されたデータが入力されたことを確認
SELECT year, make, model FROM raw_pos.truck_dev;

-- これで、さまざまなメーカーをカウントし、TastyBytesフードトラックフリートの分布を把握できます
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_dev
GROUP BY make
ORDER BY make ASC;

/*
    上記のクエリを実行した後、データセットに問題があることに気付きます。一部のトラックのメーカーは
    「Ford」で、一部は「Ford_」であり、同じトラックメーカーに対して2つの異なるカウントが得られます。
*/

-- まず、UPDATEを使用して「Ford_」のすべての出現を「Ford」に変更します
UPDATE raw_pos.truck_dev
    SET make = 'Ford'
    WHERE make = 'Ford_';

-- make列が正常に更新されたことを確認
SELECT truck_id, make 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    make列は問題なさそうなので、truckテーブルとtruck_devテーブルをSWAPしましょう。
    このコマンドは、2つのテーブル間でメタデータとデータをアトミックにスワップし、
    truck_devテーブルを即座に新しい本番truckテーブルに昇格させます。
*/
ALTER TABLE raw_pos.truck_details SWAP WITH raw_pos.truck_dev; 

-- 以前のクエリを実行して、正確なメーカーカウントを取得します
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_details
GROUP BY
    make
ORDER BY count DESC;
/*
    変更は良さそうです。データを3つの別々の列に分割したので、本番データベースからtruck_build列を
    削除することでデータセットのクリーンアップを実行します。
    その後、truck_devテーブルは不要になるため削除できます。
*/

-- シンプルなALTER TABLE ... DROP COLUMNコマンドで古いtruck build列を削除できます
ALTER TABLE raw_pos.truck_details DROP COLUMN truck_build;

-- これでtruck_devテーブルを削除できます
DROP TABLE raw_pos.truck_details;

/*  4. UNDROPによるデータ復旧
	
    大変です！誤って本番のtruckテーブルを削除してしまいました。😱

    幸いなことに、UNDROPコマンドを使用して、テーブルを削除される前の状態に復元できます。
    UNDROPは、Snowflakeの強力なTime Travel機能の一部であり、構成されたデータ保持期間
    （デフォルト24時間）内に削除されたデータベースオブジェクトを復元できます。

    UNDROPを使用して、本番の「truck」テーブルをすぐに復元しましょう！
*/

-- オプション: このクエリを実行して「truck」テーブルが存在しないことを確認します
    -- 注意: エラー「Table TRUCK does not exist or not authorized.」は、テーブルが削除されたことを意味します
DESCRIBE TABLE raw_pos.truck_details;

-- 本番の「truck」テーブルにUNDROPを実行して、削除される前の正確な状態に復元します
UNDROP TABLE raw_pos.truck_details;

-- テーブルが正常に復元されたことを確認
SELECT * from raw_pos.truck_details;

-- 次に、実際のtruck_devテーブルを削除します
DROP TABLE raw_pos.truck_dev;

/*  5. リソースモニター
    ***********************************************************
    ユーザーガイド:                                   
    https://docs.snowflake.com/en/user-guide/resource-monitors
    ***********************************************************

    コンピュート使用量と支出の監視は、クラウドベースのワークフローにとって重要です。Snowflakeは、
    リソースモニターを使用してウェアハウスのクレジット使用量を追跡するシンプルで直感的な方法を
    提供します。

    リソースモニターを使用すると、クレジットクォータを定義し、定義された使用量のしきい値に
    達したときに関連するウェアハウスで特定のアクションをトリガーできます。

    リソースモニターが実行できるアクション:
    - NOTIFY: 指定されたユーザーまたはロールにメール通知を送信します
    - SUSPEND: しきい値に達したときに関連するウェアハウスを一時停止します
               注意: 実行中のクエリは完了が許可されます
    - SUSPEND_IMMEDIATE: しきい値に達したときに関連するウェアハウスを一時停止し、
                         実行中のすべてのクエリをキャンセルします

    それでは、ウェアハウスmy_whのリソースモニターを作成します

    Snowsightでアカウントレベルのロールをaccountadminにすばやく設定しましょう
    そのためには:
    - 画面の左下にあるユーザーアイコンをクリック
    - 「Switch Role」にカーソルを合わせる
    - ロールリストパネルで「ACCOUNTADMIN」を選択

   次に、ワークシートでaccountadminロールを使用します
*/
USE ROLE accountadmin;

-- 以下のクエリを実行してSQL経由でリソースモニターを作成します
CREATE OR REPLACE RESOURCE MONITOR my_resource_monitor
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY -- DAILY、WEEKLY、YEARLY、またはNEVER（1回限りのクォータ用）も可能
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- リソースモニターが作成されたので、my_whに適用します
ALTER WAREHOUSE my_wh 
    SET RESOURCE_MONITOR = my_resource_monitor;

/*  6. 予算
    ****************************************************
      ユーザーガイド:                                   
      https://docs.snowflake.com/en/user-guide/budgets 
    ****************************************************
      
    前のステップでは、ウェアハウスのクレジット使用量を監視できるリソースモニターを構成しました。
    このステップでは、Snowflakeのコストをより包括的かつ柔軟に管理するための予算を作成します。
    
    リソースモニターはウェアハウスとコンピュート使用量に特に関連付けられていますが、予算は
    任意のSnowflakeオブジェクトまたはサービスのコストを追跡し、支出制限を課し、
    金額が指定されたしきい値に達したときにユーザーに通知するために使用できます。
*/

-- まず予算を作成しましょう
CREATE OR REPLACE SNOWFLAKE.CORE.BUDGET my_budget()
    COMMENT = 'My Tasty Bytes Budget';

/*
    予算を構成する前に、アカウントのメールアドレスを確認する必要があります。

    メールアドレスを確認するには:
    - 画面の左下にあるユーザーアイコンをクリック
    - 設定をクリック
    - メールフィールドにメールアドレスを入力
    - 「保存」をクリック
    - メールを確認し、指示に従ってメールを確認
        注意: 数分経ってもメールが届かない場合は、「Resend Verification」をクリック
     
    新しい予算が設定され、メールが確認され、アカウントレベルのロールがaccountadminに設定されたので、
    Snowsightの予算ページに移動して、予算にリソースを追加しましょう。

    Snowsightの予算ページに移動するには:
    - ナビゲーションメニューの管理者ボタンをクリック
    - 最初の項目「コスト管理」をクリック
    - 「予算」タブをクリック
    
    ウェアハウスの選択を求められた場合は、tb_dev_whを選択します。そうでない場合は、
    画面の右上にあるウェアハウスパネルからウェアハウスがtb_dev_whに設定されていることを確認します。
    
    予算ページには、現在の期間の支出に関するメトリクスが表示されます。
    画面の中央には、現在の支出と予測される支出のグラフが表示されます。
    画面の下部には、先ほど作成した「MY_BUDGET」予算が表示されます。
    それをクリックして予算ページを表示します
    
    画面右上の「<- 予算の詳細」をクリックすると、
    予算詳細パネルが表示されます。ここでは、予算とそれに関連付けられている
    すべてのリソースに関する情報を表示できます。監視されているリソースがないので、
    今すぐいくつか追加しましょう。
    「編集」ボタンをクリックして予算編集パネルを開きます
    
    - 予算名はそのまま
    - 支出制限を100に設定
    - 先ほど確認したメールアドレスを入力
    - 「+ タグおよびリソース」ボタンをクリックしてリソースをいくつか追加
    - データベースを展開し、次にTB_101を展開して、ANALYTICSスキーマの横のチェックボックスをオン
    - 下にスクロールして「ウェアハウス」を展開
    - 「TB_DE_WH」のチェックボックスをオン
    - 「完了」をクリック
    - 予算編集メニューに戻り、「変更を保存」をクリック
*/

/*  7. ユニバーサル検索
    **************************************************************************
      ユーザーガイド                                                             
      https://docs.snowflake.com/en/user-guide/ui-snowsight-universal-search  
    **************************************************************************

    ユニバーサル検索を使用すると、アカウント内の任意のオブジェクトを簡単に見つけることができ、
    さらにMarketplaceのデータ製品、関連するSnowflakeドキュメント、コミュニティナレッジベースの
    記事を探索できます。

    今すぐ試してみましょう。
    - ユニバーサル検索を使用するには、ナビゲーションメニューの「検索」をクリックします
    - ここでユニバーサル検索UIが表示されます。最初の検索用語を入力しましょう
    - 検索バーに「truck」と入力して結果を観察します。上部のセクションは、データベース、テーブル、
      ビュー、ステージなど、アカウントの関連オブジェクトのカテゴリです。データベースオブジェクトの
      下には、関連するマーケットプレースのリストとドキュメントのセクションが表示されます
      ※各オブジェクトにインデックスをするのに少し時間がかかるため、表示されない場合はまた後ほど

    - 自然言語で検索用語を入力して、探しているものを説明することもできます。どのトラックフランチャイズが
      最も多くのリピート顧客を持っているかを知りたい場合は、「Which truck franchise has the most loyal customer base?」のように検索できます。
      「テーブルとビュー」セクションの横にある「すべて表示 >」ボタンを
      クリックすると、クエリに関連するすべての関連テーブルとビューを表示できます

    ユニバーサル検索は、さまざまなスキーマから複数のテーブルとビューを返します。各オブジェクトに
    関連する列がどのようにリストされているかにも注目してください。これらはすべて、リピート顧客に
    関するデータ駆動型の回答を得るための優れた出発点です。
*/

-------------------------------------------------------------------------
--リセット--
-------------------------------------------------------------------------
-- 作成したオブジェクトを削除
DROP RESOURCE MONITOR IF EXISTS my_resource_monitor;
DROP TABLE IF EXISTS raw_pos.truck_dev;

-- truck詳細をリセット
CREATE OR REPLACE TABLE raw_pos.truck_details
AS 
SELECT * EXCLUDE (year, make, model)
FROM raw_pos.truck;

DROP WAREHOUSE IF EXISTS my_wh;
-- クエリタグの設定を解除
ALTER SESSION UNSET query_tag;