/***************************************************************************************************       
アセット:      Zero to Snowflake - Horizonによるガバナンス
バージョン:    v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Horizonによるガバナンス
1. ロールとアクセス制御の紹介
2. 自動タグ付けによるタグベースの分類
3. マスキングポリシーによる列レベルのセキュリティ
4. 行アクセスポリシーによる行レベルのセキュリティ
5. データメトリック関数によるデータ品質監視
6. トラストセンターによるアカウントセキュリティ監視

****************************************************************************************************/

-- セッションのクエリタグを設定
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "governance_with_horizon"}}';

-- まず、ワークシートのコンテキストを設定しましょう
USE ROLE useradmin;
USE DATABASE tb_101;
USE WAREHOUSE tb_dev_wh;

/*  1. ロールとアクセス制御の紹介
    *************************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-access-control-overview
    *************************************************************************
    
    Snowflakeアクセス制御フレームワークは以下に基づいています:
      - ロールベースアクセス制御（RBAC）: アクセス権限はロールに割り当てられ、ロールはユーザーに割り当てられます
      - 任意アクセス制御（DAC）: 各オブジェクトには所有者がおり、所有者はそのオブジェクトへのアクセスを許可できます
    
    Snowflakeのアクセス制御を理解するための重要な概念:
      - セキュア可能なオブジェクト: セキュア可能なオブジェクトとは、誰が使用または表示できるかを制御できるものです。
        明示的に許可されていない場合、アクセスできません。これらのオブジェクトは個人ではなくグループ（ロール）によって
        管理されます。例えば、データベース、テーブル、関数はすべてセキュア可能なオブジェクトです
      - ロール: ロールは配布できる権限のセットのようなものです。これらのロールを個々のユーザーに、
        あるいは他のロールにも付与でき、権限のチェーンを作成できます
      - 権限: 権限は、オブジェクトに対して何かを行うための特定の許可です。多くの小さな権限を組み合わせて、
        誰かが持つアクセスの量を正確に制御できます
      - ユーザー: ユーザーは、Snowflakeが認識する単なるアイデンティティ（ユーザー名など）です。
        実際の人またはコンピュータープログラムの場合があります
    
      Snowflakeシステム定義ロールの定義:
       - ORGADMIN: 組織レベルでの操作を管理するロール
       - ACCOUNTADMIN: システムの最上位レベルのロールであり、アカウント内の限られた/制御された数のユーザーにのみ
          付与する必要があります
       - SECURITYADMIN: すべてのオブジェクトグラントをグローバルに管理でき、ユーザーとロールを作成、監視、管理できるロール
       - USERADMIN: ユーザーとロールの管理のみに専念するロール
       - SYSADMIN: アカウントでウェアハウスとデータベースを作成する権限を持つロール
       - PUBLIC: PUBLICは、すべてのユーザーとロールに自動的に付与される疑似ロールです。セキュア可能なオブジェクトを
           所有でき、所有するものはすべてアカウント内の他のすべてのユーザーとロールが利用できるようになります

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+

    このセクションでは、カスタムデータスチュワードロールを作成し、それに権限を関連付ける方法を見ていきます。
*/
-- まず、アカウントに既に存在するロールを確認しましょう
SHOW ROLES;

-- 次に、データスチュワードロールを作成します
CREATE OR REPLACE ROLE tb_data_steward
    COMMENT = 'カスタムロール';
-- ロールが作成されたので、SECURITYADMINロールに切り替えて、新しいロールに権限を付与できます

/*
    新しいロールが作成されたので、クエリを実行するためにウェアハウスを使用できるようにしたいと考えます。
    先に進む前に、ウェアハウスの権限についてより深く理解しましょう。
     
    - MODIFY: サイズの変更を含む、ウェアハウスのプロパティの変更を可能にします
    - MONITOR: ウェアハウスで実行された現在および過去のクエリと、そのウェアハウスの使用統計の
       表示を可能にします
    - OPERATE: ウェアハウスの状態の変更（停止、開始、一時停止、再開）を可能にします。さらに、
       ウェアハウスで実行された現在および過去のクエリの表示と、実行中のクエリの中止を可能にします
    - USAGE: 仮想ウェアハウスの使用を可能にし、その結果、ウェアハウスでのクエリの実行を可能にします。
       SQLステートメントが送信されたときに自動再開するようにウェアハウスが構成されている場合、
       ウェアハウスは自動的に再開してステートメントを実行します
    - ALL: ウェアハウスのOWNERSHIPを除くすべての権限を付与します

      ウェアハウスの権限を理解したので、新しいロールにoperateとusage権限を付与できます。
      まず、SECURITYADMINロールに切り替えます。
*/
USE ROLE securityadmin;
-- まず、ロールにウェアハウスtb_dev_whを使用する権限を付与します
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_data_steward;

/*
     次に、Snowflakeデータベースとスキーマのグラントを理解しましょう:
      - MODIFY: データベース設定の変更を可能にします
      - MONITOR: DESCRIBEコマンドの実行を可能にします
      - USAGE: データベースの使用を可能にします。これには、SHOW DATABASESコマンド出力でのデータベース詳細の
         返却が含まれます。データベース内のオブジェクトを表示またはアクションを実行するには、追加の権限が必要です
      - ALL: データベースのOWNERSHIPを除くすべての権限を付与します
*/

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_steward;

/*
    Snowflakeテーブルとビュー内のデータへのアクセスは、次の権限を通じて管理されます:
        SELECT: データを取得する権限を付与します
        INSERT: 新しい行を追加できます
        UPDATE: 既存の行を変更できます
        DELETE: 行を削除できます
        TRUNCATE: テーブルのすべての行を削除できます

      次に、raw_customerスキーマのテーブルでSELECTクエリを実行できることを確認します。
*/

-- RAW_CUSTOMERスキーマのすべてのテーブルにSELECT権限を付与
GRANT SELECT ON ALL TABLES IN SCHEMA raw_customer TO ROLE tb_data_steward;
-- governanceスキーマとgovernanceスキーマのすべてのテーブルにALL権限を付与
GRANT ALL ON SCHEMA governance TO ROLE tb_data_steward;
GRANT ALL ON ALL TABLES IN SCHEMA governance TO ROLE tb_data_steward;

/*
    新しいロールを使用するには、ロールを現在のユーザーに付与する必要もあります。次の2つのクエリを実行して、
    現在のユーザーに新しいデータスチュワードロールを使用する権限を付与します。
*/
SET my_user = CURRENT_USER();
GRANT ROLE tb_data_steward TO USER IDENTIFIER($my_user);

/*
    最後に、以下のクエリを実行して、新しく作成したロールを使用しましょう！
    --> あるいは、ワークシートUIの「Select role and warehouse」ボタンをクリックして、
        「tb_data_steward」を選択することでロールを使用することもできます。
*/
USE ROLE tb_data_steward;

-- お祝いに、これから作業するデータのタイプを確認しましょう
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    顧客ロイヤルティデータが表示されています。素晴らしい！しかし、よく見ると、
    このテーブルには機密性の高い個人を特定できる情報、つまりPIIが満載であることは明らかです。
    次のセクションでは、これをどのように軽減できるかをさらに詳しく見ていきます。
*/

/*  2. 自動タグ付けによるタグベースの分類
    ******************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/classify-auto
    ******************************************************

    前回のクエリでは、顧客ロイヤルティテーブルに保存されているかなりの量の個人を特定できる情報（PII）に
    気付きました。Snowflakeの自動タグ付け機能とタグベースのマスキングを組み合わせて使用して、
    クエリ結果の機密データを難読化できます。

    Snowflakeは、データベーススキーマの列を継続的に監視することで、機密情報を自動的に検出して
    タグ付けできます。データエンジニアがスキーマに分類プロファイルを割り当てると、
    そのスキーマのテーブル内のすべての機密データは、プロファイルのスケジュールに基づいて
    自動的に分類されます。
    
    これから分類プロファイルを作成し、列のセマンティックカテゴリに基づいて列に自動的に割り当てられる
    タグを指定します。accountadminロールに切り替えることから始めましょう。
*/
USE ROLE accountadmin;

/*
    governanceスキーマを作成し、その中にPII用のタグを作成し、新しいロールにデータベースオブジェクトに
    タグを適用する権限を付与します。
*/
CREATE OR REPLACE TAG governance.pii;
GRANT APPLY TAG ON ACCOUNT TO ROLE tb_data_steward;

/*
    まず、ロールtb_data_stewardに、raw_customerスキーマでデータ分類を実行し、
    分類プロファイルを作成するための適切な権限を付与する必要があります。
*/
GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA raw_customer TO ROLE tb_data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE tb_data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA governance TO ROLE tb_data_steward;

-- データスチュワードロールに戻ります
USE ROLE tb_data_steward;

/*
    分類プロファイルを作成します。スキーマに追加されたオブジェクトは即座に分類され、
    30日間有効で、自動的にタグ付けされます。
*/
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance.tb_classification_profile(
    {
      'minimum_object_age_for_classification_days': 0,
      'maximum_classification_validity_days': 30,
      'auto_tag': true
    });

/*
    指定されたセマンティックカテゴリに基づいて列に自動的にタグ付けするタグマップを作成します。
    これは、semantic_categories配列内の値のいずれかで分類された列が、自動的にPIIタグでタグ付けされることを意味します。
*/
CALL governance.tb_classification_profile!SET_TAG_MAP(
  {'column_tag_map':[
    {
      'tag_name':'tb_101.governance.pii',
      'tag_value':'pii',
      'semantic_categories':['NAME', 'PHONE_NUMBER', 'POSTAL_CODE', 'DATE_OF_BIRTH', 'CITY', 'EMAIL']
    }]});

-- 次に、SYSTEM$CLASSIFYを呼び出して、分類プロファイルを使用してcustomer_loyaltyテーブルを自動的に分類します
CALL SYSTEM$CLASSIFY('tb_101.raw_customer.customer_loyalty', 'tb_101.governance.tb_classification_profile');

/*
    次のクエリを実行して、自動分類とタグ付けの結果を確認します。すべてのSnowflakeアカウントで利用可能な、
    自動生成されたINFORMATION_SCHEMAからメタデータを取得します。各列がどのようにタグ付けされたか、
    そしてそれが前のステップで作成した分類プロファイルとどのように関連しているかを確認してください。
    
    すべての列がPRIVACY_CATEGORYとSEMANTIC_CATEGORYタグでタグ付けされており、
    それぞれ独自の目的があることがわかります。PRIVACY_CATEGORYは列内の個人データの機密性レベルを示し、
    SEMANTIC_CATEGORYはデータが表す実世界の概念を説明します。
    
    最後に、分類タグマップ配列で指定したセマンティックカテゴリでタグ付けされた列が、
    カスタムの「PII」タグでタグ付けされていることに注意してください。
*/
SELECT 
    column_name,
    tag_database,
    tag_schema,
    tag_name,
    tag_value,
    apply_method
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('raw_customer.customer_loyalty', 'table'));

/*  3. マスキングポリシーによる列レベルのセキュリティ
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-column-intro
    **************************************************************

    Snowflakeの列レベルのセキュリティにより、マスキングポリシーを使用して列のデータを保護できます。
    2つの主要な機能を提供します: クエリ時に機密データを隠したり変換したりできる動的データマスキングと、
    Snowflakeに入る前にデータをトークン化し、クエリ時にデトークン化できる外部トークン化です。

    機密列がPIIとしてタグ付けされたので、そのタグに関連付けるいくつかのマスキングポリシーを作成します。
    1つ目は、姓名、メール、電話番号などの機密文字列データ用です。2つ目は、誕生日などの機密DATE値用です。

    マスキングロジックは両方とも似ています: 現在のロールがPIIタグ付き列をクエリし、アカウント管理者または
    TastyBytes管理者でない場合、文字列値は「MASKED」と表示されます。日付値は元の年のみを表示し、
    月と日は01-01として表示されます。
*/

-- 機密文字列データ用のマスキングポリシーを作成
CREATE OR REPLACE MASKING POLICY governance.mask_string_pii AS (original_value STRING)
RETURNS STRING ->
  CASE WHEN
    -- ユーザーの現在のロールが特権ロールのいずれでもない場合、列をマスクします
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN '****MASKED****'
    -- それ以外の場合（タグが機密でないか、ロールが特権を持つ場合）、元の値を表示します
    ELSE original_value
  END;

-- 次に、機密DATEデータ用のマスキングポリシーを作成
CREATE OR REPLACE MASKING POLICY governance.mask_date_pii AS (original_value DATE)
RETURNS DATE ->
  CASE WHEN
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN DATE_TRUNC('year', original_value) -- マスクされた場合、年のみが変更されず、月と日は01-01になります
    ELSE original_value
  END;

-- 両方のマスキングポリシーを、顧客ロイヤルティテーブルに自動的に適用されたタグにアタッチします
ALTER TAG governance.pii SET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;

/*
    publicロールに切り替え、顧客ロイヤルティテーブルから最初の100行をクエリして、
    マスキングポリシーが機密データをどのように難読化するかを観察します。
*/
USE ROLE public;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- 次に、TB_ADMINロールに切り替えて、管理者ロールにマスキングポリシーが適用されないことを観察します
USE ROLE tb_admin;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*  4. 行アクセスポリシーによる行レベルのセキュリティ
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-row-intro
    ***********************************************************

    Snowflakeは、行アクセスポリシーを使用した行レベルのセキュリティをサポートしており、
    クエリ結果でどの行が返されるかを決定します。ポリシーはテーブルにアタッチされ、
    定義したルールに対して各行を評価することで機能します。これらのルールは、多くの場合、
    クエリを実行しているユーザーの属性（現在のロールなど）を使用します。

    例えば、行アクセスポリシーを使用して、米国のユーザーが米国内の顧客のデータのみを
    表示できるようにすることができます。

    まず、データスチュワードロールにロールを切り替えましょう。
*/
USE ROLE tb_data_steward;

-- 行アクセスポリシーを作成する前に、行ポリシーマップを作成します
CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permission STRING);

/*
    行ポリシーマップは、ロールを許可されたアクセス行の値に関連付けます。
    例えば、ロールtb_data_engineerを国の値「United States」に関連付けると、
    tb_data_engineerは国の値が「United States」である行のみを表示します。
*/
INSERT INTO governance.row_policy_map
    VALUES('tb_data_engineer', 'United States');

/*
    行ポリシーマップが配置されたので、行アクセスポリシーを作成します。
    
    このポリシーは、管理者には無制限の行アクセスがあることを示し、ポリシーマップ内の他のロールは
    関連する国と一致する行のみを表示できます。
*/
CREATE OR REPLACE ROW ACCESS POLICY governance.customer_loyalty_policy
    AS (country STRING) RETURNS BOOLEAN ->
        CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') 
        OR EXISTS 
            (
            SELECT 1
                FROM governance.row_policy_map rp
            WHERE
                UPPER(rp.role) = CURRENT_ROLE()
                AND rp.country_permission = country
            );

-- 行アクセスポリシーを顧客ロイヤルティテーブルの「country」列に適用します
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_loyalty_policy ON (country);

/*
    次に、行ポリシーマップで「United States」に関連付けたロールに切り替えて、
    行アクセスポリシーを使用してテーブルをクエリした結果を観察します。
*/
USE ROLE tb_data_engineer;

-- 米国の顧客のみが表示されるはずです
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    よくできました！Snowflakeの列および行レベルのセキュリティ戦略を使用してデータをガバナンスおよび
    保護する方法についてより深く理解できたはずです。個人を特定できる情報を含む列を保護するために
    マスキングポリシーと組み合わせて使用するタグを作成する方法と、ロールが特定の列値のみに
    アクセスできるようにする行アクセスポリシーを学びました。
*/

/*  5. データメトリック関数によるデータ品質監視
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-quality-intro
    ***********************************************************

    Snowflakeは、データメトリック関数（DMF）を使用してデータの一貫性と信頼性を維持します。
    これは、プラットフォーム内で直接品質チェックを自動化するための強力な機能です。
    これらのチェックを任意のテーブルまたはビューでスケジュールすることで、ユーザーはデータの
    整合性を明確に理解でき、より信頼性の高いデータに基づいた意思決定につながります。
    
    Snowflakeは、即座に使用できる事前構築されたシステムDMFと、固有のビジネスロジックのために
    カスタムDMFを作成する柔軟性の両方を提供し、包括的な品質監視を保証します。

    システムDMFのいくつかを見てみましょう！
*/

-- 次に、DMFの使用を開始するためにTastyBytesデータスチュワードロールに戻ります
USE ROLE tb_data_steward;

-- これは、order headerテーブルからnullの顧客IDのパーセンテージを返します
SELECT SNOWFLAKE.CORE.NULL_PERCENT(SELECT customer_id FROM raw_pos.order_header);

-- DUPLICATE_COUNTを使用して重複する注文IDをチェックできます
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT order_id FROM raw_pos.order_header); 

-- すべての注文の平均注文合計金額
SELECT SNOWFLAKE.CORE.AVG(SELECT order_total FROM raw_pos.order_header);

/*
    特定のビジネスルールに従ってデータ品質を監視するために、独自のカスタムデータメトリック関数を
    作成することもできます。単価に数量を掛けた値と等しくない注文合計をチェックするカスタムDMFを作成します。
*/

-- カスタムデータメトリック関数を作成
CREATE OR REPLACE DATA METRIC FUNCTION governance.invalid_order_total_count(
    order_prices_t table(
        order_total NUMBER,
        unit_price NUMBER,
        quantity INTEGER
    )
)
RETURNS NUMBER
AS
'SELECT COUNT(*)
 FROM order_prices_t
 WHERE order_total != unit_price * quantity';

-- 合計が単価 * 数量と等しくない新しい注文をシミュレート
INSERT INTO raw_pos.order_detail
SELECT
    904745311,
    459520442,
    52,
    null,
    0,
    2, -- 数量
    5.0, -- 単価
    5.0, -- 合計価格（意図的に誤り）
    null;

-- 注文詳細テーブルでカスタムDMFを呼び出します
SELECT governance.invalid_order_total_count(
    SELECT 
        price, 
        unit_price, 
        quantity 
    FROM raw_pos.order_detail
) AS num_orders_with_incorrect_price;

-- 注文詳細テーブルにデータメトリックスケジュールを設定して変更時にトリガーします
ALTER TABLE raw_pos.order_detail
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- カスタムDMFをテーブルに割り当て
ALTER TABLE raw_pos.order_detail
    ADD DATA METRIC FUNCTION governance.invalid_order_total_count
    ON (price, unit_price, quantity);

/*  6. トラストセンターによるアカウントセキュリティ監視
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/trust-center/overview
    **************************************************************

    トラストセンターは、スキャナーを使用してアカウントのセキュリティリスクを評価および監視するための
    自動チェックを有効にします。スキャナーは、アカウントのセキュリティリスクと違反をチェックする
    スケジュールされたバックグラウンドプロセスであり、その結果に基づいて推奨されるアクションを提供します。
    これらは、スキャナーパッケージにグループ化されることがよくあります。
    
    トラストセンターの一般的な使用例:
        - ユーザーに対して多要素認証が有効になっていることを確認
        - 過剰に権限を持つロールの発見
        - 少なくとも90日間ログインしていない非アクティブユーザーの発見
        - リスクの高いユーザーの発見と軽減

    開始する前に、トラストセンターの管理者になるために必要な権限を管理者ロールに付与する必要があります。
*/
USE ROLE accountadmin;
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_ADMIN TO ROLE tb_admin;
USE ROLE tb_admin; -- TastyBytes管理者ロールに戻ります

/*
    ナビゲーションメニューで「ガバナンスとセキュリティ」にカーソルを合わせ、次に「トラストセンター」をクリックします。
    必要に応じて、トラストセンターを別のブラウザタブで開くこともできます。
    トラストセンターを最初にロードすると、いくつかのペインとセクションが表示されます:
        1. タブ: Findings、Scanner Packages
        2. パスワードの準備状況ペイン
        3. 未解決のセキュリティ違反
        4. フィルター付きの違反リスト

    タブの下に、CIS Benchmarks Scanner Packageを有効にすることを勧めるメッセージが表示される場合があります。
    次にそれを行います。

    「Scanner Packages」タブをクリックします。ここには、スキャナーパッケージのリストが表示されます。
    これらは、アカウントのセキュリティリスクをチェックするスケジュールされたバックグラウンドプロセスである
    スキャナーのグループです。各スキャナーパッケージについて、名前、プロバイダー、アクティブおよび
    非アクティブなスキャナーの数、およびステータスを確認できます。Security Essentialsスキャナーパッケージを
    除き、すべてのスキャナーパッケージはデフォルトで無効になっています。
    
    「CIS Benchmarks」をクリックして、スキャナーパッケージの詳細を確認します。ここには、
    スキャナーパッケージの名前と説明、およびパッケージを有効にするオプションが表示されます。
    その下には、スキャナーパッケージ内のスキャナーのリストがあります。これらのスキャナーの
    いずれかをクリックすると、スケジュール、最後に実行された時間と日、説明などの詳細が表示されます。

    「Enable Package」ボタンをクリックして有効にしましょう。「Enable Scanner Package」モーダルが
    ポップアップし、スキャナーパッケージのスケジュールを設定できます。このパッケージを月次スケジュールで
    実行するように構成しましょう。

    「Frequency」のドロップダウンをクリックして、「Monthly」のオプションを選択します。
    他のすべての値はそのままにしておきます。パッケージは、有効にすると、構成されたスケジュールで
    自動的に実行されることに注意してください。
    
    オプションで、通知設定を構成できます。最小重大度トリガーレベルが「Critical」で、
    Recipientsの下で「Admin users」が選択されているデフォルト値を保持できます。
    「continue」を押します。
    スキャナーパッケージが完全に有効になるまで数分かかる場合があります。

    アカウントの「Threat Intelligence」スキャナーパッケージについて、もう一度これを繰り返しましょう。
    前回のスキャナーパッケージと同じ構成設定を使用します。
    
    両方のパッケージが有効になったら、「Findings」タブに戻り、スキャナーパッケージが発見した
    違反を確認します。

    各重大度レベルでの違反数のグラフと共に、違反リストにさらに多くのエントリが表示されるはずです。
    違反リストでは、短い説明、重大度、スキャナーパッケージを含む、すべての違反に関する詳細情報を
    確認できます。違反を解決済みとしてマークするオプションもあります。
    さらに、個々の違反のいずれかをクリックすると、サマリーや修復のオプションなど、
    違反に関するより詳細な情報が表示される詳細ペインが表示されます。

    違反リストは、ドロップダウンオプションを使用して、ステータス、重大度、スキャナーパッケージで
    フィルタリングできます。違反グラフの重大度カテゴリをクリックすると、そのタイプのフィルターも
    適用されます。
    
    現在アクティブなフィルターカテゴリの横にある「X」をクリックして、フィルターをキャンセルします。
*/

-------------------------------------------------------------------------
--リセット--
-------------------------------------------------------------------------
USE ROLE accountadmin;

-- データスチュワードロールを削除
DROP ROLE IF EXISTS tb_data_steward;

-- マスキングポリシー
ALTER TAG IF EXISTS governance.pii UNSET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;
DROP MASKING POLICY IF EXISTS governance.mask_string_pii;
DROP MASKING POLICY IF EXISTS governance.mask_date_pii;

-- 自動分類
ALTER SCHEMA raw_customer UNSET CLASSIFICATION_PROFILE;
DROP SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE IF EXISTS tb_classification_profile;

-- 行アクセスポリシー
ALTER TABLE raw_customer.customer_loyalty 
    DROP ROW ACCESS POLICY governance.customer_loyalty_policy;
DROP ROW ACCESS POLICY IF EXISTS governance.customer_loyalty_policy;

-- データメトリック関数
DELETE FROM raw_pos.order_detail WHERE order_detail_id = 904745311;
ALTER TABLE raw_pos.order_detail
    DROP DATA METRIC FUNCTION governance.invalid_order_total_count ON (price, unit_price, quantity);
DROP FUNCTION governance.invalid_order_total_count(TABLE(NUMBER, NUMBER, INTEGER));
ALTER TABLE raw_pos.order_detail UNSET DATA_METRIC_SCHEDULE;

-- タグの設定を解除
ALTER TABLE raw_customer.customer_loyalty
  MODIFY
    COLUMN first_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN last_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN e_mail UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN phone_number UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN postal_code UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN marital_status UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN gender UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN birthday_date UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN country UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN city UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY;

-- PIIタグを削除
DROP TAG IF EXISTS governance.pii;
-- クエリタグの設定を解除
ALTER SESSION UNSET query_tag;
ALTER WAREHOUSE tb_dev_wh SUSPEND;