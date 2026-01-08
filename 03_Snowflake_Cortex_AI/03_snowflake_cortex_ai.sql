/*************************************************************************************************** 
アセット:      Zero to Snowflake - AISQL関数
バージョン:    v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

AISQL関数
1. SENTIMENT()を使用してトラック顧客レビューをポジティブ、ネガティブ、またはニュートラルとしてスコアリングおよびラベル付け
2. AI_CLASSIFY()を使用して、食品の品質やサービス体験などのテーマごとにレビューを分類
3. EXTRACT_ANSWER()を使用してレビューテキストから特定の苦情や賞賛を抽出
4. AI_SUMMARIZE_AGG()を使用してトラックブランド名ごとの顧客センチメントの要約を生成

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "aisql_functions"}}';

/*
    AISQL関数を活用して顧客レビューからインサイトを得る意図でTastyBytesデータアナリストの
    役割を担うので、コンテキストを適切に設定しましょう。
*/

USE ROLE tb_analyst;
USE DATABASE tb_101;
USE WAREHOUSE tb_analyst_wh;

/* 1. 大規模なセンチメント分析
    ***************************************************************
    すべてのフードトラックブランドの顧客センチメントを分析して、最もパフォーマンスの高い
    トラックを特定し、フリート全体の顧客満足度メトリクスを作成します。
    Cortex Playgroundでは、個々のレビューを手動で分析しました。ここでは、
    SENTIMENT()関数を使用して、Snowflakeの公式センチメント範囲に従って、
    顧客レビューを-1（ネガティブ）から+1（ポジティブ）まで自動的にスコアリングします。
    ***************************************************************/

-- ビジネス上の質問: 「各トラックブランドに対して顧客はどのように感じているか？」
-- このクエリを実行して、フードトラックネットワーク全体の顧客センチメントを分析し、フィードバックを分類してください

SELECT
    truck_brand_name,
    COUNT(*) AS total_reviews,
    AVG(CASE WHEN sentiment >= 0.5 THEN sentiment END) AS avg_positive_score,
    AVG(CASE WHEN sentiment BETWEEN -0.5 AND 0.5 THEN sentiment END) AS avg_neutral_score,
    AVG(CASE WHEN sentiment <= -0.5 THEN sentiment END) AS avg_negative_score
FROM (
    SELECT
        truck_brand_name,
        SNOWFLAKE.CORTEX.SENTIMENT (review) AS sentiment
    FROM harmonized.truck_reviews_v
    WHERE
        language ILIKE '%en%'
        AND review IS NOT NULL
    LIMIT 10000
)
GROUP BY
    truck_brand_name
ORDER BY total_reviews DESC;

/*
    重要なインサイト:
        Cortex Playgroundでレビューを1つずつ分析していたのが、数千件を体系的に処理するように
        移行したことに注目してください。SENTIMENT()関数は、すべてのレビューを自動的にスコアリングし、
        ポジティブ、ネガティブ、ニュートラルに分類し、フリート全体の顧客満足度メトリクスを即座に提供します。
    センチメントスコアの範囲:
        ポジティブ:   0.5 ～ 1
        ニュートラル: -0.5 ～ 0.5
        ネガティブ:   -0.5 ～ -1
*/

/* 2. 顧客フィードバックの分類
    ***************************************************************
    次に、すべてのレビューを分類して、顧客がサービスのどの側面について最も話しているかを
    理解しましょう。AI_CLASSIFY()関数を使用します。この関数は、単純なキーワードマッチングではなく、
    AIの理解に基づいて、レビューをユーザー定義のカテゴリに自動的に分類します。
    このステップでは、顧客フィードバックをビジネスに関連する運用領域に分類し、
    その分布パターンを分析します。
    ***************************************************************/

-- ビジネス上の質問: 「顧客は主に何についてコメントしているか - 食品の品質、サービス、または配達体験？」
-- 分類クエリを実行:

WITH classified_reviews AS (
  SELECT
    truck_brand_name,
    AI_CLASSIFY(
      review,
      ['Food Quality', 'Pricing', 'Service Experience', 'Staff Behavior']
    ):labels[0] AS feedback_category
  FROM
    harmonized.truck_reviews_v
  WHERE
    language ILIKE '%en%'
    AND review IS NOT NULL
    AND LENGTH(review) > 30
  LIMIT
    10000
)
SELECT
  truck_brand_name,
  feedback_category,
  COUNT(*) AS number_of_reviews
FROM
  classified_reviews
GROUP BY
  truck_brand_name,
  feedback_category
ORDER BY
  truck_brand_name,
  number_of_reviews DESC;
                
/*
    重要なインサイト:
        AI_CLASSIFY()が、数千のレビューを食品の品質、サービス体験などのビジネスに関連する
        テーマに自動的に分類した様子を観察してください。食品の品質がトラックブランド全体で
        最も議論されているトピックであることがすぐにわかり、運用チームに顧客の優先事項に関する
        明確で実用的なインサイトを提供します。
*/

/* 3. 特定の運用インサイトの抽出
    ***************************************************************
    次に、非構造化テキストから正確な回答を得るために、EXTRACT_ANSWER()関数を利用します。
    この強力な関数により、顧客フィードバックに関する特定のビジネス上の質問をして、
    直接的な回答を受け取ることができます。このステップの目標は、顧客レビューで言及されている
    正確な運用上の問題を特定し、即座の注意が必要な特定の問題を強調することです。
    ***************************************************************/

-- ビジネス上の質問: 「各顧客レビュー内で見つかる特定の運用上の問題またはポジティブな言及は何か？」
-- 次のクエリを実行しましょう:

  SELECT
    truck_brand_name,
    primary_city,
    LEFT(review, 100) || '...' AS review_preview,
    -- LEFT(SNOWFLAKE.CORTEX.AI_TRANSLATE(review, 'en', 'ja'), 100) || '...' AS review_preview_ja,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review,
        'What specific improvement or complaint is mentioned in this review?' -- このレビューで言及されている具体的な改善点または不満は何ですか？
    ) AS specific_feedback
FROM 
    harmonized.truck_reviews_v
WHERE 
    language = 'en'
    AND review IS NOT NULL
    AND LENGTH(review) > 50
ORDER BY truck_brand_name, primary_city ASC
LIMIT 10000;

/*
    重要なインサイト:
        EXTRACT_ANSWER()が長い顧客レビューから具体的で実用的なインサイトをどのように抽出するかに
        注目してください。手動レビューではなく、この関数は「friendly staff was saving grace（親切な
        スタッフが救いだった）」や「hot dogs are cooked to perfection（ホットドッグは完璧に調理されている）」
        などの具体的なフィードバックを自動的に特定します。結果は、密度の高いテキストを、
        運用チームが即座に活用できる具体的で引用可能なフィードバックに変換することです。
*/

/* 4. エグゼクティブサマリーの生成
    ***************************************************************
    最後に、顧客フィードバックの簡潔なサマリーを作成するために、AI_SUMMARIZE_AGG()関数を使用します。
    この強力な関数は、長い非構造化テキストから短く一貫性のあるサマリーを生成します。
    このステップの目標は、各トラックブランドの顧客レビューの本質を理解しやすいサマリーに
    凝縮し、全体的なセンチメントと重要なポイントの迅速な概要を提供することです。
    ***************************************************************/

-- ビジネス上の質問: 「各トラックブランドの主要なテーマと全体的なセンチメントは何か？」
-- サマリー化クエリを実行:

SELECT
  truck_brand_name,
  AI_SUMMARIZE_AGG (review) AS review_summary,
  SNOWFLAKE.CORTEX.AI_TRANSLATE(review_summary, 'en', 'ja') AS review_summary_ja
FROM
  (
    SELECT
      truck_brand_name,
      review
    FROM
      harmonized.truck_reviews_v
    LIMIT
      100
  )
GROUP BY
  truck_brand_name;


/*
  重要なインサイト:
      AI_SUMMARIZE_AGG()関数は、長いレビューを明確なブランドレベルのサマリーに凝縮します。
      これらのサマリーは、繰り返されるテーマとセンチメントのトレンドを強調し、意思決定者に
      各フードトラックのパフォーマンスの迅速な概要を提供し、個々のレビューを読むことなく
      顧客の認識をより速く理解できるようにします。
*/

/*************************************************************************************************** 
    AI SQL関数の変革力をうまく実証しました。顧客フィードバック分析を個別のレビュー処理から
    体系的な本番規模のインテリジェンスにシフトしました。これら4つのコア関数を通じた旅は、
    それぞれが明確な分析目的を果たし、生の顧客の声を包括的なビジネスインテリジェンス
    （体系的、スケーラブル、即座に実行可能）に変換する方法を明確に示しています。
    かつて個別のレビュー分析が必要だったものが、今では数秒で数千のレビューを処理し、
    データ駆動型の運用改善に重要な感情的コンテキストと具体的な詳細の両方を提供します。
****************************************************************************************************/