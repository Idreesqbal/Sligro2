{{
  config(
    partition_by={
      "field": "LOAD_DATETIME",
      "data_type": "timestamp",
      "granularity": "month"
    }

  )
}}

WITH header AS (
    SELECT
        load_datetime,
        start_datetime,
        end_datetime,
        ionrba,
        purchase_order_bk,
        purchase_order_hk,
        goods_received_bk,
        goods_received_hk
    FROM {{ ref('bv_bdv__goods_received') }}
),

line AS (
    SELECT *
    FROM {{ ref('rv_sat__goods_received_line') }}
),

{# Create new goods received/purchase order line bk using business logic.
We need to join with the header because we don't have the received date in the lines.
We use QUALIFY to remove duplicates. #}
unique_key_creation AS (
    SELECT
        line.acbdbe,
        line.aiprbe,
        line.akkdbe,
        line.argrbe,
        line.arnrbe,
        line.arombe,
        line.askdbe,
        line.bdbtbe,
        line.bkdtbe,
        line.bkusbe,
        line.bsaabe,
        line.btkdbe,
        line.cnprbe,
        line.dcaabe,
        line.emprbe,
        line.etaabe,
        line.etikbe,
        line.fedtbe,
        line.fekdbe,
        line.feusbe,
        line.gewibe,
        line.godtbe,
        line.gousbe,
        line.inprbe,
        line.ionrbe,
        line.irnrbe,
        line.ivaabe,
        line.jgdtbe,
        line.kobdbe,
        line.kopcbe,
        line.lvnrbe,
        line.odaabe,
        line.oskdbe,
        line.ovaabe,
        line.ovkgbe,
        line.rdkdbe,
        line.stikbe,
        line.tbaabe,
        line.tibdbe,
        line.toaabe,
        line.vpombe,
        line.vrklbe,
        line.zbprbe,
        line.load_datetime,
        line.end_datetime,
        line.start_datetime,
        line.record_source,
        line.btkdbe_description,
        header.purchase_order_bk,
        header.goods_received_bk,
        line.arnrbe AS article_bk,
        header.purchase_order_hk,
        header.goods_received_hk,
        concat(header.purchase_order_bk, "||", line.irnrbe, "||", line.arnrbe) AS purchase_order_line_bk,
        concat(header.goods_received_bk, "||", line.irnrbe, "||", line.arnrbe) AS goods_received_line_bk
    FROM line
    INNER JOIN header
        ON
            line.ionrbe = header.ionrba
            AND cast(line.start_datetime AS date) BETWEEN
            cast(header.start_datetime AS date) AND coalesce(cast(header.end_datetime AS date), "9999-12-31")
    QUALIFY row_number() OVER (
        PARTITION BY header.goods_received_bk, line.irnrbe, line.arnrbe, line.start_datetime
        ORDER BY line.start_datetime ASC, line.load_datetime DESC
    ) = 1
),

{# Hashing the BKs #}
apply_hashing AS (
    SELECT
        *,
        upper(to_hex(sha256(purchase_order_line_bk))) AS purchase_order_line_hk,
        upper(to_hex(sha256(goods_received_line_bk))) AS goods_received_line_hk,
        upper(to_hex(sha256(article_bk))) AS article_hk,
        upper(to_hex(sha256(purchase_order_bk || purchase_order_line_bk || article_bk)))
            AS purchase_order_line_article_hk,
        upper(to_hex(sha256(goods_received_bk || goods_received_line_bk || article_bk)))
            AS goods_received_line_article_hk,
        upper(to_hex(sha256(goods_received_bk || article_bk)))
            AS goods_received_article_hk

    FROM unique_key_creation
),

{# End date the goods_received_line_article_hk based on the start_datetime.
This is needed so that lines with the same ID but different received
dates can be active at the same time and we don't loose history. #}
end_date_calculation AS (
    SELECT
        * EXCEPT (end_datetime),
        lag(start_datetime) OVER (
            PARTITION BY goods_received_line_article_hk
            ORDER BY start_datetime DESC
        ) AS end_datetime
    FROM apply_hashing
)

SELECT *
FROM end_date_calculation
WHERE
    {# Clause to remove headers where lines only contain negative amounts,
    as per agreement with data architect and BI team on 12/02/2025. #}
    ovaabe >= 0 AND bsaabe >= 0
