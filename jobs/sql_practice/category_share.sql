-- 品类销售排行与占比：TopN + 占比计算
-- 考点：窗口函数 ROW_NUMBER + 子查询做分母
WITH cat_gmv AS (
    SELECT category, SUM(amount) AS gmv
    FROM dwd_order_detail
    GROUP BY category
),
ranked AS (
    SELECT category,
           gmv,
           ROW_NUMBER() OVER (ORDER BY gmv DESC) AS rn,
           SUM(gmv) OVER ()                      AS total_gmv
    FROM cat_gmv
)
SELECT category,
       ROUND(gmv, 2)                                AS gmv,
       rn                                           AS rank_no,
       ROUND(gmv / total_gmv * 100, 2)              AS ratio_pct
FROM ranked
ORDER BY rn;
