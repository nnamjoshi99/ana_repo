
DECLARE 
    start_dt    TIMESTAMP := from_dt;
    end_dt      TIMESTAMP := to_dt;

BEGIN

-- Changing the from date to process the data that hasn't been processed in the staging.
IF start_dt = '1900-01-01'::TIMESTAMP THEN 
    SELECT 
        -- Since this domain involves multiple data sources, so taking the one that has the minimum upload date.
		CASE WHEN MIN(kortex_upld_ts) IS NULL THEN '1900-01-01'::TIMESTAMP
		ELSE MIN(kortex_upld_ts) END INTO start_dt
    FROM (
        -- This will filter out the maximum upload timestamp for all the sources.
        SELECT 
            src_nm
            ,MAX(kortex_upld_ts) AS kortex_upld_ts
        FROM sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc
        GROUP BY 1
    );
END IF;

/* Code Section - BEGIN */

DROP TABLE IF EXISTS mkt_prfmnc_pos_nielsen_mkt_upc_tmp;
CREATE TEMP TABLE mkt_prfmnc_pos_nielsen_mkt_upc_tmp AS (
  WITH nielsen_mrkt_ref_tmp AS (
    SELECT DISTINCT
      "market key" AS mkt_key,
      UPPER("market description") AS mkt_desc
    FROM stage.us_nielsen_mkt_upc_market_upc_mrkt_ref mkt
  ),
  nielsen_prod_ref_tmp AS (
    SELECT
      *,
      CASE
        WHEN TRIM(upc) ~ '^[0-9]+$' THEN matrl_mstr.f_upc_check_digit_fix(
          concat(
            (
              replicate('0', 11 - len(convert (bigint, TRIM(upc))))
            ),
            (convert (bigint, TRIM(upc)))
          )
        )
        ELSE TRIM(upc)
      END AS gtin
    FROM
      (
        SELECT
          DISTINCT "product key" AS product_key,
          upc,
          UPPER("kel_corporate") AS corp_nm,
          UPPER("kel_category") AS catg_nm,
          UPPER("kel_brand") AS brand_nm,
          UPPER("kel_segment") AS mkt_prfmnc_prod_seg_cd,
          UPPER("kel_sub-segment") AS mkt_prfmnc_prod_sub_seg_cd,
          UPPER("kel_size") AS prod_size_desc,
          UPPER("kel_vendor") AS vndr_nm
        FROM
          stage.us_nielsen_mkt_upc_market_upc_prdc_ref
    )
  ),
  nielsen_prd_ref_tmp AS (
    SELECT DISTINCT
      "period key" AS period_key,
      TO_DATE(RIGHT(TRIM("period description"), 8), 'MM/DD/YY', FALSE) AS fisc_dt
    FROM
      stage.us_nielsen_mkt_upc_market_upc_prd_ref
    WHERE
      RIGHT(TRIM("period description"), 8) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$'
  ),
  nielsen_mkt_fact_tmp AS (
    SELECT
      "market key" AS mkt_key,
      "product key" AS product_key,
      "period key" AS period_key,
      'LW'::TEXT AS pd_desc,
      CASE WHEN TRIM("$") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("$")::DECIMAL(20,6) ELSE 0.0 END AS sale_val,
      CASE WHEN TRIM("units") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("units")::DECIMAL(20,6) ELSE 0.0 END AS sale_qty,
      CASE WHEN TRIM("eq") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("eq")::DECIMAL(20,6) ELSE 0.0 END AS sale_vol_lb_val,
      CASE WHEN TRIM("%acv") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("%acv")::DECIMAL(20,6) ELSE 0.0 END AS acv_reach_pct,
      CASE WHEN TRIM("est market acv") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("est market acv")::DECIMAL(20,6) ELSE 0.0 END AS estmt_mkt_acv_val,
      CASE WHEN TRIM("any promo $") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any promo $")::DECIMAL(20,6) ELSE 0.0 END AS any_promo_sale_val,
      CASE WHEN TRIM("any promo units") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any promo units")::DECIMAL(20,6) ELSE 0.0 END AS any_promo_unit_qty,
      CASE WHEN TRIM("any promo base $") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any promo base $")::DECIMAL(20,6) ELSE 0.0 END AS any_promo_base_sale_val,
      CASE WHEN TRIM("any promo base units") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any promo base units")::DECIMAL(20,6) ELSE 0.0 END AS any_promo_base_unit_qty,
      CASE WHEN TRIM("% subsidized units") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("% subsidized units")::DECIMAL(20,6) ELSE 0.0 END AS subsdz_unit_pct,
      CASE WHEN TRIM("any promo unit price % disc") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any promo unit price % disc")::DECIMAL(20,6) ELSE 0.0 END AS any_promo_unit_price_disc_pct,
      CASE WHEN TRIM("feat or disp cww of %acv chg ya") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("feat or disp cww of %acv chg ya")::DECIMAL(20,6) ELSE 0.0 END AS featur_disp_cww_acv_chg_yr_ago_pct,
      CASE WHEN TRIM("any disp units") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any disp units")::DECIMAL(20,6) ELSE 0.0 END AS any_disp_unit_qty,
      CASE WHEN TRIM("any disp # disp") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("any disp # disp")::DECIMAL(20,6) ELSE 0.0 END AS any_disp_disp_qty,
      src_nm,
      kortex_dprct_ts,
      kortex_upld_ts
    FROM stage.us_nielsen_mkt_upc_market_upc_fct
    WHERE TRUE
     AND kortex_upld_ts BETWEEN start_dt AND end_dt
  ),
  mkt_upc_join_tmp AS (
    SELECT
      f.period_key,
      r.mkt_desc,
      max(f.mkt_key:: int)::TEXT AS mkt_key
      FROM
      nielsen_mkt_fact_tmp f
      INNER JOIN nielsen_mrkt_ref_tmp r USING(mkt_key)
      INNER JOIN nielsen_prd_ref_tmp p USING (period_key)
    GROUP BY
      1,
      2
  ),
  mkt_plan_to_map AS (    
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER () OVER (
          PARTITION BY mkt_desc
          ORDER BY
            CASE src
              WHEN 'nomh' THEN 1
              WHEN 'mkt_plan_to' THEN 2
              WHEN 'ta_rom_map_ta' THEN 3
              WHEN 'ta_rom_map_rom' THEN 4
            END
        ) AS rn
      FROM (
        SELECT DISTINCT 
          'nomh' AS src, UPPER(retailer) AS mkt_desc, LPAD("retailer id", 10, '0') AS plan_to_nbr
        FROM stage.sales_user_item_never_out_must_have_item_never_out_must_have  
        WHERE "retailer id" IS NOT NULL AND "retailer id" != ''
        UNION ALL
        SELECT DISTINCT
          'mkt_plan_to' AS src, UPPER(mkt_prfmnc_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_mkt_plan_to
        WHERE plan_to_nbr IS NOT NULL
        UNION ALL
        SELECT DISTINCT
          'ta_rom_map_ta' AS src, UPPER(trading_area_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
        WHERE trading_area_desc != ''
        UNION ALL
        SELECT DISTINCT
          'ta_rom_map_rom' AS src, UPPER(rest_of_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
        WHERE rest_of_mkt_desc != ''
      )
    )
    WHERE rn = 1
  ),
  bi_wkly_retlr AS (
    SELECT DISTINCT
      UPPER(mkt_desc) AS mkt_desc
    FROM sales_prfmnc_eval.mkt_prfmnc_bi_wkly_retlr
  ),
  nielsen_mkt_upc AS (
    SELECT
      mkt_key,
      mkt_desc,
      gtin,
      'UPC'::TEXT AS gtin_type_cd,
      corp_nm,
      pd_desc,
      sale_val,
      sale_qty,
      sale_vol_lb_val,
      any_promo_sale_val,
      any_promo_unit_qty,
      any_promo_base_sale_val,
      any_promo_base_unit_qty,
      subsdz_unit_pct,
      any_promo_unit_price_disc_pct,
      featur_disp_cww_acv_chg_yr_ago_pct,
      acv_reach_pct,
      estmt_mkt_acv_val,
      'dol'::TEXT AS val_curr_cd,
      any_disp_unit_qty,
      any_disp_disp_qty,
      'each'::TEXT AS qty_uom,
      CASE WHEN bw.mkt_desc IS NOT NULL THEN 'L2W' ELSE 'LW' END AS retlr_pace_cd,
      fisc_dt,
      catg_nm,
      brand_nm,
      mkt_prfmnc_prod_seg_cd,
      mkt_prfmnc_prod_sub_seg_cd,
      prod_size_desc,
      vndr_nm,
      plan_to_nbr,
      src_nm,
      Md5(src_nm || mkt_desc || gtin || corp_nm || pd_desc || fisc_dt) AS hash_key,
      kortex_dprct_ts,
      kortex_upld_ts,
      CURRENT_TIMESTAMP::TIMESTAMP AS kortex_cre_ts,
      CURRENT_TIMESTAMP::TIMESTAMP AS kortex_updt_ts
    FROM
      nielsen_mkt_fact_tmp fact
      INNER JOIN nielsen_prod_ref_tmp USING (product_key)
      INNER JOIN nielsen_mrkt_ref_tmp USING (mkt_key)
      INNER JOIN nielsen_prd_ref_tmp USING (period_key)
      INNER JOIN mkt_upc_join_tmp USING(period_key,mkt_key,mkt_desc)
      LEFT JOIN mkt_plan_to_map USING (mkt_desc)
      LEFT JOIN bi_wkly_retlr bw USING (mkt_desc)
  )
  SELECT * FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY mkt_desc, gtin, corp_nm, fisc_dt ORDER BY kortex_upld_ts DESC) AS rn
    FROM nielsen_mkt_upc
  )
  WHERE rn = 1
);

RAISE INFO 'Successfully created temp table mkt_prfmnc_pos_nielsen_mkt_upc_tmp.';
/* Code Section - END */

-- Deleting the updated records from the temporary
DELETE FROM sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc
USING mkt_prfmnc_pos_nielsen_mkt_upc_tmp act
WHERE
  act.mkt_desc = mkt_prfmnc_pos_mkt_upc.mkt_desc
  AND act.gtin = mkt_prfmnc_pos_mkt_upc.gtin
  AND act.corp_nm = mkt_prfmnc_pos_mkt_upc.corp_nm
  AND act.pd_desc = mkt_prfmnc_pos_mkt_upc.pd_desc
  AND act.fisc_dt = mkt_prfmnc_pos_mkt_upc.fisc_dt
  AND act.src_nm = mkt_prfmnc_pos_mkt_upc.src_nm
;


-- Insert the remaining records in the delta data to the final persistent data
INSERT INTO sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc (
  SELECT 
    mkt_key,
    mkt_desc,
    gtin,
    gtin_type_cd,
    corp_nm,
    pd_desc,
    sale_val,
    sale_qty,
    sale_vol_lb_val,
    any_promo_sale_val,
    any_promo_unit_qty,
    any_promo_base_sale_val,
    any_promo_base_unit_qty,
    subsdz_unit_pct,
    any_promo_unit_price_disc_pct,
    featur_disp_cww_acv_chg_yr_ago_pct,
    acv_reach_pct,
    estmt_mkt_acv_val,
    val_curr_cd,
    any_disp_unit_qty,
    any_disp_disp_qty,
    qty_uom,
    retlr_pace_cd,
    fisc_dt,
    catg_nm,
    brand_nm,
    mkt_prfmnc_prod_seg_cd,
    mkt_prfmnc_prod_sub_seg_cd,
    prod_size_desc,
    vndr_nm,
    plan_to_nbr,
    src_nm,
    hash_key,
    kortex_dprct_ts,
    kortex_upld_ts,
    kortex_cre_ts,
    kortex_updt_ts
  FROM mkt_prfmnc_pos_nielsen_mkt_upc_tmp
);

DROP TABLE IF EXISTS mkt_prfmnc_pos_nielsen_mkt_upc_tmp;

/* Merge Section - END */


DROP TABLE IF EXISTS mkt_prfmnc_pos_nielsen_mkt_upc_2w_8w_tmp;
CREATE TEMP TABLE mkt_prfmnc_pos_nielsen_mkt_upc_2w_8w_tmp AS (
  WITH osa_proxy_raw AS (
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY mkt_desc, period_desc, upc, corp_nm --, src_nm
          ORDER BY kortex_upld_ts DESC
        ) AS rn
      FROM (
        SELECT
          UPPER("market description") AS mkt_desc,
          TRIM("upc") AS upc,
          UPPER("period sum") AS period_desc,
          UPPER("kel_corporate") AS corp_nm,
          UPPER("kel_category") AS catg_nm,
          UPPER("kel_brand") AS brand_nm,
          UPPER("kel_segment") AS mkt_prfmnc_prod_seg_cd,
          UPPER("kel_sub-segment") AS mkt_prfmnc_prod_sub_seg_cd,
          UPPER("kel_size") AS prod_size_desc,
          UPPER("kel_vendor") AS vndr_nm,
          CASE WHEN TRIM("%acv reach") ~ '^\\-?[0-9]*\\.?[0-9]+$' THEN TRIM("%acv reach")::FLOAT8 ELSE 0 END AS acv_reach_pct,
          src_nm,
          kortex_upld_ts,
          kortex_dprct_ts
        FROM stage.us_nielsen_mkt_upc_osa_proxy_market_upc_osa_proxy_fct
        WHERE
          TRUE
          AND kortex_upld_ts BETWEEN start_dt AND end_dt
          AND RIGHT(TRIM("period sum"), 8) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$'
      )
    )
    WHERE rn = 1
  ),
  acv_reach_tmfrm AS (
    SELECT
      *,
      CASE
        WHEN TRIM(upc) ~ '^[0-9]+$' THEN matrl_mstr.f_upc_check_digit_fix(
          concat(
            (
              replicate('0', 11 - len(convert (bigint, TRIM(upc))))
            ),
            (convert (bigint, TRIM(upc)))
          )
        )
        ELSE TRIM(upc)
      END AS gtin
    FROM (
      SELECT
        mkt_desc,
        upc,
        TO_DATE(RIGHT(TRIM(period_desc), 8), 'MM/DD/YY', FALSE) AS fisc_dt,
        CASE
          WHEN UPPER(period_desc) LIKE 'LAST 2 WEEK%' THEN 'L2W'
          WHEN UPPER(period_desc) LIKE 'LAST 8 WEEK%' THEN 'L8W'
        END AS pd_desc,
        corp_nm,
        catg_nm,
        brand_nm,
        mkt_prfmnc_prod_seg_cd,
        mkt_prfmnc_prod_sub_seg_cd,
        prod_size_desc,
        vndr_nm,
        acv_reach_pct,
        src_nm,
        kortex_upld_ts,
        kortex_dprct_ts
      FROM osa_proxy_raw
    )
  ),
  mkt_desc_to_key AS (
    SELECT * FROM (
      SELECT
        mkt_desc,
        mkt_key,
        ROW_NUMBER() OVER(PARTITION BY mkt_desc ORDER BY kortex_upld_ts DESC) AS rn
        FROM (
          SELECT DISTINCT
            "market key" AS mkt_key, UPPER("market description") AS mkt_desc, kortex_upld_ts
          FROM stage.us_nielsen_mkt_upc_market_upc_mrkt_ref
        )
    )
    WHERE rn = 1
  ),
  mkt_plan_to_map AS (    
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER () OVER (
          PARTITION BY mkt_desc
          ORDER BY
            CASE src
              WHEN 'nomh' THEN 1
              WHEN 'mkt_plan_to' THEN 2
              WHEN 'ta_rom_map_ta' THEN 3
              WHEN 'ta_rom_map_rom' THEN 4
            END
        ) AS rn
      FROM (
        SELECT DISTINCT 
          'nomh' AS src, UPPER(retailer) AS mkt_desc, LPAD("retailer id", 10, '0') AS plan_to_nbr
        FROM stage.sales_user_item_never_out_must_have_item_never_out_must_have  
        WHERE "retailer id" IS NOT NULL AND "retailer id" != ''
        UNION ALL
        SELECT DISTINCT
          'mkt_plan_to' AS src, UPPER(mkt_prfmnc_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_mkt_plan_to
        WHERE plan_to_nbr IS NOT NULL
        UNION ALL
        SELECT DISTINCT
          'ta_rom_map_ta' AS src, UPPER(trading_area_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
        WHERE trading_area_desc != ''
        UNION ALL
        SELECT DISTINCT
          'ta_rom_map_rom' AS src, UPPER(rest_of_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
        FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
        WHERE rest_of_mkt_desc != ''
      )
    )
    WHERE rn = 1
  ),
  bi_wkly_retlr AS (
    SELECT DISTINCT
      UPPER(mkt_desc) AS mkt_desc
    FROM sales_prfmnc_eval.mkt_prfmnc_bi_wkly_retlr
  ),
  acv_tmfrm_retlr_pace AS (
    SELECT
      mkt_key,
      mkt_desc,
      gtin,
      'UPC'::TEXT AS gtin_type_cd,
      corp_nm,
      pd_desc,
      NULL::FLOAT8 AS sale_val,
      NULL::FLOAT8 AS sale_qty,
      NULL::FLOAT8 AS sale_vol_lb_val,
      NULL::FLOAT8 AS any_promo_sale_val,
      NULL::FLOAT8 AS any_promo_unit_qty,
      NULL::FLOAT8 AS any_promo_base_sale_val,
      NULL::FLOAT8 AS any_promo_base_unit_qty,
      NULL::FLOAT8 AS subsdz_unit_pct,
      NULL::FLOAT8 AS any_promo_unit_price_disc_pct,
      NULL::FLOAT8 AS featur_disp_cww_acv_chg_yr_ago_pct,
      acv_reach_pct,
      NULL::FLOAT8 AS estmt_mkt_acv_val,
      NULL::TEXT AS val_curr_cd,
      NULL::FLOAT8 AS any_disp_unit_qty,
      NULL::FLOAT8 AS any_disp_disp_qty,
      NULL::TEXT AS qty_uom,
      CASE WHEN bw.mkt_desc IS NOT NULL THEN 'L2W' ELSE 'LW' END AS retlr_pace_cd,
      fisc_dt,
      catg_nm,
      brand_nm,
      mkt_prfmnc_prod_seg_cd,
      mkt_prfmnc_prod_sub_seg_cd,
      prod_size_desc,
      vndr_nm,
      plan_to_nbr,
      src_nm,
      Md5(src_nm || mkt_desc || gtin || corp_nm || pd_desc || fisc_dt) AS hash_key,
      kortex_dprct_ts,
      kortex_upld_ts,
      CURRENT_TIMESTAMP::TIMESTAMP AS kortex_cre_ts,
      CURRENT_TIMESTAMP::TIMESTAMP AS kortex_updt_ts
    FROM acv_reach_tmfrm
    LEFT JOIN mkt_plan_to_map USING (mkt_desc)
    LEFT JOIN bi_wkly_retlr bw USING (mkt_desc)
    LEFT JOIN mkt_desc_to_key USING (mkt_desc)
  )
  SELECT * FROM acv_tmfrm_retlr_pace
);
RAISE INFO 'Successfully created temp table mkt_prfmnc_pos_nielsen_mkt_upc_2w_8w_tmp.';

-- Deleting the updated records from the temporary
DELETE FROM sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc
USING mkt_prfmnc_pos_nielsen_mkt_upc_2w_8w_tmp act
WHERE
  act.mkt_desc = mkt_prfmnc_pos_mkt_upc.mkt_desc
  AND act.gtin = mkt_prfmnc_pos_mkt_upc.gtin
  AND act.corp_nm = mkt_prfmnc_pos_mkt_upc.corp_nm
  AND act.pd_desc = mkt_prfmnc_pos_mkt_upc.pd_desc
  AND act.fisc_dt = mkt_prfmnc_pos_mkt_upc.fisc_dt
  AND act.src_nm = mkt_prfmnc_pos_mkt_upc.src_nm
;


-- Insert the remaining records in the delta data to the final persistent data
INSERT INTO sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc (
  SELECT 
    mkt_key,
    mkt_desc,
    gtin,
    gtin_type_cd,
    corp_nm,
    pd_desc,
    sale_val,
    sale_qty,
    sale_vol_lb_val,
    any_promo_sale_val,
    any_promo_unit_qty,
    any_promo_base_sale_val,
    any_promo_base_unit_qty,
    subsdz_unit_pct,
    any_promo_unit_price_disc_pct,
    featur_disp_cww_acv_chg_yr_ago_pct,
    acv_reach_pct,
    estmt_mkt_acv_val,
    val_curr_cd,
    any_disp_unit_qty,
    any_disp_disp_qty,
    qty_uom,
    retlr_pace_cd,
    fisc_dt,
    catg_nm,
    brand_nm,
    mkt_prfmnc_prod_seg_cd,
    mkt_prfmnc_prod_sub_seg_cd,
    prod_size_desc,
    vndr_nm,
    plan_to_nbr,
    src_nm,
    hash_key,
    kortex_dprct_ts,
    kortex_upld_ts,
    kortex_cre_ts,
    kortex_updt_ts
  FROM mkt_prfmnc_pos_nielsen_mkt_upc_2w_8w_tmp
);

DROP TABLE IF EXISTS mkt_plan_to_map_tmp;
CREATE TEMP TABLE mkt_plan_to_map_tmp AS (
  SELECT * FROM (
    SELECT
      *,
      ROW_NUMBER () OVER (
        PARTITION BY mkt_desc
        ORDER BY
          CASE src
            WHEN 'nomh' THEN 1
            WHEN 'mkt_plan_to' THEN 2
            WHEN 'ta_rom_map_ta' THEN 3
            WHEN 'ta_rom_map_rom' THEN 4
          END
      ) AS rn
    FROM (
      SELECT DISTINCT 
        'nomh' AS src, UPPER(retailer) AS mkt_desc, LPAD("retailer id", 10, '0') AS plan_to_nbr
      FROM stage.sales_user_item_never_out_must_have_item_never_out_must_have  
      WHERE "retailer id" IS NOT NULL AND "retailer id" != ''
      UNION ALL
      SELECT DISTINCT
        'mkt_plan_to' AS src, UPPER(mkt_prfmnc_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
      FROM sales_prfmnc_eval.mkt_prfmnc_mkt_plan_to
      WHERE plan_to_nbr IS NOT NULL
      UNION ALL
      SELECT DISTINCT
        'ta_rom_map_ta' AS src, UPPER(trading_area_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
      FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
      WHERE trading_area_desc != ''
      UNION ALL
      SELECT DISTINCT
        'ta_rom_map_rom' AS src, UPPER(rest_of_mkt_desc) AS mkt_desc, LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr
      FROM sales_prfmnc_eval.mkt_prfmnc_trading_area_rest_of_mkt_map mptaromm
      WHERE rest_of_mkt_desc != ''
    )
  )
  WHERE rn = 1
);

UPDATE sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc act
SET
  plan_to_nbr = tmp.plan_to_nbr
FROM mkt_plan_to_map_tmp tmp
WHERE
  act.mkt_desc = tmp.mkt_desc
;

DROP TABLE IF EXISTS retlr_pace_tmp;
CREATE TEMP TABLE retlr_pace_tmp AS (
  SELECT DISTINCT
    'L2W' AS retlr_pace_cd,
    UPPER(mkt_desc) AS mkt_desc
  FROM sales_prfmnc_eval.mkt_prfmnc_bi_wkly_retlr

  UNION ALL

  SELECT DISTINCT
    'LW' AS retlr_pace_cd,
    mkt_desc
  FROM sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc
  WHERE mkt_desc NOT IN (
    SELECT UPPER(mkt_desc) FROM sales_prfmnc_eval.mkt_prfmnc_bi_wkly_retlr
  )
);

UPDATE sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc act
SET
  retlr_pace_cd = tmp.retlr_pace_cd
FROM retlr_pace_tmp tmp
WHERE
  act.mkt_desc = tmp.mkt_desc
;

-- Raise the error info in case of a failure because of any exception
EXCEPTION WHEN OTHERS THEN 
    RAISE INFO 'An exception occurred in sales_prfmnc_eval.mkt_prfmnc_pos_mkt_upc.';
    RAISE INFO 'Error code: %, Error message: %', SQLSTATE, SQLERRM;
COMMIT;

END;
