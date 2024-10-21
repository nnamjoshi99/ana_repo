DECLARE 
    start_dt    TIMESTAMP := from_dt;
    end_dt      TIMESTAMP := to_dt;

BEGIN

-- Changing the from date to process the data that hasn't been processed in the staging.
IF start_dt = '1900-01-01'::TIMESTAMP THEN 
    SELECT 
		CASE WHEN MAX(kortex_upld_ts) IS NULL THEN '1900-01-01'::TIMESTAMP
		ELSE MAX(kortex_upld_ts) END INTO start_dt
    FROM sales_exec.cust_serv_cust_order_fulfl;
END IF;

--/* Code Section - BEGIN */

-- Cleaning the order fulfillment data
DROP TABLE IF EXISTS order_fullfillment_clean;
CREATE TEMP TABLE order_fullfillment_clean AS (
    SELECT
        src_nm
        ,CASE WHEN TRIM(po_number)='' THEN NULL ELSE po_number END AS po_nbr
        ,po_type AS po_type 
        ,CASE WHEN TRIM("internal_item_id") ~ '^[0-9]+$' THEN TRIM("internal_item_id") ELSE NULL END AS intrnl_item_id
        ,CASE WHEN TRIM("internal_dc_id") ~ '^[0-9]+$' THEN TRIM("internal_dc_id") ELSE NULL END AS intrnl_dc_id
        ,CASE WHEN TRIM(create_dt) = '' THEN NULL ELSE create_dt::DATE END AS po_cre_dt
        ,CASE WHEN TRIM(mabd) = '' THEN NULL ELSE TRIM(mabd)::DATE END AS po_must_arrv_by_dt
        ,CASE WHEN TRIM("order_qty") ~ '^[0-9]+$' THEN TRIM("order_qty")::INT ELSE 0 END AS po_order_qty
        ,CASE WHEN TRIM("received_early") ~ '^[0-9]+$' THEN TRIM("received_early")::INT ELSE 0 END AS po_recv_early_qty
        ,CASE WHEN TRIM("received_on_time") ~ '^[0-9]+$' THEN TRIM("received_on_time")::INT ELSE 0 END AS po_recv_on_time_qty
        ,CASE WHEN TRIM("received_late") ~ '^[0-9]+$' THEN TRIM("received_late")::INT ELSE 0 END AS po_recv_late_qty
        ,CASE WHEN TRIM("short") ~ '^[0-9]+$' THEN TRIM("short")::INT ELSE 0 END AS po_short_qty
        ,CASE WHEN TRIM("over") ~ '^[0-9]+$' THEN TRIM("over")::INT ELSE 0 END AS po_over_qty
        ,kortex_dprct_ts
        ,kortex_upld_ts
    FROM stage."e2open_order_fulfl_orderfulfill-corporate"
    WHERE kortex_upld_ts BETWEEN start_dt AND end_dt
);

-- Cleaning the dc master table and creating a map between the internal dc id and retailr name
DROP TABLE IF EXISTS order_planto;
CREATE TEMP TABLE order_planto AS (
    WITH dc_master_clean AS (
        SELECT 
            CASE WHEN TRIM("internal_dc_id") ~ '^[0-9]+$' THEN TRIM("internal_dc_id")::BIGINT ELSE 0 END AS intrnl_dc_id
            ,retailer AS plan_to_nm
        FROM stage."e2open_distbn_ctr_mstr_dcmaster-corporate"
        -- doing group by just to remove duplicates if any!
        GROUP BY 1,2
    )
    SELECT 
        a.* 
        ,b.plan_to_nm
    FROM order_fullfillment_clean a 
    LEFT JOIN dc_master_clean b 
    ON a.intrnl_dc_id = b.intrnl_dc_id
);


DROP TABLE IF EXISTS order_planto_gtin;
CREATE TEMP TABLE order_planto_gtin AS (
    WITH item_master_clean AS (
        SELECT 
            intrnl_item_id
            ,matrl_mstr.f_upc_check_digit_fix(concat((replicate('0', 11-len(convert (bigint, upc)))), (convert (bigint, upc)))) AS gtin 
        FROM (
            SELECT 
                CASE WHEN TRIM("internal_item_id") ~ '^[0-9]+$' THEN TRIM("internal_item_id")::BIGINT ELSE 0 END AS intrnl_item_id
                ,CASE WHEN TRIM("retailer_upc") ~ '^[0-9]+$' THEN TRIM("retailer_upc")::BIGINT ELSE 0 END AS upc
            FROM stage."e2open_item_mstr_itemmaster-corporate"
            -- removes the column without upc
            WHERE upc IS NOT NULL OR TRIM(upc) != ''
            -- doing group by just to remove duplicates if any!
            GROUP BY 1,2
        )
    )
    SELECT 
        a.*
        ,b.gtin 
        ,c.fisc_wk_end_dt
    FROM order_planto a
    LEFT JOIN item_master_clean b 
        ON a.intrnl_item_id = b.intrnl_item_id
    LEFT JOIN fin_acctg_ops.ref_fisc_cal_wk  c
        ON a.po_cre_dt BETWEEN c.fisc_wk_start_dt AND c.fisc_wk_end_dt
);


DROP TABLE IF EXISTS cust_serv_cust_order_fulfl_temp;
CREATE TEMP TABLE cust_serv_cust_order_fulfl_temp AS (
    SELECT
         po_nbr
        ,po_type
        ,plan_to_nm
        ,gtin 
        ,intrnl_item_id
        ,intrnl_dc_id
        ,po_cre_dt
        ,po_must_arrv_by_dt
        ,po_order_qty 
        ,po_recv_early_qty 
        ,po_recv_on_time_qty
        ,po_recv_late_qty
        ,po_short_qty
        ,po_over_qty
        ,fisc_wk_end_dt
        ,src_nm
        ,Md5(src_nm || po_nbr || po_cre_dt || intrnl_dc_id || intrnl_item_id) AS hash_key
        ,kortex_dprct_ts
        ,kortex_upld_ts
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY po_nbr, intrnl_item_id, intrnl_dc_id, po_cre_dt, src_nm ORDER BY kortex_upld_ts DESC) AS rn
        FROM order_planto_gtin
    )
    WHERE rn = 1
);
--/* Code Section - END */

/* Merge Section - BEGIN */

-- Updating the records from the original table which match with the delta
UPDATE sales_exec.cust_serv_cust_order_fulfl act
SET 
    po_type                 = tmp.po_type,
    plan_to_nm              = tmp.plan_to_nm,
    gtin                    = tmp.gtin,
    po_must_arrv_by_dt      = tmp.po_must_arrv_by_dt,
    po_order_qty            = tmp.po_order_qty,
    po_recv_early_qty       = tmp.po_recv_early_qty,
    po_recv_on_time_qty     = tmp.po_recv_on_time_qty,
    po_recv_late_qty        = tmp.po_recv_late_qty,
    po_short_qty            = tmp.po_short_qty,
    po_over_qty             = tmp.po_over_qty,
    kortex_updt_ts          = CURRENT_TIMESTAMP 
FROM cust_serv_cust_order_fulfl_temp tmp
WHERE   act.src_nm          = tmp.src_nm
    AND act.po_nbr          = tmp.po_nbr
    AND act.intrnl_dc_id    = tmp.intrnl_dc_id
    AND act.intrnl_item_id  = tmp.intrnl_item_id
    AND act.po_cre_dt       = tmp.po_cre_dt;

-- Deleting the updated records from the temporary
DELETE FROM cust_serv_cust_order_fulfl_temp 
USING sales_exec.cust_serv_cust_order_fulfl act
WHERE   cust_serv_cust_order_fulfl_temp.src_nm          = act.src_nm
    AND cust_serv_cust_order_fulfl_temp.po_nbr          = act.po_nbr
    AND cust_serv_cust_order_fulfl_temp.intrnl_dc_id    = act.intrnl_dc_id
    AND cust_serv_cust_order_fulfl_temp.intrnl_item_id  = act.intrnl_item_id
    AND cust_serv_cust_order_fulfl_temp.po_cre_dt       = act.po_cre_dt;


-- Insert the remaining records in the delta data to the final persistent data
INSERT INTO sales_exec.cust_serv_cust_order_fulfl (
    SELECT
         po_nbr
        ,po_type
        ,plan_to_nm
        ,gtin 
        ,'UPC' AS gtin_type_cd 
        ,intrnl_item_id
        ,intrnl_dc_id
        ,po_cre_dt
        ,fisc_wk_end_dt
        ,po_must_arrv_by_dt
        ,po_order_qty 
        ,po_recv_early_qty 
        ,po_recv_on_time_qty
        ,po_recv_late_qty
        ,po_short_qty
        ,po_over_qty
        ,src_nm
        ,hash_key
        ,kortex_dprct_ts
        ,kortex_upld_ts
        ,CURRENT_TIMESTAMP AS kortex_cre_ts 
        ,CURRENT_TIMESTAMP AS kortex_updt_ts 
    FROM cust_serv_cust_order_fulfl_temp
);

/* Merge Section - END */

-- Raise the error info in case of a failure because of any exception
EXCEPTION WHEN OTHERS THEN 
    RAISE INFO 'An exception occurred in sales_exec.cust_serv_cust_order_fulfl.';
    RAISE INFO 'Error code: %, Error message: %', SQLSTATE, SQLERRM;
COMMIT;

END;
-- sample file
