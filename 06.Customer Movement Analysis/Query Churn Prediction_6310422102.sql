SELECT YEAR_MONTH, STATUS, CASE WHEN STATUS = 'CHURN' THEN (-1)*COUNT_CUST ELSE COUNT_CUST END AS COUNT_CUST 
FROM ( 
    SELECT YEAR_MONTH, STATUS, COUNT(DISTINCT CUST_CODE) AS COUNT_CUST
    FROM ( 
        -- NEW,REPEAT,REACTIVATE 
        SELECT  YEAR_MONTH, CUST_CODE, REGISTER_DATE, --SHOP_MONTH, PREVIOUS_MONTH, 
                DATE_DIFF(SHOP_MONTH, PREVIOUS_MONTH, MONTH) AS R, 
                CASE WHEN SHOP_MONTH = REGISTER_DATE THEN 'NEW' 
                WHEN DATE_DIFF(SHOP_MONTH, PREVIOUS_MONTH, MONTH) = 1 THEN 'REPEAT' 
                WHEN DATE_DIFF(SHOP_MONTH, PREVIOUS_MONTH, MONTH) > 1 THEN 'REACTIVATE' END AS STATUS 
        FROM (
            -- PREVIOUS_MONTH
            SELECT CUST_CODE, YEAR_MONTH, SHOP_MONTH, REGISTER_DATE, LAG(SHOP_MONTH) OVER (PARTITION BY CUST_CODE ORDER BY SHOP_MONTH) AS PREVIOUS_MONTH 
            FROM (
                SELECT CUST_CODE, YEAR_MONTH, SHOP_MONTH, REGISTER_DATE
                FROM (
                    SELECT ALL_CUST.CUST_CODE, SHOP_MONTH, REGISTER_DATE
                    FROM (
                        -- ALL_CUST
                        SELECT CUST_CODE, DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)), MONTH) AS SHOP_MONTH 
                        FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                        WHERE CUST_CODE IS NOT NULL
                        GROUP BY 1,2
                    ) ALL_CUST LEFT JOIN (
                        -- REGISTER_DATE
                        SELECT CUST_CODE, DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(MIN(SHOP_DATE) AS STRING)),MONTH) AS REGISTER_DATE 
                        FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                        WHERE CUST_CODE IS NOT NULL
                        GROUP BY 1
                    ) REGISTER ON ALL_CUST.CUST_CODE = REGISTER.CUST_CODE
                ) ALL_CUST_2 LEFT JOIN (
                    -- YEAR_MONTH PERIOD
                    SELECT DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)), MONTH) AS YEAR_MONTH 
                    FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                    WHERE CUST_CODE IS NOT NULL
                    GROUP BY 1
                ) MONTH_PERIOD ON ALL_CUST_2.SHOP_MONTH = MONTH_PERIOD.YEAR_MONTH
            ) PREV_M
        ) N_R_R
        UNION ALL 
        -- CHURN
        SELECT YEAR_MONTH, CUST_CODE, REGISTER_DATE, R, STATUS --, LAST_DATE
        FROM (
            SELECT CUST_CODE, STATUS, REGISTER_DATE, NULL AS R, MIN(CASE WHEN STATUS = 'CHURN' THEN YEAR_MONTH END) AS YEAR_MONTH --, LAST_DATE
            FROM (
                SELECT *, CASE WHEN YEAR_MONTH > LAST_DATE THEN 'CHURN' END AS STATUS 
                FROM (
                    -- YEAR_MONTH PERIOD
                    SELECT 1 AS KEY, DATE_TRUNC(PARSE_DATE('%Y%m%d',CAST(SHOP_DATE AS STRING)), MONTH) AS YEAR_MONTH 
                    FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                    WHERE CUST_CODE IS NOT NULL
                    GROUP BY 1,2
                    ) A LEFT JOIN (
                        SELECT 1 AS KEY, *
                        FROM (
                            -- ALL_CUST
                            SELECT CUST_CODE, DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)), MONTH) AS SHOP_MONTH 
                            FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                            WHERE CUST_CODE IS NOT NULL
                            GROUP BY 1,2
                        ) A1 RIGHT JOIN (
                            --REGISTER_DATE AND LAST_DATE
                            SELECT  CUST_CODE AS CUST_CODE_RE, DATE_TRUNC(PARSE_DATE('%Y%m%d',CAST(MIN(SHOP_DATE) AS STRING)),MONTH) AS REGISTER_DATE,
                                    DATE_TRUNC(PARSE_DATE('%Y%m%d',CAST(MAX(SHOP_DATE) AS STRING)),MONTH) AS LAST_DATE 
                            FROM `data-rookery-331410.Supermarket_Data.Supermarket_Data_Table`
                            WHERE CUST_CODE IS NOT NULL
                            GROUP BY 1
                        ) A2 ON A1.CUST_CODE = A2.CUST_CODE_RE
                ) B ON A.KEY = B.KEY
            )
            GROUP BY 1,2,3 --,4
        ) 
        WHERE STATUS IS NOT NULL
    ) RESULT
    GROUP BY 1,2
) ALL_RESULT