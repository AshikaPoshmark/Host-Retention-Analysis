-- Problem Statement : The Host coming back on the next month M2 and M3 after getting activated has declined from 42+% to 38%.  So, We need to analysis the factors that are influencing in the decline of host retention.



------------------ Hypothesis 1 : Retention has dropped because hosts have not returned as AU on Poshmark.------------------


-- CTE to calculate active hosts by month
WITH active_host_table AS (SELECT (TO_CHAR(DATE_TRUNC('month', show_host_activated_at ), 'YYYY-MM')) AS Show_Host_Activated_Month,
       CASE WHEN DATE(show_host_activated_at) <= event_date THEN (TO_CHAR(DATE_TRUNC('month', event_date ), 'YYYY-MM')) END AS Active_month,
       DATEDIFF(month,show_host_activated_at,event_date) + 1 AS months_since_show_host_activated,
       count(distinct Active_user ) As Active_Host
 FROM analytics.dw_users_cs
 LEFT JOIN analytics.dw_users ON dw_users_cs.user_id = dw_users.user_id
 LEFT JOIN analytics.dw_shows ON dw_users_cs.user_id = dw_shows.creator_id
 LEFT JOIN (SELECT event_date,
        -- Count distinct active and valid users
        CASE WHEN is_active IS true AND is_valid_user IS true THEN user_id ELSE NULL END AS Active_user
 FROM analytics.dw_user_events_daily
    WHERE (event_date < '2024-06-01' AND event_date >= '2022-10-01') AND domain ='us'
    GROUP BY 1,2)AS AU_table ON dw_users_cs.user_id = AU_table.Active_user
 WHERE  show_host_activated_at is not null
 AND dw_shows.origin_domain ='us'
 AND show_host_activated_at >= '2022-10-01'
 AND Active_month is NOT NULL
 GROUP BY 1,2,3
 ORDER BY 1 desc, 2,3),

-- CTE to calculate host retention by month
Host_retention_table AS (SELECT (TO_CHAR(DATE_TRUNC('month', show_host_activated_at ), 'YYYY-MM')) AS Show_Host_Activated_Month,
       (TO_CHAR(DATE_TRUNC('month', start_at ), 'YYYY-MM')) AS Show_Month,
       DATEDIFF(month,show_host_activated_at,dw_shows.start_at) + 1 AS months_since_show_host_activated,
       count( distinct dw_users.user_id) AS hosts
 FROM analytics.dw_users_cs
 LEFT JOIN analytics.dw_users ON dw_users_cs.user_id = dw_users.user_id
 LEFT JOIN analytics.dw_shows ON dw_users_cs.user_id = dw_shows.creator_id
 WHERE  show_host_activated_at is not null
  AND dw_shows.origin_domain ='us'
  AND (start_at < '2024-06-01' AND start_at >= '2022-10-01')
  AND (show_host_activated_at < '2024-06-01' AND show_host_activated_at >= '2022-10-01')
  AND Show_Month is not null
 GROUP BY 1,2,3
 ORDER BY 1 desc,2,3)

-- Final query to join active hosts and host retention tables
SELECT active_host_table.Show_Host_Activated_Month, Show_Month as active_since_host_activated_month,
       Host_retention_table.months_since_show_host_activated,Active_Host, hosts
 FROM active_host_table
 LEFT JOIN Host_retention_table ON Host_retention_table.Show_Host_Activated_Month = active_host_table.Show_Host_Activated_Month
 AND Host_retention_table.Show_Month = active_host_table.Active_month
 AND Host_retention_table.months_since_show_host_activated = active_host_table.months_since_show_host_activated
 ORDER BY 1 DESC , 2;



-- Hypothesis 2: Hosts who belong to lower seller segments have increased and they tend to have poorer retention.


----------------------------- Host Retention by Seller Segment - Monthly -----------------------------------


SELECT (TO_CHAR(DATE_TRUNC('month', show_host_activated_at ), 'YYYY-MM')) AS Show_Host_Activated_Month,
       (TO_CHAR(DATE_TRUNC('month', start_at ), 'YYYY-MM')) AS Show_Month,
       DATEDIFF(month,show_host_activated_at,dw_shows.start_at) + 1 AS months_since_show_host_activated,
       coalesce(seller_segments_gmv_start.seller_segment_daily, '1. No Sales')  AS seller_segment,
       COUNT( DISTINCT dw_users.user_id)
FROM analytics.dw_users_cs
LEFT JOIN analytics.dw_users ON dw_users_cs.user_id = dw_users.user_id
LEFT JOIN analytics.dw_shows ON dw_users_cs.user_id = dw_shows.creator_id
LEFT JOIN analytics_scratch.l365d_seller_segment_gmv AS seller_segments_gmv_start
                             ON dw_shows.creator_id = seller_segments_gmv_start.seller_id AND
                                (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) >
                                (TO_CHAR(DATE_TRUNC('month', seller_segments_gmv_start.start_date ), 'YYYY-MM'))
                                AND  (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) <=
                               (TO_CHAR(DATE_TRUNC('month', coalesce(seller_segments_gmv_start.end_date, GETDATE()) ), 'YYYY-MM'))
WHERE  show_host_activated_at IS NOT NULL
  AND dw_shows.origin_domain ='us'
  AND (start_at < '2024-06-01' AND start_at >= '2022-10-01')
  AND (show_host_activated_at < '2024-06-01' AND show_host_activated_at >= '2022-10-01')
AND Show_Month IS NOT NULL
GROUP BY 1,2,3,4
ORDER BY 1 DESC,2,3,4;

----------------------------- Host Retention by Show Type across Seller Segments - Monthly -----------------------------------

SELECT (TO_CHAR(DATE_TRUNC('month', show_host_activated_at ), 'YYYY-MM')) AS Show_Host_Activated_Month,
       (TO_CHAR(DATE_TRUNC('month', start_at ), 'YYYY-MM')) AS Show_Month,
       DATEDIFF(month,show_host_activated_at,dw_shows.start_at) + 1 AS months_since_show_host_activated,
       CASE
           WHEN show_host_activated_at = live_show_host_activated_at THEN 'live_show_activated_host'
           WHEN show_host_activated_at = silent_show_host_activated_at THEN 'silent_show_activated_host'
       END AS show_type,
       coalesce(seller_segments_gmv_start.seller_segment_daily, '1. No Sales')  AS seller_segment,
       count( distinct dw_users.user_id) as Host_count
FROM analytics.dw_users_cs
LEFT JOIN analytics.dw_users ON dw_users_cs.user_id = dw_users.user_id
LEFT JOIN analytics.dw_shows ON dw_users_cs.user_id = dw_shows.creator_id
LEFT JOIN analytics_scratch.l365d_seller_segment_gmv AS seller_segments_gmv_start
                             ON dw_shows.creator_id = seller_segments_gmv_start.seller_id and
                                (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) >
                                (TO_CHAR(DATE_TRUNC('month', seller_segments_gmv_start.start_date ), 'YYYY-MM'))
                                and  (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) <=
                               (TO_CHAR(DATE_TRUNC('month', coalesce(seller_segments_gmv_start.end_date, GETDATE()) ), 'YYYY-MM'))
WHERE  show_host_activated_at IS NOT NULL
  AND dw_shows.origin_domain ='us'
  AND (start_at < '2024-06-01' AND start_at >= '2022-10-01')
  AND (show_host_activated_at < '2024-06-01' AND show_host_activated_at >= '2022-10-01')
AND Show_Month IS NOT NULL
GROUP BY 1,2,3,4,5
ORDER BY 1 desc,2,3,4,5;
