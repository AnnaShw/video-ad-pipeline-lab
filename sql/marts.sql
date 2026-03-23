SELECT 
    campaign_id,
    SUM(CASE WHEN event = 'impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN event = 'click' THEN 1 ELSE 0 END) AS clicks,
    CAST(SUM(CASE WHEN event = 'click' THEN 1 ELSE 0 END) AS FLOAT) / 
        NULLIF(SUM(CASE WHEN event = 'impression' THEN 1 ELSE 0 END), 0) AS ctr
FROM events
GROUP BY campaign_id
ORDER BY impressions DESC;
