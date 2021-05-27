CREATE TABLE v_pool_indicators AS
SELECT
    pid.id AS indicator_id,
    {classifiers},
    l.land_class AS unfccc_land_class,
    pid.name AS indicator,
    CAST(l.year AS INTEGER) AS year,
    l.age_range AS age_range,
    CAST(l.area AS REAL) AS area,
    CAST(SUM(p.poolvalue) AS REAL) AS pool_tc,
    CAST(SUM(p.poolvalue) / l.area AS REAL) AS pool_tc_per_ha
FROM r_pool_indicators pid
INNER JOIN r_pool_collection_pools pcp
    ON pid.pool_collection_id = pcp.pool_collection_id
INNER JOIN pools p
    ON pcp.pool_id = p.poolid
INNER JOIN r_location l
    ON p.locationdimid = l.locationdimid
WHERE l.year > 0
GROUP BY
    pid.id,
    pid.name,
    l.year,
    l.age_range,
    l.area,
    {classifiers},
    l.land_class;
