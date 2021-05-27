CREATE UNLOGGED TABLE v_total_disturbed_areas{table_suffix} AS
SELECT
    {classifiers},
    l.land_class AS unfccc_land_class,
    CAST(l.year AS INTEGER) AS year,
    dt.disturbancetype AS disturbance_code,
    dt.disturbancetypename AS disturbance_type,
    CAST(ROUND(CAST(SUM(di.area) AS NUMERIC), 6) AS REAL) AS dist_area
FROM disturbancedimension{table_suffix} di
INNER JOIN r_location{table_suffix} l
    ON di.locationdimid = l.locationdimid
INNER JOIN disturbancetypedimension{table_suffix} dt
    ON di.disturbancetypedimid = dt.id
GROUP BY
    {classifiers},
    l.land_class,
    dt.disturbancetype,
    dt.disturbancetypename,
    l.year;