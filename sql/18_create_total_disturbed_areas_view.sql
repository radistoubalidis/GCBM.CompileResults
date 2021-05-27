CREATE TABLE v_total_disturbed_areas AS
SELECT
    {classifiers},
    l.land_class AS unfccc_land_class,
    CAST(l.year AS INTEGER) AS year,
    dt.disturbancetype AS disturbance_code,
    dt.disturbancetypename AS disturbance_type,
    CAST(SUM(di.area) AS REAL) AS dist_area
FROM disturbancedimension di
INNER JOIN r_location l
    ON di.locationdimid = l.locationdimid
INNER JOIN disturbancetypedimension dt
    ON di.disturbancetypedimid = dt.id
GROUP BY
    {classifiers},
    l.land_class,
    dt.disturbancetype,
    dt.disturbancetypename,
    l.year;