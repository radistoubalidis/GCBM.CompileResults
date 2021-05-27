CREATE TABLE v_disturbance_indicators AS
SELECT
    {classifiers},
    l.land_class AS unfccc_land_class,
    CAST(l.year AS INTEGER) AS year,
    dt.disturbancetype AS disturbance_code,
    dt.disturbancetypename AS disturbance_type,
    CAST(
        CASE
            WHEN ac.startage = -1 THEN 'N/A'
            WHEN ac.endage = -1 THEN ac.startage || '+'
            ELSE ac.startage || '-' || ac.endage
        END
        AS TEXT
    ) AS pre_dist_age_range,
    l.age_range AS post_dist_age_range,
    CAST(SUM(di.area) AS REAL) AS dist_area,
    CAST(SUM(f.fluxvalue) AS REAL) AS dist_product,
    CAST(SUM(f.fluxvalue) / SUM(di.area) AS REAL) AS dist_product_per_ha
FROM (
    SELECT
        locationdimid,
        moduleinfodimid,
        disturbancedimid,
        SUM(f.fluxvalue) AS fluxvalue
    FROM fluxes f
    GROUP BY
        locationdimid,
        moduleinfodimid,
        disturbancedimid
) AS f
INNER JOIN r_location l
    ON f.locationdimid = l.locationdimid
INNER JOIN disturbancedimension di
    ON f.disturbancedimid = di.id
INNER JOIN disturbancetypedimension dt
    ON di.disturbancetypedimid = dt.id
LEFT JOIN ageclassdimension ac
    ON di.predistageclassdimid = ac.id
WHERE l.year > 0
GROUP BY
    {classifiers},
    l.land_class,
    dt.disturbancetype,
    dt.disturbancetypename,
    ac.startage,
    ac.endage,
    l.age_range,
    l.year;