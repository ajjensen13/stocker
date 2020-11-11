DELETE FROM src."CompanyProfiles" d
WHERE NOT EXISTS (
        SELECT
            a."Symbol",
            a."Exchange"
        FROM
            src."CompanyProfiles" a
                INNER JOIN (
                SELECT DISTINCT b."Symbol", MAX(b."Created") "Created"
                FROM  src."CompanyProfiles" b
                GROUP BY b."Symbol"
            ) c ON a."Symbol" = c."Symbol" AND a."Created" = c."Created"
        WHERE d."Symbol" = a."Symbol" AND d."Exchange" = a."Exchange"
    );

DELETE FROM stage."CompanyProfiles" d
WHERE NOT EXISTS (
    SELECT
        a."Symbol",
        a."Exchange"
    FROM
        stage."CompanyProfiles" a
            INNER JOIN (
            SELECT DISTINCT b."Symbol", MAX(b."Created") "Created"
            FROM  stage."CompanyProfiles" b
            GROUP BY b."Symbol"
        ) c ON a."Symbol" = c."Symbol" AND a."Created" = c."Created"
    WHERE d."Symbol" = a."Symbol" AND d."Exchange" = a."Exchange"
);
