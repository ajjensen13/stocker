ALTER TABLE src."CompanyProfiles"
    DROP CONSTRAINT companyprofile_pk;

ALTER TABLE stage."CompanyProfiles"
    DROP CONSTRAINT companyprofile_pk;

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

ALTER TABLE src."CompanyProfiles"
    ADD CONSTRAINT company_profiles_pk
        PRIMARY KEY ("Symbol");

ALTER TABLE stage."CompanyProfiles"
    ADD CONSTRAINT company_profiles_pk
        PRIMARY KEY ("Symbol");
