ALTER TABLE src."CompanyProfiles"
    DROP CONSTRAINT company_profiles_pk;

ALTER TABLE stage."CompanyProfiles"
    DROP CONSTRAINT company_profiles_pk;

ALTER TABLE src."CompanyProfiles"
    ADD CONSTRAINT companyprofile_pk
        PRIMARY KEY ("Symbol");

ALTER TABLE stage."CompanyProfiles"
    ADD CONSTRAINT companyprofile_pk
        PRIMARY KEY ("Symbol");
