ALTER TABLE recon_cases
    DROP CONSTRAINT IF EXISTS recon_cases_resolution_check;

ALTER TABLE recon_cases
    ADD CONSTRAINT recon_cases_resolution_check
    CHECK (resolution IN ('PENDING','PROPOSED','APPROVED','REJECTED','AUTO_RESOLVED','ESCALATE'));
