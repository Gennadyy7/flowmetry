CREATE OR REPLACE FUNCTION register_application_from_metric()
RETURNS TRIGGER AS $$
DECLARE
    app_name TEXT;
    app_id BIGINT;
BEGIN
    app_name := NEW.attributes->>'service.name';

    IF app_name IS NULL OR trim(app_name) = '' THEN
        app_name := NEW.attributes->>'application.name';
    END IF;
    IF app_name IS NULL OR trim(app_name) = '' THEN
        app_name := 'unknown';
    END IF;

    INSERT INTO application (name) VALUES (app_name)
    ON CONFLICT (name) DO NOTHING;

    SELECT id INTO app_id FROM application WHERE name = app_name;

    NEW.application_id := app_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_register_application ON metrics_info;
CREATE TRIGGER trg_register_application
BEFORE INSERT ON metrics_info
FOR EACH ROW
EXECUTE FUNCTION register_application_from_metric();