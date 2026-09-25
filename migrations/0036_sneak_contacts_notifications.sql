-- Shared, tenant-scoped CRM and reliable owner notification queue.
CREATE TABLE sneak_contact_settings (
 site_id TEXT PRIMARY KEY REFERENCES sneak_sites(id) ON DELETE CASCADE,
 signup_notifications INTEGER NOT NULL DEFAULT 1,
 inquiry_notifications INTEGER NOT NULL DEFAULT 1,
 weekly_digest INTEGER NOT NULL DEFAULT 1,
 digest_day INTEGER NOT NULL DEFAULT 5,
 digest_hour INTEGER NOT NULL DEFAULT 17,
 timezone TEXT NOT NULL DEFAULT 'America/New_York',
 popup_mode TEXT NOT NULL DEFAULT 'optional',
 popup_after_views INTEGER NOT NULL DEFAULT 3,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO sneak_contact_settings(site_id) SELECT id FROM sneak_sites;
CREATE TRIGGER sneak_site_contact_settings AFTER INSERT ON sneak_sites BEGIN
 INSERT OR IGNORE INTO sneak_contact_settings(site_id) VALUES(NEW.id);
END;
CREATE TABLE sneak_contacts (
 id TEXT PRIMARY KEY,
 site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
 email TEXT NOT NULL,
 name TEXT NOT NULL DEFAULT '', phone TEXT NOT NULL DEFAULT '',
 consumer_id TEXT REFERENCES sneak_consumer_users(id) ON DELETE SET NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 last_activity_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(site_id,email)
);
CREATE INDEX idx_contacts_site_activity ON sneak_contacts(site_id,last_activity_at DESC);
CREATE TABLE sneak_contact_events (
 id TEXT PRIMARY KEY,
 contact_id TEXT NOT NULL REFERENCES sneak_contacts(id) ON DELETE CASCADE,
 site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
 event_type TEXT NOT NULL, lead_id TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_contact_events_site_created ON sneak_contact_events(site_id,created_at);
CREATE TABLE sneak_owner_notifications (
 id TEXT PRIMARY KEY,
 site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
 kind TEXT NOT NULL, reference_id TEXT,
 period_start TEXT, period_end TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 processed_at TEXT
);
CREATE TABLE sneak_owner_email_deliveries (
 id TEXT PRIMARY KEY,
 notification_id TEXT NOT NULL REFERENCES sneak_owner_notifications(id) ON DELETE CASCADE,
 recipient TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at TEXT, claimed_at TEXT, sent_at TEXT,
 provider_message_id TEXT, last_error TEXT,
 UNIQUE(notification_id,recipient)
);
CREATE INDEX idx_owner_email_pending ON sneak_owner_email_deliveries(status,next_attempt_at);
-- Existing contacts are visible without sending historical signup/inquiry notices.
INSERT OR IGNORE INTO sneak_contacts(id,site_id,email,consumer_id,created_at,last_activity_at)
 SELECT 'consumer_'||id,site_id,lower(trim(email)),id,created_at,COALESCE(last_activity_at,last_login_at,created_at)
 FROM sneak_consumer_users WHERE status='active';
INSERT OR IGNORE INTO sneak_contacts(id,site_id,email,name,phone,created_at,last_activity_at)
 SELECT 'lead_'||id,site_id,lower(trim(email)),name,COALESCE(phone,''),created_at,created_at FROM sneak_leads ORDER BY created_at DESC;
CREATE TRIGGER sneak_lead_to_contact AFTER INSERT ON sneak_leads BEGIN
 INSERT INTO sneak_contacts(id,site_id,email,name,phone)
 VALUES('contact_'||lower(hex(randomblob(16))),NEW.site_id,lower(trim(NEW.email)),NEW.name,COALESCE(NEW.phone,''))
 ON CONFLICT(site_id,email) DO UPDATE SET
 name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE name END,
 phone=CASE WHEN excluded.phone<>'' THEN excluded.phone ELSE phone END,last_activity_at=CURRENT_TIMESTAMP;
 INSERT INTO sneak_contact_events(id,contact_id,site_id,event_type,lead_id)
 SELECT 'inquiry_'||NEW.id,id,NEW.site_id,'inquiry',NEW.id FROM sneak_contacts WHERE site_id=NEW.site_id AND email=lower(trim(NEW.email));
 INSERT OR IGNORE INTO sneak_owner_notifications(id,site_id,kind,reference_id)
 VALUES('inquiry_'||NEW.id,NEW.site_id,'inquiry',NEW.id);
END;
CREATE TRIGGER sneak_login_to_contact AFTER UPDATE OF last_login_at ON sneak_consumer_users
 WHEN NEW.status='active' AND NEW.last_login_at IS NOT OLD.last_login_at BEGIN
 INSERT INTO sneak_contacts(id,site_id,email,consumer_id)
 VALUES('consumer_'||NEW.id,NEW.site_id,lower(trim(NEW.email)),NEW.id)
 ON CONFLICT(site_id,email) DO UPDATE SET consumer_id=NEW.id,last_activity_at=CURRENT_TIMESTAMP;
 INSERT OR IGNORE INTO sneak_contact_events(id,contact_id,site_id,event_type,created_at)
 SELECT 'login_'||NEW.id||'_'||NEW.last_login_at,id,NEW.site_id,'login',NEW.last_login_at FROM sneak_contacts WHERE site_id=NEW.site_id AND email=lower(trim(NEW.email));
 INSERT OR IGNORE INTO sneak_owner_notifications(id,site_id,kind,reference_id)
 SELECT 'signup_'||NEW.id,NEW.site_id,'signup',NEW.id WHERE OLD.activated_at IS NULL;
END;
CREATE TRIGGER sneak_activity_to_contact AFTER INSERT ON sneak_consumer_activity_events BEGIN
 UPDATE sneak_contacts SET last_activity_at=NEW.created_at WHERE site_id=NEW.site_id AND consumer_id=NEW.user_id;
END;

CREATE TABLE sneak_lead_rate_limits (id TEXT PRIMARY KEY, count INTEGER NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX idx_lead_rate_expiry ON sneak_lead_rate_limits(expires_at);
