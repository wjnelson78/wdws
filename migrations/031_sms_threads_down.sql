-- 031_sms_threads_down.sql
BEGIN;

DROP TABLE IF EXISTS core.sms_messages;
DROP TABLE IF EXISTS core.sms_threads;

COMMIT;
