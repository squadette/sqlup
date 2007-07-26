alter table databaselog add blah int
-- SQLUP-CUT
-- rollback script

alter table databaselog drop column blah 
