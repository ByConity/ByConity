SELECT count() FROM system.parts WHERE table = NULL AND database = currentDatabase(0);
SELECT DISTINCT marks FROM system.parts WHERE (table = NULL) AND (database = currentDatabase(0)) AND (active = 1);
