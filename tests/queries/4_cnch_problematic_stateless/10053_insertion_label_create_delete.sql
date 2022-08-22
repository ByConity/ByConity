DROP TABLE IF EXISTS label;
CREATE TABLE label (i Int, d Date) ENGINE = CnchMergeTree ORDER BY i;


INSERT INTO label FORMAT Values SETTINGS insertion_label = 'aaa' (0, 0);
INSERT INTO label FORMAT Values SETTINGS insertion_label = 'aaa' (0, 0);
SELECT database, table, name, status FROM system.insertion_labels WHERE database = 'test' AND table = 'label';


DELETE LABEL label 'aaa';
SELECT database, table, name, status FROM system.insertion_labels WHERE database = 'test' AND table = 'label';


INSERT INTO label FORMAT Values SETTINGS insertion_label = 'aaa' (0, 0);
INSERT INTO label FORMAT Values SETTINGS insertion_label = 'bbb' (0, 0);
INSERT INTO label FORMAT Values SETTINGS insertion_label = 'ccc' (0, 0);
SELECT database, table, name, status FROM system.insertion_labels WHERE database = 'test' AND table = 'label';


DROP TABLE label;
