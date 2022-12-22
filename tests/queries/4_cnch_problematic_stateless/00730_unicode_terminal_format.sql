
DROP TABLE IF EXISTS unicode;
CREATE TABLE unicode(c1 Int,c2 String, c3 String) ENGINE = CnchMergeTree ORDER BY c1;
INSERT INTO unicode VALUES (1,'Здравствуйте', 'Этот код можно отредактировать и запустить!');
INSERT INTO unicode VALUES (2,'你好', '这段代码是可以编辑并且能够运行的！');
INSERT INTO unicode VALUES (3,'Hola', '¡Este código es editable y ejecutable!');
INSERT INTO unicode VALUES (4,'Bonjour', 'Ce code est modifiable et exécutable !');
INSERT INTO unicode VALUES (5,'Ciao', 'Questo codice è modificabile ed eseguibile!');
INSERT INTO unicode VALUES (6,'こんにちは', 'このコードは編集して実行出来ます！');
INSERT INTO unicode VALUES (7,'안녕하세요', '여기에서 코드를 수정하고 실행할 수 있습니다!');
INSERT INTO unicode VALUES (8,'Cześć', 'Ten kod można edytować oraz uruchomić!');
INSERT INTO unicode VALUES (9,'Olá', 'Este código é editável e executável!');
INSERT INTO unicode VALUES (10,'Chào bạn', 'Bạn có thể edit và run code trực tiếp!');
INSERT INTO unicode VALUES (11,'Hallo', 'Dieser Code kann bearbeitet und ausgeführt werden!');
INSERT INTO unicode VALUES (12,'Hej', 'Den här koden kan redigeras och köras!');
INSERT INTO unicode VALUES (13,'Ahoj', 'Tento kód můžete upravit a spustit');
INSERT INTO unicode VALUES (14,'Tabs \t Tabs', 'Non-first \t Tabs');
INSERT INTO unicode VALUES (15,'Control characters \x1f\x1f\x1f\x1f with zero width', 'Invalid UTF-8 which eats pending characters \xf0, or invalid by itself \x80 with zero width');
INSERT INTO unicode VALUES (16,'Russian ё and ё ', 'Zero bytes \0 \0 in middle');
SELECT * FROM unicode order by c1 SETTINGS max_threads = 1 FORMAT PrettyNoEscapes;
SELECT 'Tabs \t Tabs', 'Long\tTitle' FORMAT PrettyNoEscapes;

SELECT '你好', '世界' FORMAT Vertical;
SELECT 'Tabs \t Tabs', 'Non-first \t Tabs' FORMAT Vertical;
SELECT 'Control characters \x1f\x1f\x1f\x1f with zero width', 'Invalid UTF-8 which eats pending characters \xf0, and invalid by itself \x80 with zero width' FORMAT Vertical;
SELECT 'Russian ё and ё', 'Zero bytes \0 \0 in middle' FORMAT Vertical;

DROP TABLE IF EXISTS unicode;
