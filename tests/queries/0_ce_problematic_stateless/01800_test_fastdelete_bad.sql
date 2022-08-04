DROP TABLE IF EXISTS fastdelete_bad;

CREATE TABLE fastdelete_bad (c1 Date, c2 Int64, c3 String, c4 Map(String, Int64), c5 Int64 Alias c2 + 1) ENGINE = MergeTree PARTITION BY c1 ORDER BY c2;

-- invalid predicate
ALTER TABLE fastdelete_bad FASTDELETE WHERE id = 0; -- { serverError 47 }
-- delete a column that doesn't exist in table or is not a physical column
ALTER TABLE fastdelete_bad FASTDELETE id WHERE c2 = 1; -- { serverError 16 }
ALTER TABLE fastdelete_bad FASTDELETE c1, id WHERE c2 = 1; -- { serverError 16 }
ALTER TABLE fastdelete_bad FASTDELETE c5 WHERE c2 = 1; -- { serverError 16 }
ALTER TABLE fastdelete_bad FASTDELETE _part_row_number WHERE c2 = 1; -- { serverError 16 }
-- execute multiple fastdelete mutations in one statement
ALTER TABLE fastdelete_bad FASTDELETE WHERE c2 = 1, FASTDELETE WHERE c2 = 2; -- { serverError 48 }

DROP TABLE fastdelete_bad;
