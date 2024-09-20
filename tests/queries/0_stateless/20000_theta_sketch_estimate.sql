DROP TABLE IF EXISTS test.test_theta_sketch;

CREATE TABLE test.test_theta_sketch
(
    `test_col` Int8,
    `test_col2` SketchBinary
) ENGINE = MergeTree() ORDER BY test_col;

INSERT INTO test.test_theta_sketch values (1, 'AgMDAAAazJMFAAAAAACAPxX5fcu9hqEFw5f8EoFwnR4oBDhkJpYOMJ+FWhAWLpBs2C0jd0u5NX4=')

SELECT thetaSketchEstimate(test_col2) FROM test.test_theta_sketch;

INSERT INTO test.test_theta_sketch values (2, 'AgMDAAAazJMEAAAAAACAP+egZA96NAcHbakWvEpmYR6sUwzUAj6rLtyf/LHURyhv')

SELECT thetaSketchEstimate(test_col2) FROM test.test_theta_sketch;

DROP TABLE IF EXISTS test.test_theta_sketch;