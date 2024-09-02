-- not setting dialect type mysql due to map type
use test;
set text_case_option='LOWERCASE';
CREATE TABLE test.TeSt_1
(
    `boOl` BOOLEan,
    `tinY` TinYinT,
    `sMaLL` smallinT,
    `iNt` iNteGer,
    `BigINT` bIginT,
    `FlOAT` floAt,
    `douBlE` DOUbLe,
    `dEciMaL` decImal(30, 3),
    `vracHAr` varCHaR(100),
    `binARy` bINARY(20),
    `DaTE` Date,
    `tIMe` tiME,
    `datEtIme` DaTeTImE,
    `tIMEsTamP` tiMestamP,
    `Array` ARRAy(varCHaR),
    `MAp` maP(varCHaR, varCHaR)
)
ENGINE = CnchMergeTree
ORDER BY INt;

DROP TABLE TeSt_1;
