-- MySQL 全数据类型测试表
-- 覆盖 MySQL 5.7/8.0 所有内置数据类型
-- 包含多行测试数据：正数边界值、负数边界值、零/null、特殊值

DROP TABLE IF EXISTS test_all_types;

CREATE TABLE test_all_types (
    id INT PRIMARY KEY AUTO_INCREMENT,

    -- ===================== 整数类型（有符号/无符号） =====================
    col_tinyint           TINYINT,              -- -128 ~ 127
    col_tinyint_unsigned  TINYINT UNSIGNED,     -- 0 ~ 255
    col_smallint          SMALLINT,             -- -32768 ~ 32767
    col_smallint_unsigned SMALLINT UNSIGNED,    -- 0 ~ 65535
    col_mediumint         MEDIUMINT,            -- -8388608 ~ 8388607
    col_mediumint_unsigned MEDIUMINT UNSIGNED,  -- 0 ~ 16777215
    col_int               INT,                  -- -2147483648 ~ 2147483647
    col_int_unsigned      INT UNSIGNED,         -- 0 ~ 4294967295
    col_bigint            BIGINT,               -- -9223372036854775808 ~ 9223372036854775807
    col_bigint_unsigned   BIGINT UNSIGNED,      -- 0 ~ 18446744073709551615
    col_bool              BOOL,                 -- TINYINT(1) 别名

    -- ===================== 浮点/定点类型 =====================
    col_float             FLOAT,                -- 单精度
    col_float_p           FLOAT(20,4),          -- 带精度
    col_double            DOUBLE,               -- 双精度
    col_double_precision  DOUBLE PRECISION,     -- DOUBLE 别名
    col_real              REAL,                 -- DOUBLE 别名
    col_decimal           DECIMAL(20,6),        -- 定点数
    col_decimal_p         DECIMAL(10,2),        -- 定点数小精度
    col_numeric           NUMERIC(15,4),        -- DECIMAL 别名
    col_dec_zero          DECIMAL(5,0),         -- 无小数位

    -- ===================== 位类型 =====================
    col_bit1              BIT(1),               -- 1 位
    col_bit8              BIT(8),               -- 8 位
    col_bit32             BIT(32),              -- 32 位
    col_bit64             BIT(64),              -- 64 位（最大）

    -- ===================== 日期时间类型 =====================
    col_date              DATE,                 -- 日期 '1000-01-01' ~ '9999-12-31'
    col_datetime          DATETIME,             -- 无小数秒
    col_datetime_3        DATETIME(3),          -- 毫秒精度
    col_datetime_6        DATETIME(6),          -- 微秒精度
    col_timestamp         TIMESTAMP,            -- 无小数秒
    col_timestamp_3       TIMESTAMP(3),         -- 毫秒精度
    col_timestamp_6       TIMESTAMP(6),         -- 微秒精度
    col_time              TIME,                 -- 无小数秒
    col_time_3            TIME(3),              -- 毫秒精度
    col_time_6            TIME(6),              -- 微秒精度
    col_year              YEAR,                 -- YEAR(4) 默认
    col_year_4            YEAR(4),              -- 4 位年份

    -- ===================== 字符串类型 =====================
    col_char_1            CHAR(1),              -- 最小 CHAR
    col_char_10           CHAR(10),             -- 普通 CHAR
    col_char_255          CHAR(255),            -- 最大 CHAR
    col_varchar_1         VARCHAR(1),           -- 最小 VARCHAR
    col_varchar_255       VARCHAR(255),         -- 普通 VARCHAR
    col_varchar_large     VARCHAR(5000),        -- 大 VARCHAR
    col_tinytext          TINYTEXT,             -- 0 ~ 255 字节
    col_text              TEXT,                 -- 0 ~ 65535 字节
    col_mediumtext        MEDIUMTEXT,           -- 0 ~ 16MB
    col_longtext          LONGTEXT,             -- 0 ~ 4GB

    -- ===================== 二进制类型 =====================
    col_binary_1          BINARY(1),            -- 最小 BINARY
    col_binary_10         BINARY(10),           -- 普通 BINARY
    col_varbinary_1       VARBINARY(1),         -- 最小 VARBINARY
    col_varbinary_255     VARBINARY(255),       -- 普通 VARBINARY
    col_tinyblob          TINYBLOB,             -- 0 ~ 255 字节
    col_blob              BLOB,                 -- 0 ~ 65535 字节
    col_mediumblob        MEDIUMBLOB,           -- 0 ~ 16MB
    col_longblob          LONGBLOB,             -- 0 ~ 4GB

    -- ===================== ENUM / SET =====================
    col_enum              ENUM('value1','value2','value3'),
    col_enum_long         ENUM('pending','processing','success','failed','timeout','cancelled'),
    col_set               SET('a','b','c','d'),
    col_set_full          SET('read','write','execute','admin'),

    -- ===================== JSON =====================
    col_json              JSON,
    col_json_array        JSON,
    col_json_nested       JSON,

    -- ===================== 空间类型 (MySQL 5.7+/8.0) =====================
    col_geometry          GEOMETRY,
    col_point             POINT,
    col_linestring        LINESTRING,
    col_polygon           POLYGON,
    col_multipoint        MULTIPOINT,
    col_multilinestring   MULTILINESTRING,
    col_multipolygon      MULTIPOLYGON,
    col_geometrycollection GEOMETRYCOLLECTION,

    -- ===================== 可空字段 =====================
    col_nullable          INT,
    col_nullable_str      VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===================== 行 1：正数边界值（最大值） =====================
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned, col_bool,
    col_float, col_float_p, col_double, col_double_precision, col_real,
    col_decimal, col_decimal_p, col_numeric, col_dec_zero,
    col_bit1, col_bit8, col_bit32, col_bit64,
    col_date, col_datetime, col_datetime_3, col_datetime_6,
    col_timestamp, col_timestamp_3, col_timestamp_6,
    col_time, col_time_3, col_time_6, col_year, col_year_4,
    col_char_1, col_char_10, col_char_255, col_varchar_1, col_varchar_255, col_varchar_large,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_binary_1, col_binary_10, col_varbinary_1, col_varbinary_255,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_enum_long, col_set, col_set_full,
    col_json, col_json_array, col_json_nested,
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    col_nullable, col_nullable_str
) VALUES (
    127, 255, 32767, 65535,
    8388607, 16777215, 2147483647, 4294967295,
    9223372036854775807, 18446744073709551615, TRUE,
    3.402823466E38, 99999.9999, 1.7976931348623157E308, 1.7976931348623157E308, 1.7976931348623157E308,
    9999999999.999999, 99999999.99, 99999.9999, 99999,
    1, 255, 4294967295, 18446744073709551615,
    '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999', '9999-12-31 23:59:59.999999',
    '2038-01-19 03:14:07', '2038-01-19 03:14:07.999', '2038-01-19 03:14:07.999999',
    '23:59:59', '23:59:58.999', '23:59:58.999999', 2155, 2155,
    'A', 'MAX_VALUE', REPEAT('X', 255), 'M', REPEAT('Y', 255), REPEAT('Z', 5000),
    'tiny text max', 'normal text max', 'medium text max', 'long text max',
    UNHEX('FF'), UNHEX('FFFFFFFFFFFFFFFFFFFF'), UNHEX('FF'), UNHEX(REPEAT('FF', 255)),
    UNHEX(REPEAT('AB', 100)), UNHEX(REPEAT('CD', 1000)), UNHEX(REPEAT('EF', 10000)), UNHEX(REPEAT('12', 100000)),
    'value1', 'success', 'a,b,c,d', 'read,write,execute,admin',
    '{"max": true, "value": 99999}', '[1,2,3,4,5]', '{"nested": {"deep": {"value": "max"}}}',
    ST_GeomFromText('POINT(1 1)'),
    ST_PointFromText('POINT(1 1)'),
    ST_LineFromText('LINESTRING(0 0, 1 1, 2 2)'),
    ST_PolyFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
    ST_MultiPointFromText('MULTIPOINT(0 0, 1 1, 2 2)'),
    ST_MLineFromText('MULTILINESTRING((0 0, 1 1), (2 2, 3 3))'),
    ST_MPolyFromText('MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))'),
    2147483647, 'max string value'
);

-- ===================== 行 2：负数边界值（最小值） =====================
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned, col_bool,
    col_float, col_float_p, col_double, col_double_precision, col_real,
    col_decimal, col_decimal_p, col_numeric, col_dec_zero,
    col_bit1, col_bit8, col_bit32, col_bit64,
    col_date, col_datetime, col_datetime_3, col_datetime_6,
    col_timestamp, col_timestamp_3, col_timestamp_6,
    col_time, col_time_3, col_time_6, col_year, col_year_4,
    col_char_1, col_char_10, col_char_255, col_varchar_1, col_varchar_255, col_varchar_large,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_binary_1, col_binary_10, col_varbinary_1, col_varbinary_255,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_enum_long, col_set, col_set_full,
    col_json, col_json_array, col_json_nested,
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    col_nullable, col_nullable_str
) VALUES (
    -128, 0, -32768, 0,
    -8388608, 0, -2147483648, 0,
    -9223372036854775808, 0, FALSE,
    -3.402823466E38, -99999.9999, -1.7976931348623157E308, -1.7976931348623157E308, -1.7976931348623157E308,
    -9999999999.999999, -99999999.99, -99999.9999, -99999,
    0, 0, 0, 0,
    '1000-01-01', '1000-01-01 00:00:00', '1000-01-01 00:00:00.000', '1000-01-01 00:00:00.000000',
    '1970-01-01 00:00:01', '1970-01-01 00:00:01.000', '1970-01-01 00:00:01.000000',
    '00:00:01', '00:00:01.000', '00:00:01.000000', 1901, 1901,
    'Z', 'MIN_VALUE', REPEAT('A', 255), 'N', REPEAT('B', 255), REPEAT('C', 5000),
    'tiny text min', 'normal text min', 'medium text min', 'long text min',
    UNHEX('00'), UNHEX('00000000000000000000'), UNHEX('00'), UNHEX(REPEAT('00', 255)),
    UNHEX(REPEAT('00', 100)), UNHEX(REPEAT('00', 1000)), UNHEX(REPEAT('00', 10000)), UNHEX(REPEAT('00', 100000)),
    'value3', 'failed', '', '',
    '{"min": true, "value": -99999}', '[-1,-2,-3,-4,-5]', '{"nested": {"deep": {"value": "min"}}}',
    ST_GeomFromText('POINT(-1 -1)'),
    ST_PointFromText('POINT(-1 -1)'),
    ST_LineFromText('LINESTRING(0 0, -1 -1, -2 -2)'),
    ST_PolyFromText('POLYGON((-1 -1, -1 0, 0 0, 0 -1, -1 -1))'),
    ST_MultiPointFromText('MULTIPOINT(-1 -1, -2 -2, -3 -3)'),
    ST_MLineFromText('MULTILINESTRING((0 0, -1 -1), (-2 -2, -3 -3))'),
    ST_MPolyFromText('MULTIPOLYGON(((-1 -1, -1 0, 0 0, 0 -1, -1 -1)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(-1 -1), LINESTRING(0 0, -1 -1))'),
    -2147483648, 'min string value'
);

-- ===================== 行 3：零值/空值/null =====================
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned, col_bool,
    col_float, col_float_p, col_double, col_double_precision, col_real,
    col_decimal, col_decimal_p, col_numeric, col_dec_zero,
    col_bit1, col_bit8, col_bit32, col_bit64,
    col_date, col_datetime, col_datetime_3, col_datetime_6,
    col_timestamp, col_timestamp_3, col_timestamp_6,
    col_time, col_time_3, col_time_6, col_year, col_year_4,
    col_char_1, col_char_10, col_char_255, col_varchar_1, col_varchar_255, col_varchar_large,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_binary_1, col_binary_10, col_varbinary_1, col_varbinary_255,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_enum_long, col_set, col_set_full,
    col_json, col_json_array, col_json_nested,
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    col_nullable, col_nullable_str
) VALUES (
    0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0, NULL,
    0.0, 0.0000, 0.0, 0.0, 0.0,
    0.000000, 0.00, 0.0000, 0,
    0, 0, 0, 0,
    '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00.000', '1970-01-01 00:00:00.000000',
    '1970-01-01 00:00:01', '1970-01-01 00:00:01.000', '1970-01-01 00:00:01.000000',
    '00:00:00', '00:00:00.000', '00:00:00.000000', 2000, 2000,
    '', '', '', '', '', '',
    '', '', '', '',
    UNHEX('00'), UNHEX('00000000000000000000'), UNHEX('00'), NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL
);

-- ===================== 行 4：特殊值（特殊字符、特殊日期、负时间） =====================
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned, col_bool,
    col_float, col_float_p, col_double, col_double_precision, col_real,
    col_decimal, col_decimal_p, col_numeric, col_dec_zero,
    col_bit1, col_bit8, col_bit32, col_bit64,
    col_date, col_datetime, col_datetime_3, col_datetime_6,
    col_timestamp, col_timestamp_3, col_timestamp_6,
    col_time, col_time_3, col_time_6, col_year, col_year_4,
    col_char_1, col_char_10, col_char_255, col_varchar_1, col_varchar_255, col_varchar_large,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_binary_1, col_binary_10, col_varbinary_1, col_varbinary_255,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_enum_long, col_set, col_set_full,
    col_json, col_json_array, col_json_nested,
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    col_nullable, col_nullable_str
) VALUES (
    1, 1, 1, 1,
    1, 1, 1, 1,
    1, 1, TRUE,
    3.14159, 3.1416, 2.718281828459045, 2.718281828459045, 2.718281828459045,
    3.141593, 3.14, 3.1416, 31416,
    1, 170, 4294967295, 18446744073709551615,
    '2026-06-20', '2026-06-20 12:30:45', '2026-06-20 12:30:45.678', '2026-06-20 12:30:45.678901',
    '2026-06-20 12:30:45', '2026-06-20 12:30:45.678', '2026-06-20 12:30:45.678901',
    '12:30:45', '12:30:45.500', '12:30:45.500000', 2026, 2026,
    '中', '中文测试', REPEAT('简', 255), 'S', 'Special: <>&"''\\ \n \t', 'Unicode: 中文 日本語 한국어 Emoji 😀🎉',
    'Line1\nLine2\tTab', 'Quote: "test" and ''single''', 'Mixed: <html>&amp;</html>', 'Long text with newline\nand tab\tand special chars <>&"''',
    UNHEX('41'), UNHEX('4142434445464748494A'), UNHEX('41'), UNHEX('414243'),
    UNHEX('414243'), UNHEX('4142434445'), UNHEX('414243444546'), UNHEX('41424344454647'),
    'value2', 'pending', 'a,c', 'read,execute',
    '{"unicode": "中文", "emoji": "😀", "number": 42, "null_val": null, "bool": true}',
    '[{"id": 1, "name": "test"}, {"id": 2, "name": "中文"}]',
    '{"a": [1, 2, {"b": "deep"}], "c": null, "d": true}',
    ST_GeomFromText('POINT(116.404 39.915)'),
    ST_PointFromText('POINT(121.474 31.230)'),
    ST_LineFromText('LINESTRING(0 0, 1 1, 2 0, 3 1)'),
    ST_PolyFromText('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))'),
    ST_MultiPointFromText('MULTIPOINT((0 0), (1 1), (2 2))'),
    ST_MLineFromText('MULTILINESTRING((0 0, 1 1, 2 2), (3 3, 4 4, 5 5))'),
    ST_MPolyFromText('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((3 3, 3 5, 5 5, 5 3, 3 3)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2), POLYGON((0 0, 0 1, 1 1, 1 0, 0 0)))'),
    0, 'special chars: <>&"''\\'
);

-- ===================== 行 5：混合中间值 =====================
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned, col_bool,
    col_float, col_float_p, col_double, col_double_precision, col_real,
    col_decimal, col_decimal_p, col_numeric, col_dec_zero,
    col_bit1, col_bit8, col_bit32, col_bit64,
    col_date, col_datetime, col_datetime_3, col_datetime_6,
    col_timestamp, col_timestamp_3, col_timestamp_6,
    col_time, col_time_3, col_time_6, col_year, col_year_4,
    col_char_1, col_char_10, col_char_255, col_varchar_1, col_varchar_255, col_varchar_large,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_binary_1, col_binary_10, col_varbinary_1, col_varbinary_255,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_enum_long, col_set, col_set_full,
    col_json, col_json_array, col_json_nested,
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    col_nullable, col_nullable_str
) VALUES (
    64, 128, 16384, 32768,
    4194304, 8388608, 1073741824, 2147483648,
    4611686018427387904, 9223372036854775808, TRUE,
    1.5, 12345.6789, 3.141592653589793, 3.141592653589793, 3.141592653589793,
    1234567.890123, 12345.67, 1234.5678, 12345,
    0, 85, 16777215, 9223372036854775808,
    '2026-01-15', '2026-01-15 06:15:30', '2026-01-15 06:15:30.500', '2026-01-15 06:15:30.500000',
    '2026-01-15 06:15:30', '2026-01-15 06:15:30.500', '2026-01-15 06:15:30.500000',
    '06:15:30', '06:15:30.500', '06:15:30.500000', 1999, 1999,
    'M', 'MID_VALUE', REPEAT('M', 255), 'D', 'Middle value test', 'A medium length string for testing purposes',
    'mid tiny text', 'mid normal text', 'mid medium text', 'mid long text',
    UNHEX('80'), UNHEX('80808080808080808080'), UNHEX('80'), UNHEX(REPEAT('80', 128)),
    UNHEX(REPEAT('55', 50)), UNHEX(REPEAT('55', 500)), UNHEX(REPEAT('55', 5000)), UNHEX(REPEAT('55', 50000)),
    'value2', 'processing', 'b,d', 'write,admin',
    '{"mid": 50, "float_val": 50.5}', '[10, 20, 30]', '{"level": 2, "data": [1, 2, 3]}',
    ST_GeomFromText('POINT(50 50)'),
    ST_PointFromText('POINT(50.5 50.5)'),
    ST_LineFromText('LINESTRING(0 0, 50 50, 100 100)'),
    ST_PolyFromText('POLYGON((0 0, 0 50, 50 50, 50 0, 0 0))'),
    ST_MultiPointFromText('MULTIPOINT(10 10, 20 20, 30 30)'),
    ST_MLineFromText('MULTILINESTRING((0 0, 50 50), (50 50, 100 100))'),
    ST_MPolyFromText('MULTIPOLYGON(((0 0, 0 50, 50 50, 50 0, 0 0)), ((50 50, 50 100, 100 100, 100 50, 50 50)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(50 50), LINESTRING(0 0, 100 100))'),
    1073741824, 'mid string value'
);

-- ===================== 验证 =====================
SELECT COUNT(*) AS total_rows FROM test_all_types;
SELECT
    COUNT(col_tinyint) AS tinyint_not_null,
    COUNT(col_json) AS json_not_null,
    COUNT(col_point) AS point_not_null,
    COUNT(col_nullable) AS nullable_not_null
FROM test_all_types;
