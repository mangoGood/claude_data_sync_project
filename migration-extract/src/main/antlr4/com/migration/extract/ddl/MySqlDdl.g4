/*
 * MySQL DDL 语法——只为"把 DDL 施加到表结构模型上"服务，不是通用 MySQL 语法。
 *
 * 设计原则：结构精确、表达式不透明。
 *   - 精确解析：列名、类型分量（类型名/长度/标度/unsigned/charset/enum 取值表）、
 *     列属性、主键与唯一索引的列清单与顺序；
 *   - 不透明消费：DEFAULT (...) / GENERATED AS (...) / CHECK (...) 的表达式体、
 *     分区子句、表选项——一律按配平括号或 token 串整段吃掉，只留原文。
 *
 * 我们从不对这些表达式求值，只是存下来重建 DDL 或给人看。MySQL DDL 语法里最难的部分
 * 正是任意表达式，裁掉之后规模从 ~2500 行落到几百行——这是自己写语法而不是 vendor
 * Debezium 全量语法的前提。
 *
 * 另一条原则是"松语法、严 listener"：语法只负责切出 token 结构，类型名归一
 * （INTEGER→int、BOOL→tinyint(1)、DEC/NUMERIC/FIXED→decimal）等语义判断全部交给 listener。
 * 我们不是在校验 SQL 合法性，是在提取结构，宽进严出更耐受真实世界的 DDL 形态。
 *
 * 阶段 1 只做 CREATE TABLE；ALTER TABLE / RENAME TABLE 在阶段 2 补，
 * 在那之前它们落到 otherStatement，由调用方按"解析不了"处理。
 */
grammar MySqlDdl;

options { caseInsensitive = true; }

// ============================ 语句入口 ============================

ddlStatement
    : createTableStatement SEMI? EOF
    | alterTableStatement SEMI? EOF
    | renameTableStatement SEMI? EOF
    | dropTableStatement SEMI? EOF
    | createIndexStatement SEMI? EOF
    | dropIndexStatement SEMI? EOF
    | otherStatement EOF
    ;

createTableStatement
    // CREATE TABLE t LIKE src —— 结构复制，listener 去查源表版本
    : CREATE TEMPORARY? TABLE ifNotExists? tableName
        ( LIKE tableName
        | LPAREN LIKE tableName RPAREN
        ) trailing?                                                  # createTableLike
    // 常规建表。createDefinitions 之后剩下的（表选项 / 分区 / AS SELECT）整段留给 listener
    | CREATE TEMPORARY? TABLE ifNotExists? tableName
        createDefinitions trailing?                                  # createTablePlain
    // CREATE TABLE t AS SELECT / CREATE TABLE t SELECT —— 没有列定义，结构推不出来。
    // 这里必须锚定 SELECT 关键字：写成"表名之后随便什么"的话，任何<b>语法坏掉</b>的建表语句
    // （少个右括号之类）都会落到这条上，被当成"结构推不出来"降级处理，而不是报解析失败——
    // 两者都会降级，但前者会把语法缺口伪装成"这张表本来就推不出结构"，永远补不上。
    | CREATE TEMPORARY? TABLE ifNotExists? tableName AS? SELECT trailing?   # createTableAsSelect
    ;

/** 语句尾部：表选项、分区子句、AS SELECT。整段按 token 串吃掉，listener 用正则取表级 charset。 */
trailing
    : ~(SEMI | EOF)+
    ;

// ============================ ALTER / RENAME / DROP ============================

/**
 * 多子句 ALTER 必须<b>按书写顺序逐条</b>作用在中间态上，不能并行归并——
 * {@code ALTER TABLE t ADD a INT, DROP b, MODIFY c BIGINT} 三条之间是有依赖的
 * （比如先 CHANGE 改名再 MODIFY 新名字），归并之后顺序一乱，模型就与源库不同了。
 */
alterTableStatement
    : ALTER (ONLINE | OFFLINE)? IGNORE? TABLE tableName
        alterSpecification (COMMA alterSpecification)*
    ;

alterSpecification
    : ADD COLUMN? identifier dataType columnAttribute* columnPosition?              # altAddColumn
    | ADD COLUMN? LPAREN columnDefinition (COMMA columnDefinition)* RPAREN          # altAddColumnList
    | DROP COLUMN? identifier restrictOrCascade?                                    # altDropColumn
    | MODIFY COLUMN? identifier dataType columnAttribute* columnPosition?           # altModifyColumn
    | CHANGE COLUMN? identifier identifier dataType columnAttribute* columnPosition? # altChangeColumn
    | RENAME COLUMN identifier TO identifier                                        # altRenameColumn
    | ALTER COLUMN? identifier
        ( SET DEFAULT defaultValue
        | DROP DEFAULT
        | SET (VISIBLE | INVISIBLE) )                                               # altAlterColumn
    | ADD constraintName? PRIMARY KEY indexType? keyPartList indexOption*           # altAddPrimaryKey
    | DROP PRIMARY KEY                                                              # altDropPrimaryKey
    | ADD constraintName? UNIQUE (INDEX | KEY)? identifier? indexType? keyPartList indexOption*  # altAddUnique
    | ADD (INDEX | KEY) identifier? indexType? keyPartList indexOption*             # altAddIndex
    | ADD (FULLTEXT | SPATIAL) (INDEX | KEY)? identifier? keyPartList indexOption*  # altAddFulltext
    | ADD constraintName? FOREIGN KEY identifier? keyPartList referenceDefinition   # altAddForeign
    | ADD constraintName? CHECK parenBlock (NOT? ENFORCED)?                         # altAddCheck
    | DROP (INDEX | KEY) identifier                                                 # altDropIndex
    | DROP FOREIGN KEY identifier                                                   # altDropForeign
    | DROP (CONSTRAINT | CHECK) identifier                                          # altDropConstraint
    | RENAME (INDEX | KEY) identifier TO identifier                                 # altRenameIndex
    | RENAME (TO | AS)? tableName                                                   # altRenameTable
    | CONVERT TO CHARACTER SET charsetName (COLLATE charsetName)?                   # altConvertCharset
    | ALTER INDEX identifier (VISIBLE | INVISIBLE)                                  # altAlterIndex
    // 分区操作不改列布局，但它们以 ADD/DROP 开头，必须显式列出来才能与"改列"的形态区分开
    | ADD PARTITION parenBlock?                                                     # altPartition
    | DROP PARTITION identifier                                                     # altPartition2
    | alterOther                                                                    # altOther
    ;

columnPosition
    : FIRST                                                                         # posFirst
    | AFTER identifier                                                              # posAfter
    ;

restrictOrCascade : RESTRICT | CASCADE ;

/**
 * 不影响列布局的子句（{@code ALGORITHM=INPLACE}、{@code LOCK=NONE}、{@code DISABLE KEYS}、
 * 表选项、其余分区操作……）。只要解析得通就行，不入模型。边界是逗号——多子句 ALTER 靠它切开。
 *
 * <p><b>首 token 不能是 ADD/DROP/MODIFY/CHANGE/RENAME/ALTER/CONVERT</b>。写成"逗号之前随便什么"
 * 的话，任何<b>语法覆盖之外的改列子句</b>都会落到这里被当成"不影响结构"静默跳过——
 * 而漏施加一条改列的 ALTER，该表之后的每一个版本都是错的，行事件按错的列布局解析，
 * 写进目标库的是合法值、看不出异常。这与 extract 的事件分发只放行白名单是同一个道理：
 * 黑名单漏一个就是一次静默损坏，所以这里宁可报解析失败（E3023，会降级并留痕），
 * 也不能默认放过。
 */
alterOther
    : ~(COMMA | SEMI | ADD | DROP | MODIFY | CHANGE | RENAME | ALTER | CONVERT) ~(COMMA | SEMI)*
    ;

renameTableStatement
    : RENAME TABLE tableName TO tableName (COMMA tableName TO tableName)*
    ;

dropTableStatement
    : DROP TEMPORARY? TABLE ifExists? tableName (COMMA tableName)* restrictOrCascade?
    ;

createIndexStatement
    : CREATE (UNIQUE | FULLTEXT | SPATIAL)? INDEX identifier indexType?
        ON tableName keyPartList indexOption* trailing?
    ;

dropIndexStatement
    : DROP INDEX identifier ON tableName trailing?
    ;

otherStatement
    : ~EOF*
    ;

ifNotExists : IF NOT EXISTS ;
ifExists    : IF EXISTS ;

tableName   : (identifier DOT)? identifier ;

// ============================ 建表定义体 ============================

createDefinitions
    : LPAREN createDefinition (COMMA createDefinition)* RPAREN
    ;

createDefinition
    : indexDefinition                                                # defIndex
    | columnDefinition                                               # defColumn
    ;

columnDefinition
    : identifier dataType columnAttribute*
    ;

// ---- 类型 ----

/**
 * 松形式：类型名 + 可选长度/取值表 + 任意顺序的后缀。
 * 类型名是否合法、长度是精度还是显示宽度，全由 listener 判断。
 */
dataType
    : NATIONAL? typeToken typeToken? lengthSpec? typeSuffix*
    ;

/**
 * 第二个可选 typeToken 用于两词类型：DOUBLE PRECISION / CHARACTER VARYING /
 * LONG VARBINARY 等。listener 按组合归一。
 */
typeToken
    : TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT | MIDDLEINT | INT1 | INT2 | INT3 | INT4 | INT8
    | REAL | DOUBLE | FLOAT | FLOAT4 | FLOAT8 | PRECISION
    | DECIMAL | DEC | NUMERIC | FIXED
    | BIT | BOOL | BOOLEAN | SERIAL
    | DATE | TIME | TIMESTAMP | DATETIME | YEAR
    | CHAR | VARCHAR | VARYING | NCHAR | NVARCHAR
    | BINARY | VARBINARY
    | TINYBLOB | BLOB | MEDIUMBLOB | LONGBLOB | LONG
    | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT
    | ENUM | SET
    | JSON
    | GEOMETRY | POINT | LINESTRING | POLYGON
    | MULTIPOINT | MULTILINESTRING | MULTIPOLYGON | GEOMETRYCOLLECTION
    ;

lengthSpec
    : LPAREN signedNumber (COMMA signedNumber)? RPAREN               # numericLength
    | LPAREN STRING_LITERAL (COMMA STRING_LITERAL)* RPAREN           # valueListLength
    ;

typeSuffix
    : UNSIGNED
    | SIGNED
    | ZEROFILL
    | BINARY
    | charsetSpec
    | collateSpec
    ;

charsetSpec : (CHARACTER SET | CHARSET) charsetName ;
collateSpec : COLLATE charsetName ;
/** 字符集/排序规则名可以是标识符、也可以是带引号的串（DEFAULT CHARSET='utf8mb4'） */
charsetName : identifier | STRING_LITERAL | BINARY | DEFAULT ;

// ---- 列属性 ----

columnAttribute
    : NOT NULL_LITERAL                                               # attrNotNull
    | NULL_LITERAL                                                   # attrNull
    | DEFAULT defaultValue                                           # attrDefault
    | ON UPDATE defaultValue                                         # attrOnUpdate
    | AUTO_INCREMENT                                                 # attrAutoIncrement
    | (GENERATED ALWAYS)? AS parenBlock (STORED | VIRTUAL)?          # attrGenerated
    | PRIMARY? KEY                                                   # attrPrimaryKey
    | UNIQUE KEY?                                                    # attrUnique
    | COMMENT STRING_LITERAL                                         # attrComment
    | COLLATE charsetName                                            # attrCollate
    | charsetSpec                                                    # attrCharset
    | CHECK parenBlock (NOT? ENFORCED)?                              # attrCheck
    | (VISIBLE | INVISIBLE)                                          # attrVisibility
    | COLUMN_FORMAT identifier                                       # attrColumnFormat
    | STORAGE identifier                                             # attrStorage
    | SRID signedNumber                                              # attrSrid
    | referenceDefinition                                            # attrReference
    ;

/**
 * DEFAULT / ON UPDATE 的值。只保留原文，不求值——所以这里可以很松。
 * 括号形式（8.0 的 DEFAULT (expr)）走 parenBlock，函数调用走 identifier parenBlock。
 */
defaultValue
    : parenBlock
    | literal
    | identifier parenBlock?
    ;

literal
    : NULL_LITERAL
    | STRING_LITERAL
    | signedNumber
    | BIT_STRING
    | HEX_STRING
    | TRUE
    | FALSE
    ;

signedNumber : (PLUS | MINUS)? NUMBER_LITERAL ;

// ---- 索引定义 ----

indexDefinition
    : constraintName? PRIMARY KEY indexType? keyPartList indexOption*        # idxPrimary
    | constraintName? UNIQUE (INDEX | KEY)? identifier? indexType? keyPartList indexOption*  # idxUnique
    | (INDEX | KEY) identifier? indexType? keyPartList indexOption*          # idxPlain
    | (FULLTEXT | SPATIAL) (INDEX | KEY)? identifier? keyPartList indexOption*  # idxFulltext
    | constraintName? FOREIGN KEY identifier? keyPartList referenceDefinition # idxForeign
    | constraintName? CHECK parenBlock (NOT? ENFORCED)?                       # idxCheck
    ;

constraintName : CONSTRAINT identifier? ;

indexType      : USING identifier ;

keyPartList    : LPAREN keyPart (COMMA keyPart)* RPAREN ;

/** 列名 [(前缀长度)] [ASC|DESC]，或 8.0 的函数索引 ((expr)) */
keyPart        : (identifier | parenBlock) lengthSpec? (ASC | DESC)? ;

indexOption
    : USING identifier
    | KEY_BLOCK_SIZE EQ? signedNumber
    | COMMENT STRING_LITERAL
    | (VISIBLE | INVISIBLE)
    | WITH PARSER identifier
    | ENGINE_ATTRIBUTE EQ? STRING_LITERAL
    | SECONDARY_ENGINE_ATTRIBUTE EQ? STRING_LITERAL
    ;

referenceDefinition
    : REFERENCES tableName keyPartList? referenceAction*
    ;

referenceAction
    : MATCH identifier
    | ON (DELETE | UPDATE) (RESTRICT | CASCADE | SET NULL_LITERAL | NO ACTION | SET DEFAULT)
    ;

// ---- 配平括号：表达式一律整段吃掉 ----

parenBlock
    : LPAREN (~(LPAREN | RPAREN) | parenBlock)* RPAREN
    ;

// ============================ 标识符 ============================

identifier
    : IDENTIFIER
    | BACKTICK_QUOTED
    | DOUBLE_QUOTED
    | nonReservedKeyword
    ;

/**
 * 可以当标识符用的关键字。MySQL 的非保留字有几百个，这里只列本语法定义了 token 的那些——
 * 没定义 token 的词本来就会被词法器归到 IDENTIFIER，不需要在这里出现。
 * 列不全的后果是"某张表解析不了"，会被阶段 1 的启动自检当场发现，不会静默出错。
 */
nonReservedKeyword
    : COMMENT | ENGINE | CHARSET | TEMPORARY | FIRST | AFTER | ACTION | ENFORCED
    | VISIBLE | INVISIBLE | STORAGE | COLUMN_FORMAT | SRID | PARSER | KEY_BLOCK_SIZE
    | TEXT | JSON | BIT | DATE | TIME | TIMESTAMP | DATETIME | YEAR | ENUM
    | BOOL | BOOLEAN | FIXED | SERIAL | VALUE | NO | LANGUAGE | DEFINER
    | POINT | POLYGON | LINESTRING | GEOMETRY | GEOMETRYCOLLECTION
    | MULTIPOINT | MULTILINESTRING | MULTIPOLYGON
    | NATIONAL | NCHAR | NVARCHAR | PRECISION | VARYING
    | ENGINE_ATTRIBUTE | SECONDARY_ENGINE_ATTRIBUTE
    | MEDIUMTEXT | LONGTEXT | TINYTEXT | MEDIUMBLOB | LONGBLOB | TINYBLOB
    | ROW_FORMAT | AVG_ROW_LENGTH | CHECKSUM | CONNECTION | COMPRESSION
    | AUTOEXTEND_SIZE | START | TRANSACTION | ALWAYS
    | ONLINE | OFFLINE | MODIFY | PARTITION
    ;

// ============================ 词法 ============================

SEMI   : ';' ;
COMMA  : ',' ;
DOT    : '.' ;
LPAREN : '(' ;
RPAREN : ')' ;
EQ     : '=' ;
PLUS   : '+' ;
MINUS  : '-' ;

/*
 * 块注释整段跳过——注意这会连同 MySQL 的版本注释一起吃掉，如 SHOW CREATE TABLE 尾部的
 * /*!50100 PARTITION BY ... *\/ 和 ALTER 里的 /*!80000 ALGORITHM=INSTANT *\/。
 * 对本语法来说正是想要的：分区与算法子句都不影响列布局。
 * 代价是"整条语句被版本注释包起来"的形态会被读成空语句——那种形态只出现在
 * 触发器/存储程序的 dump 里，不在我们建模的范围内，且会被 listener 当成解析失败上报。
 */
BLOCK_COMMENT : '/*' .*? '*/' -> skip ;
LINE_COMMENT  : ('--' [ \t] | '#') ~[\r\n]* -> skip ;

/** MySQL 字符串：反斜杠转义 + 双写引号 */
STRING_LITERAL
    : '\'' ('\\' . | '\'\'' | ~['\\])* '\''
    ;

/** ANSI_QUOTES 关闭时 " 是字符串，打开时是标识符。当标识符处理，更常见也更安全 */
DOUBLE_QUOTED
    : '"' ('\\' . | '""' | ~["\\])* '"'
    ;

BACKTICK_QUOTED : '`' (~'`' | '``')* '`' ;

BIT_STRING : [b] '\'' [01]* '\'' | '0b' [01]+ ;
HEX_STRING : [x] '\'' [0-9a-f]* '\'' | '0x' [0-9a-f]+ ;

NUMBER_LITERAL : [0-9]+ ('.' [0-9]+)? ([e] [+-]? [0-9]+)? | '.' [0-9]+ ;

// ---- 关键字（必须在 IDENTIFIER 之前）----

CREATE : 'CREATE' ;
TABLE : 'TABLE' ;
TEMPORARY : 'TEMPORARY' ;
IF : 'IF' ;
NOT : 'NOT' ;
EXISTS : 'EXISTS' ;
LIKE : 'LIKE' ;
AS : 'AS' ;
SELECT : 'SELECT' ;
ALTER : 'ALTER' ;
ADD : 'ADD' ;
DROP : 'DROP' ;
MODIFY : 'MODIFY' ;
CHANGE : 'CHANGE' ;
RENAME : 'RENAME' ;
COLUMN : 'COLUMN' ;
TO : 'TO' ;
CONVERT : 'CONVERT' ;
ONLINE : 'ONLINE' ;
OFFLINE : 'OFFLINE' ;
IGNORE : 'IGNORE' ;
PARTITION : 'PARTITION' ;
ON : 'ON' ;
UPDATE : 'UPDATE' ;
DELETE : 'DELETE' ;
DEFAULT : 'DEFAULT' ;
AUTO_INCREMENT : 'AUTO_INCREMENT' ;
GENERATED : 'GENERATED' ;
ALWAYS : 'ALWAYS' ;
STORED : 'STORED' ;
VIRTUAL : 'VIRTUAL' ;
PRIMARY : 'PRIMARY' ;
KEY : 'KEY' ;
UNIQUE : 'UNIQUE' ;
INDEX : 'INDEX' ;
FULLTEXT : 'FULLTEXT' ;
SPATIAL : 'SPATIAL' ;
FOREIGN : 'FOREIGN' ;
REFERENCES : 'REFERENCES' ;
CONSTRAINT : 'CONSTRAINT' ;
CHECK : 'CHECK' ;
ENFORCED : 'ENFORCED' ;
MATCH : 'MATCH' ;
RESTRICT : 'RESTRICT' ;
CASCADE : 'CASCADE' ;
NO : 'NO' ;
ACTION : 'ACTION' ;
USING : 'USING' ;
WITH : 'WITH' ;
PARSER : 'PARSER' ;
KEY_BLOCK_SIZE : 'KEY_BLOCK_SIZE' ;
ENGINE_ATTRIBUTE : 'ENGINE_ATTRIBUTE' ;
SECONDARY_ENGINE_ATTRIBUTE : 'SECONDARY_ENGINE_ATTRIBUTE' ;
COMMENT : 'COMMENT' ;
COLLATE : 'COLLATE' ;
CHARACTER : 'CHARACTER' ;
CHARSET : 'CHARSET' ;
SET : 'SET' ;
ASC : 'ASC' ;
DESC : 'DESC' ;
VISIBLE : 'VISIBLE' ;
INVISIBLE : 'INVISIBLE' ;
COLUMN_FORMAT : 'COLUMN_FORMAT' ;
STORAGE : 'STORAGE' ;
SRID : 'SRID' ;
ENGINE : 'ENGINE' ;
ROW_FORMAT : 'ROW_FORMAT' ;
AVG_ROW_LENGTH : 'AVG_ROW_LENGTH' ;
CHECKSUM : 'CHECKSUM' ;
CONNECTION : 'CONNECTION' ;
COMPRESSION : 'COMPRESSION' ;
AUTOEXTEND_SIZE : 'AUTOEXTEND_SIZE' ;
START : 'START' ;
TRANSACTION : 'TRANSACTION' ;
LANGUAGE : 'LANGUAGE' ;
DEFINER : 'DEFINER' ;
FIRST : 'FIRST' ;
AFTER : 'AFTER' ;
VALUE : 'VALUE' ;
NULL_LITERAL : 'NULL' ;
TRUE : 'TRUE' ;
FALSE : 'FALSE' ;
UNSIGNED : 'UNSIGNED' ;
SIGNED : 'SIGNED' ;
ZEROFILL : 'ZEROFILL' ;
NATIONAL : 'NATIONAL' ;
PRECISION : 'PRECISION' ;
VARYING : 'VARYING' ;

TINYINT : 'TINYINT' ;
SMALLINT : 'SMALLINT' ;
MEDIUMINT : 'MEDIUMINT' ;
MIDDLEINT : 'MIDDLEINT' ;
INT : 'INT' ;
INTEGER : 'INTEGER' ;
BIGINT : 'BIGINT' ;
INT1 : 'INT1' ;
INT2 : 'INT2' ;
INT3 : 'INT3' ;
INT4 : 'INT4' ;
INT8 : 'INT8' ;
REAL : 'REAL' ;
DOUBLE : 'DOUBLE' ;
FLOAT : 'FLOAT' ;
FLOAT4 : 'FLOAT4' ;
FLOAT8 : 'FLOAT8' ;
DECIMAL : 'DECIMAL' ;
DEC : 'DEC' ;
NUMERIC : 'NUMERIC' ;
FIXED : 'FIXED' ;
BIT : 'BIT' ;
BOOL : 'BOOL' ;
BOOLEAN : 'BOOLEAN' ;
SERIAL : 'SERIAL' ;
DATE : 'DATE' ;
TIME : 'TIME' ;
TIMESTAMP : 'TIMESTAMP' ;
DATETIME : 'DATETIME' ;
YEAR : 'YEAR' ;
CHAR : 'CHAR' ;
VARCHAR : 'VARCHAR' ;
NCHAR : 'NCHAR' ;
NVARCHAR : 'NVARCHAR' ;
BINARY : 'BINARY' ;
VARBINARY : 'VARBINARY' ;
TINYBLOB : 'TINYBLOB' ;
BLOB : 'BLOB' ;
MEDIUMBLOB : 'MEDIUMBLOB' ;
LONGBLOB : 'LONGBLOB' ;
LONG : 'LONG' ;
TINYTEXT : 'TINYTEXT' ;
TEXT : 'TEXT' ;
MEDIUMTEXT : 'MEDIUMTEXT' ;
LONGTEXT : 'LONGTEXT' ;
ENUM : 'ENUM' ;
JSON : 'JSON' ;
GEOMETRY : 'GEOMETRY' ;
POINT : 'POINT' ;
LINESTRING : 'LINESTRING' ;
POLYGON : 'POLYGON' ;
MULTIPOINT : 'MULTIPOINT' ;
MULTILINESTRING : 'MULTILINESTRING' ;
MULTIPOLYGON : 'MULTIPOLYGON' ;
GEOMETRYCOLLECTION : 'GEOMETRYCOLLECTION' ;

IDENTIFIER : [a-z_$\u0080-\uFFFF] [a-z0-9_$\u0080-\uFFFF]* ;

WS : [ \t\r\n]+ -> skip ;

/** 兜底：任何没被上面认领的字符。落到解析错误里，比词法阶段就炸掉更好定位 */
ANY_CHAR : . ;
