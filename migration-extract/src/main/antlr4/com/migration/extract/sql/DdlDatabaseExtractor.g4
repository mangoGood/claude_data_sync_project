grammar DdlDatabaseExtractor;

options { caseInsensitive = true; }

statement
    : ddlStatement EOF
    | otherStatement EOF
    ;

ddlStatement
    : createTableStatement
    | alterTableStatement
    | dropTableStatement
    | truncateStatement
    | renameTableStatement
    | createIndexStatement
    | dropIndexStatement
    | createDatabaseStatement
    | dropDatabaseStatement
    ;

createTableStatement
    : CREATE (OR REPLACE)? TEMPORARY? TABLE ifNotExists? tableName remainingSql
    ;

alterTableStatement
    : ALTER TABLE tableName remainingSql
    ;

dropTableStatement
    : DROP TABLE ifExists? tableName (COMMA tableName)* remainingSql?
    ;

truncateStatement
    : TRUNCATE TABLE? tableName remainingSql?
    ;

renameTableStatement
    : RENAME TABLE tableName TO tableName (COMMA tableName TO tableName)* remainingSql?
    ;

createIndexStatement
    : CREATE (UNIQUE | FULLTEXT | SPATIAL)? INDEX indexName ON tableName remainingSql
    ;

dropIndexStatement
    : DROP INDEX ifExists? indexName ON tableName remainingSql?
    ;

createDatabaseStatement
    : CREATE (DATABASE | SCHEMA) ifNotExists? schemaName remainingSql?
    ;

dropDatabaseStatement
    : DROP (DATABASE | SCHEMA) ifExists? schemaName remainingSql?
    ;

otherStatement
    : remainingSql
    ;

ifNotExists
    : IF NOT EXISTS
    ;

ifExists
    : IF EXISTS
    ;

tableName
    : (schemaName DOT)? identifier
    ;

schemaName
    : identifier
    ;

indexName
    : identifier
    ;

identifier
    : IDENTIFIER
    | BACKTICK_QUOTED
    | keywordAsId
    ;

keywordAsId
    : TABLE | DATABASE | SCHEMA | INDEX | KEY | TEMPORARY | OR | REPLACE
    | IF | EXISTS | NOT | TO | AND | NO | CHAIN | RELEASE | WORK
    | BEGIN | COMMIT | ROLLBACK | TRANSACTION | SAVEPOINT | SET
    | INSERT | UPDATE | DELETE | SELECT | CREATE | ALTER | DROP | TRUNCATE
    | RENAME | USE | START | WITH | CONSISTENT | SNAPSHOT | READ | WRITE | ONLY
    | UNIQUE | FULLTEXT | SPATIAL | ON | IN | FROM | INTO | VALUES | WHERE
    | ADD | COLUMN | PRIMARY | FOREIGN | REFERENCES | CONSTRAINT | DEFAULT
    | AUTO_INCREMENT | NULL | NOT_NULL | INT | VARCHAR | CHAR | TEXT | BLOB
    | DECIMAL | FLOAT | DOUBLE | DATE | DATETIME | TIMESTAMP | TIME | YEAR
    | BOOLEAN | BIGINT | SMALLINT | TINYINT | MEDIUMINT | INTEGER
    | ENUM | SET_KW | JSON | BIT | BINARY | VARBINARY
    | PARTITION | PARTITIONS | LESS | THAN | MAXVALUE | VALUE | VALUES_KW
    | PROCEDURE | FUNCTION | TRIGGER | EVENT | VIEW | CURSOR
    | DEFINER | SQL | SECURITY | INVOKER | DETERMINISTIC | CONTAINS
    | COMMENT | ENGINE | CHARSET | COLLATE | CHARACTER
    ;

remainingSql
    : ~EOF*
    ;

SEMI: ';' ;
COMMA: ',' ;
DOT: '.' ;
LPAREN: '(' ;
RPAREN: ')' ;
EQ: '=' ;

BLOCK_COMMENT: '/*' .*? '*/' -> skip ;
LINE_COMMENT: ('--' | '#') ~[\r\n]* -> skip ;

STRING_LITERAL
    : '\'' (~'\'' | '\'\'')* '\''
    ;

BACKTICK_QUOTED: '`' (~'`' | '``')* '`' ;

NUMBER_LITERAL
    : [0-9]+ ('.' [0-9]+)? ([eE] [+-]? [0-9]+)?
    ;

CREATE: 'CREATE' ;
OR: 'OR' ;
REPLACE: 'REPLACE' ;
TEMPORARY: 'TEMPORARY' ;
TABLE: 'TABLE' ;
IF: 'IF' ;
NOT: 'NOT' ;
EXISTS: 'EXISTS' ;
ALTER: 'ALTER' ;
DROP: 'DROP' ;
TRUNCATE: 'TRUNCATE' ;
RENAME: 'RENAME' ;
TO: 'TO' ;
UNIQUE: 'UNIQUE' ;
FULLTEXT: 'FULLTEXT' ;
SPATIAL: 'SPATIAL' ;
INDEX: 'INDEX' ;
KEY: 'KEY' ;
ON: 'ON' ;
INSERT: 'INSERT' ;
UPDATE: 'UPDATE' ;
DELETE: 'DELETE' ;
SELECT: 'SELECT' ;
BEGIN: 'BEGIN' ;
COMMIT: 'COMMIT' ;
ROLLBACK: 'ROLLBACK' ;
START: 'START' ;
TRANSACTION: 'TRANSACTION' ;
SAVEPOINT: 'SAVEPOINT' ;
SET: 'SET' ;
WORK: 'WORK' ;
AND: 'AND' ;
NO: 'NO' ;
CHAIN: 'CHAIN' ;
RELEASE: 'RELEASE' ;
USE: 'USE' ;
DATABASE: 'DATABASE' ;
SCHEMA: 'SCHEMA' ;
WITH: 'WITH' ;
CONSISTENT: 'CONSISTENT' ;
SNAPSHOT: 'SNAPSHOT' ;
READ: 'READ' ;
WRITE: 'WRITE' ;
ONLY: 'ONLY' ;
IN: 'IN' ;
FROM: 'FROM' ;
INTO: 'INTO' ;
VALUES: 'VALUES' ;
WHERE: 'WHERE' ;
ADD: 'ADD' ;
COLUMN: 'COLUMN' ;
PRIMARY: 'PRIMARY' ;
FOREIGN: 'FOREIGN' ;
REFERENCES: 'REFERENCES' ;
CONSTRAINT: 'CONSTRAINT' ;
DEFAULT: 'DEFAULT' ;
AUTO_INCREMENT: 'AUTO_INCREMENT' ;
NULL: 'NULL' ;
NOT_NULL: 'NOT NULL' ;
INT: 'INT' ;
VARCHAR: 'VARCHAR' ;
CHAR: 'CHAR' ;
TEXT: 'TEXT' ;
BLOB: 'BLOB' ;
DECIMAL: 'DECIMAL' ;
FLOAT: 'FLOAT' ;
DOUBLE: 'DOUBLE' ;
DATE: 'DATE' ;
DATETIME: 'DATETIME' ;
TIMESTAMP: 'TIMESTAMP' ;
TIME: 'TIME' ;
YEAR: 'YEAR' ;
BOOLEAN: 'BOOLEAN' ;
BIGINT: 'BIGINT' ;
SMALLINT: 'SMALLINT' ;
TINYINT: 'TINYINT' ;
MEDIUMINT: 'MEDIUMINT' ;
INTEGER: 'INTEGER' ;
ENUM: 'ENUM' ;
SET_KW: 'SET' ;
JSON: 'JSON' ;
BIT: 'BIT' ;
BINARY: 'BINARY' ;
VARBINARY: 'VARBINARY' ;
PARTITION: 'PARTITION' ;
PARTITIONS: 'PARTITIONS' ;
LESS: 'LESS' ;
THAN: 'THAN' ;
MAXVALUE: 'MAXVALUE' ;
VALUE: 'VALUE' ;
VALUES_KW: 'VALUES' ;
PROCEDURE: 'PROCEDURE' ;
FUNCTION: 'FUNCTION' ;
TRIGGER: 'TRIGGER' ;
EVENT: 'EVENT' ;
VIEW: 'VIEW' ;
CURSOR: 'CURSOR' ;
DEFINER: 'DEFINER' ;
SQL: 'SQL' ;
SECURITY: 'SECURITY' ;
INVOKER: 'INVOKER' ;
DETERMINISTIC: 'DETERMINISTIC' ;
CONTAINS: 'CONTAINS' ;
COMMENT: 'COMMENT' ;
ENGINE: 'ENGINE' ;
CHARSET: 'CHARSET' ;
COLLATE: 'COLLATE' ;
CHARACTER: 'CHARACTER' ;

IDENTIFIER: [a-zA-Z_\u0080-\uffff] [a-zA-Z0-9_\u0080-\uffff]* ;

WS: [ \t\r\n]+ -> skip ;
