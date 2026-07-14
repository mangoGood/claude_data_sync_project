#!/bin/bash

set -e

SOURCE_HOST="192.168.107.6"
SOURCE_PORT="3306"
SOURCE_USER="root"
SOURCE_PASS="rootpassword"
SOURCE_DB="test_db1"

TARGET_HOST="192.168.107.7"
TARGET_PORT="3306"
TARGET_USER="root"
TARGET_PASS="rootpassword"
TARGET_DB="test_db1"

TASK_ID="test1"
PROJECT_DIR="/Users/finn/Documents/git_projects/test_git"
FILES_DIR="$PROJECT_DIR/files/$TASK_ID"

MYSQL_CMD_SOURCE="mysql -h$SOURCE_HOST -P$SOURCE_PORT -u$SOURCE_USER -p$SOURCE_PASS $SOURCE_DB -N"
MYSQL_CMD_TARGET="mysql -h$TARGET_HOST -P$TARGET_PORT -u$TARGET_USER -p$TARGET_PASS $TARGET_DB -N"

echo "=========================================="
echo "MySQL Migration Test Script"
echo "=========================================="

clean_environment() {
    echo "[Step 1] Cleaning environment..."
    
    rm -rf $FILES_DIR/binlog_output/*
    rm -rf $FILES_DIR/thl_output/*
    rm -rf $FILES_DIR/checkpoint/*
    
    $MYSQL_CMD_SOURCE -e "DROP TABLE IF EXISTS test_all_types; DROP TABLE IF EXISTS test_batch;"
    $MYSQL_CMD_TARGET -e "DROP TABLE IF EXISTS test_all_types; DROP TABLE IF EXISTS test_batch;"
    
    echo "Environment cleaned."
}

create_test_tables() {
    echo "[Step 2] Creating test tables with all MySQL types..."
    
    $MYSQL_CMD_SOURCE -e "
CREATE TABLE IF NOT EXISTS test_all_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    
    col_tinyint TINYINT,
    col_tinyint_unsigned TINYINT UNSIGNED,
    col_smallint SMALLINT,
    col_smallint_unsigned SMALLINT UNSIGNED,
    col_mediumint MEDIUMINT,
    col_mediumint_unsigned MEDIUMINT UNSIGNED,
    col_int INT,
    col_int_unsigned INT UNSIGNED,
    col_bigint BIGINT,
    col_bigint_unsigned BIGINT UNSIGNED,
    
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(20, 6),
    
    col_bit BIT(8),
    
    col_date DATE,
    col_datetime DATETIME(3),
    col_timestamp TIMESTAMP(3),
    col_time TIME(3),
    col_year YEAR,
    
    col_char CHAR(10),
    col_varchar VARCHAR(255),
    col_binary BINARY(10),
    col_varbinary VARBINARY(255),
    
    col_tinytext TINYTEXT,
    col_text TEXT,
    col_mediumtext MEDIUMTEXT,
    col_longtext LONGTEXT,
    
    col_tinyblob TINYBLOB,
    col_blob BLOB,
    col_mediumblob MEDIUMBLOB,
    col_longblob LONGBLOB,
    
    col_enum ENUM('value1', 'value2', 'value3'),
    col_set SET('a', 'b', 'c', 'd'),
    
    col_json JSON,
    
    col_nullable INT,
    
    INDEX idx_int (col_int),
    INDEX idx_varchar (col_varchar)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

    $MYSQL_CMD_TARGET -e "
CREATE TABLE IF NOT EXISTS test_all_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    
    col_tinyint TINYINT,
    col_tinyint_unsigned TINYINT UNSIGNED,
    col_smallint SMALLINT,
    col_smallint_unsigned SMALLINT UNSIGNED,
    col_mediumint MEDIUMINT,
    col_mediumint_unsigned MEDIUMINT UNSIGNED,
    col_int INT,
    col_int_unsigned INT UNSIGNED,
    col_bigint BIGINT,
    col_bigint_unsigned BIGINT UNSIGNED,
    
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(20, 6),
    
    col_bit BIT(8),
    
    col_date DATE,
    col_datetime DATETIME(3),
    col_timestamp TIMESTAMP(3),
    col_time TIME(3),
    col_year YEAR,
    
    col_char CHAR(10),
    col_varchar VARCHAR(255),
    col_binary BINARY(10),
    col_varbinary VARBINARY(255),
    
    col_tinytext TINYTEXT,
    col_text TEXT,
    col_mediumtext MEDIUMTEXT,
    col_longtext LONGTEXT,
    
    col_tinyblob TINYBLOB,
    col_blob BLOB,
    col_mediumblob MEDIUMBLOB,
    col_longblob LONGBLOB,
    
    col_enum ENUM('value1', 'value2', 'value3'),
    col_set SET('a', 'b', 'c', 'd'),
    
    col_json JSON,
    
    col_nullable INT,
    
    INDEX idx_int (col_int),
    INDEX idx_varchar (col_varchar)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

    $MYSQL_CMD_SOURCE -e "CREATE TABLE IF NOT EXISTS test_batch (id INT PRIMARY KEY, name VARCHAR(100), value INT);"
    $MYSQL_CMD_TARGET -e "CREATE TABLE IF NOT EXISTS test_batch (id INT PRIMARY KEY, name VARCHAR(100), value INT);"

    echo "Test tables created."
}

start_processes() {
    echo "[Step 3] Starting processes..."
    
    cd $PROJECT_DIR
    
    echo "Stopping any existing processes..."
    pkill -f "migration-capture.*test1" 2>/dev/null || true
    pkill -f "migration-extract.*test1" 2>/dev/null || true
    pkill -f "migration-increment.*test1" 2>/dev/null || true
    sleep 2
    
    echo "Starting CaptureMain..."
    java -Dtask.id=$TASK_ID -jar migration-capture/target/migration-capture-1.0.0.jar \
        --config $FILES_DIR/config.properties > $FILES_DIR/capture.log 2>&1 &
    CAPTURE_PID=$!
    echo "CaptureMain started with PID: $CAPTURE_PID"
    
    sleep 3
    
    echo "Starting ContinuousExtractMain..."
    java -Dtask.id=$TASK_ID -jar migration-extract/target/migration-extract-1.0.0.jar \
        --config $FILES_DIR/config.properties > $FILES_DIR/extract.log 2>&1 &
    EXTRACT_PID=$!
    echo "ContinuousExtractMain started with PID: $EXTRACT_PID"
    
    sleep 3
    
    echo "Starting ContinuousIncrementMain..."
    java -Dtask.id=$TASK_ID -jar migration-increment/target/migration-increment-1.0.0.jar \
        --config $FILES_DIR/config.properties > $FILES_DIR/increment.log 2>&1 &
    INCREMENT_PID=$!
    echo "ContinuousIncrementMain started with PID: $INCREMENT_PID"
    
    echo "All processes started. Waiting for initialization..."
    sleep 5
    
    echo $CAPTURE_PID > $FILES_DIR/.capture_pid
    echo $EXTRACT_PID > $FILES_DIR/.extract_pid
    echo $INCREMENT_PID > $FILES_DIR/.increment_pid
}

stop_processes() {
    echo "Stopping processes..."
    
    if [ -f $FILES_DIR/.capture_pid ]; then
        kill $(cat $FILES_DIR/.capture_pid) 2>/dev/null || true
    fi
    if [ -f $FILES_DIR/.extract_pid ]; then
        kill $(cat $FILES_DIR/.extract_pid) 2>/dev/null || true
    fi
    if [ -f $FILES_DIR/.increment_pid ]; then
        kill $(cat $FILES_DIR/.increment_pid) 2>/dev/null || true
    fi
    
    echo "Processes stopped."
}

test_all_types_insert() {
    echo "[Step 4] Testing all MySQL types INSERT..."
    
    local count=0
    local batch_size=100
    local total=1000
    
    echo "Inserting $total rows with all types..."
    
    while [ $count -lt $total ]; do
        local sql=""
        for i in $(seq 1 $batch_size); do
            local idx=$((count + i))
            if [ $idx -gt $total ]; then
                break
            fi
            
            local tinyint_val=$((idx % 256 - 128))
            local tinyint_u_val=$((idx % 256))
            local smallint_val=$((idx % 65536 - 32768))
            local smallint_u_val=$((idx % 65536))
            local mediumint_val=$((idx % 16777216 - 8388608))
            local mediumint_u_val=$((idx % 16777216))
            local int_val=$idx
            local int_u_val=$idx
            local bigint_val=$idx
            local bigint_u_val=$idx
            
            local float_val="1.$idx"
            local double_val="2.$idx"
            local decimal_val="$idx.123456"
            
            local bit_val=$((idx % 256))
            
            local date_val="2026-04-22"
            local datetime_val="2026-04-22 10:30:45.$idx"
            local timestamp_val="2026-04-22 10:30:45.$idx"
            local time_val="10:30:45"
            local year_val="2026"
            
            local char_val="char_$idx"
            local varchar_val="varchar_test_data_$idx"
            
            local tinytext_val="tinytext_$idx"
            local text_val="text_data_$idx"
            local mediumtext_val="mediumtext_data_$idx"
            local longtext_val="longtext_data_$idx_long_text_content"
            
            local hex_val=$(printf '%02x' $((idx % 256)))
            local hex_val2=$(printf '%02x' $(((idx * 7) % 256)))
            local hex_val3=$(printf '%02x' $(((idx * 3) % 256)))
            local hex_val4=$(printf '%02x' $(((idx * 5) % 256)))
            
            local enum_idx=$((idx % 3 + 1))
            local enum_val="value$enum_idx"
            
            local set_val="a,b"
            
            local json_val='{"id": '$idx', "name": "test_'$idx'"}'
            
            local nullable_val="NULL"
            if [ $((idx % 2)) -eq 0 ]; then
                nullable_val=$idx
            fi
            
            sql="${sql}INSERT INTO test_all_types (
                col_tinyint, col_tinyint_unsigned,
                col_smallint, col_smallint_unsigned,
                col_mediumint, col_mediumint_unsigned,
                col_int, col_int_unsigned,
                col_bigint, col_bigint_unsigned,
                col_float, col_double, col_decimal,
                col_bit,
                col_date, col_datetime, col_timestamp, col_time, col_year,
                col_char, col_varchar, col_binary, col_varbinary,
                col_tinytext, col_text, col_mediumtext, col_longtext,
                col_tinyblob, col_blob, col_mediumblob, col_longblob,
                col_enum, col_set, col_json, col_nullable
            ) VALUES (
                $tinyint_val, $tinyint_u_val,
                $smallint_val, $smallint_u_val,
                $mediumint_val, $mediumint_u_val,
                $int_val, $int_u_val,
                $bigint_val, $bigint_u_val,
                $float_val, $double_val, $decimal_val,
                $bit_val,
                '$date_val', '$datetime_val', '$timestamp_val', '$time_val', $year_val,
                '$char_val', '$varchar_val', UNHEX('$hex_val'), UNHEX('$hex_val2'),
                '$tinytext_val', '$text_val', '$mediumtext_val', '$longtext_val',
                UNHEX('$hex_val'), UNHEX('$hex_val2'), UNHEX('$hex_val3'), UNHEX('$hex_val4'),
                '$enum_val', '$set_val', '$json_val', $nullable_val
            );"
        done
        
        $MYSQL_CMD_SOURCE -e "$sql"
        count=$((count + batch_size))
        
        if [ $((count % 500)) -eq 0 ]; then
            echo "  Inserted $count rows..."
        fi
    done
    
    echo "All types INSERT completed: $total rows"
}

test_batch_operations() {
    echo "[Step 5] Testing batch INSERT/UPDATE/DELETE (10000+ rows)..."
    
    echo "Batch INSERT 10000 rows..."
    local sql=""
    for i in $(seq 1 10000); do
        sql="${sql}INSERT INTO test_batch (id, name, value) VALUES ($i, 'name_$i', $i);"
    done
    $MYSQL_CMD_SOURCE -e "$sql"
    echo "Batch INSERT completed: 10000 rows"
    
    sleep 2
    
    echo "Batch UPDATE 5000 rows..."
    sql=""
    for i in $(seq 1 5000); do
        sql="${sql}UPDATE test_batch SET name='updated_$i', value=value*2 WHERE id=$i;"
    done
    $MYSQL_CMD_SOURCE -e "$sql"
    echo "Batch UPDATE completed: 5000 rows"
    
    sleep 2
    
    echo "Batch DELETE 3000 rows..."
    sql=""
    for i in $(seq 1 3000); do
        sql="${sql}DELETE FROM test_batch WHERE id=$i;"
    done
    $MYSQL_CMD_SOURCE -e "$sql"
    echo "Batch DELETE completed: 3000 rows"
}

test_multi_statement_sql() {
    echo "[Step 6] Testing multi-statement SQL..."
    
    $MYSQL_CMD_SOURCE -e "
INSERT INTO test_batch (id, name, value) VALUES (10001, 'multi_1', 100);
INSERT INTO test_batch (id, name, value) VALUES (10002, 'multi_2', 200);
INSERT INTO test_batch (id, name, value) VALUES (10003, 'multi_3', 300);
UPDATE test_batch SET name='multi_updated' WHERE id IN (10001, 10002, 10003);
DELETE FROM test_batch WHERE id = 10003;
"
    echo "Multi-statement SQL completed"
}

test_boundary_values() {
    echo "[Step 7] Testing boundary values..."
    
    $MYSQL_CMD_SOURCE -e "
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned,
    col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned,
    col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned,
    col_float, col_double, col_decimal,
    col_bit,
    col_date, col_datetime, col_timestamp, col_time, col_year,
    col_char, col_varchar, col_binary, col_varbinary,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_set, col_json, col_nullable
) VALUES (
    -128, 255,
    -32768, 65535,
    -8388608, 16777215,
    -2147483648, 4294967295,
    -9223372036854775808, 18446744073709551615,
    -3.402823466e38, 1.7976931348623157e308, 999999999999.999999,
    255,
    '1000-01-01', '1970-01-01 00:00:00.000', '1970-01-01 00:00:01.000', '-838:59:59', 1901,
    'boundary!', 'max varchar boundary test data here', UNHEX('ff'), UNHEX('ffff'),
    'tiny', 'text boundary', 'medium boundary', 'long boundary',
    UNHEX('00'), UNHEX('ff'), UNHEX('0000'), UNHEX('ffff'),
    'value1', 'a,b,c,d', '{\"boundary\": true}', NULL
);
"
    
    $MYSQL_CMD_SOURCE -e "
INSERT INTO test_all_types (
    col_tinyint, col_tinyint_unsigned,
    col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned,
    col_int, col_int_unsigned,
    col_bigint, col_bigint_unsigned,
    col_float, col_double, col_decimal,
    col_bit,
    col_date, col_datetime, col_timestamp, col_time, col_year,
    col_char, col_varchar, col_binary, col_varbinary,
    col_tinytext, col_text, col_mediumtext, col_longtext,
    col_tinyblob, col_blob, col_mediumblob, col_longblob,
    col_enum, col_set, col_json, col_nullable
) VALUES (
    127, 0,
    32767, 0,
    8388607, 0,
    2147483647, 0,
    9223372036854775807, 0,
    0, 0, 0.000000,
    0,
    '9999-12-31', '2038-01-19 03:14:07.999', '2038-01-19 03:14:07.999', '838:59:59', 2155,
    'maxchar!  ', '', UNHEX('00'), UNHEX('01'),
    '', '', '', '',
    UNHEX(''), UNHEX(''), UNHEX(''), UNHEX(''),
    'value3', '', 'null', NULL
);
"
    
    echo "Boundary values test completed"
}

verify_results() {
    echo "[Step 8] Verifying results..."
    
    sleep 10
    
    local source_all_types=$($MYSQL_CMD_SOURCE -e "SELECT COUNT(*) FROM test_all_types;")
    local target_all_types=$($MYSQL_CMD_TARGET -e "SELECT COUNT(*) FROM test_all_types;")
    
    local source_batch=$($MYSQL_CMD_SOURCE -e "SELECT COUNT(*) FROM test_batch;")
    local target_batch=$($MYSQL_CMD_TARGET -e "SELECT COUNT(*) FROM test_batch;")
    
    echo "test_all_types - Source: $source_all_types, Target: $target_all_types"
    echo "test_batch - Source: $source_batch, Target: $target_batch"
    
    if [ "$source_all_types" == "$target_all_types" ]; then
        echo "✓ test_all_types count matches"
    else
        echo "✗ test_all_types count mismatch!"
    fi
    
    if [ "$source_batch" == "$target_batch" ]; then
        echo "✓ test_batch count matches"
    else
        echo "✗ test_batch count mismatch!"
    fi
    
    echo "Verifying data consistency..."
    
    local source_checksum=$($MYSQL_CMD_SOURCE -e "SELECT MD5(GROUP_CONCAT(id ORDER BY id)) FROM test_batch;" 2>/dev/null)
    local target_checksum=$($MYSQL_CMD_TARGET -e "SELECT MD5(GROUP_CONCAT(id ORDER BY id)) FROM test_batch;" 2>/dev/null)
    
    if [ "$source_checksum" == "$target_checksum" ]; then
        echo "✓ test_batch data checksum matches"
    else
        echo "✗ test_batch data checksum mismatch!"
        echo "  Source checksum: $source_checksum"
        echo "  Target checksum: $target_checksum"
    fi
    
    echo ""
    echo "Sample data comparison:"
    echo "Source:"
    $MYSQL_CMD_SOURCE -e "SELECT * FROM test_batch ORDER BY id LIMIT 5;"
    echo ""
    echo "Target:"
    $MYSQL_CMD_TARGET -e "SELECT * FROM test_batch ORDER BY id LIMIT 5;"
}

print_summary() {
    echo ""
    echo "=========================================="
    echo "Test Summary"
    echo "=========================================="
    echo "Source DB: $SOURCE_HOST:$SOURCE_PORT/$SOURCE_DB"
    echo "Target DB: $TARGET_HOST:$TARGET_PORT/$TARGET_DB"
    echo "Task ID: $TASK_ID"
    echo ""
    echo "Tested operations:"
    echo "  - All MySQL types INSERT (1000 rows)"
    echo "  - Batch INSERT (10000 rows)"
    echo "  - Batch UPDATE (5000 rows)"
    echo "  - Batch DELETE (3000 rows)"
    echo "  - Multi-statement SQL"
    echo "  - Boundary values"
    echo ""
    echo "Total test rows: 12000+"
    echo ""
    echo "Log files:"
    echo "  - $FILES_DIR/capture.log"
    echo "  - $FILES_DIR/extract.log"
    echo "  - $FILES_DIR/increment.log"
    echo "=========================================="
}

case "${1:-all}" in
    clean)
        clean_environment
        ;;
    tables)
        create_test_tables
        ;;
    start)
        start_processes
        ;;
    stop)
        stop_processes
        ;;
    test-all-types)
        test_all_types_insert
        ;;
    test-batch)
        test_batch_operations
        ;;
    test-multi)
        test_multi_statement_sql
        ;;
    test-boundary)
        test_boundary_values
        ;;
    verify)
        verify_results
        ;;
    all)
        clean_environment
        create_test_tables
        start_processes
        test_all_types_insert
        test_batch_operations
        test_multi_statement_sql
        test_boundary_values
        verify_results
        print_summary
        ;;
    *)
        echo "Usage: $0 {clean|tables|start|stop|test-all-types|test-batch|test-multi|test-boundary|verify|all}"
        exit 1
        ;;
esac
