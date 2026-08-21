package com.migration.traffic.model;

/**
 * 语句类别。
 *
 * <p>分成两组，这个划分是回放正确性的基础：
 * <ul>
 *   <li><b>负载类</b>（{@link #isPayload()}）—— {@code SELECT}/{@code DML}/{@code DDL}/{@code DCL}，
 *       用户可以在回放时按类别筛选；</li>
 *   <li><b>会话状态类</b> —— {@code SET}/{@code USE}/{@code TCL}/{@code OTHER}，
 *       <b>永远回放</b>。它们不是负载而是会话状态：漏放一条 {@code SET autocommit=0}，
 *       其后所有 DML 的语义就变了；漏放一条 {@code USE}，后续非限定表名会落到错误的库。</li>
 * </ul>
 */
public enum StatementClass {
    /** 查询类：SELECT / SHOW / DESC / EXPLAIN 等只读语句。 */
    SELECT,
    /** 数据变更：INSERT / UPDATE / DELETE / REPLACE / LOAD DATA / CALL。 */
    DML,
    /** 结构变更：CREATE / ALTER / DROP / RENAME TABLE / TRUNCATE。 */
    DDL,
    /** 权限：GRANT / REVOKE / CREATE|ALTER|DROP|RENAME USER / SET PASSWORD / ROLE 相关。 */
    DCL,
    /** 事务控制：BEGIN / START TRANSACTION / COMMIT / ROLLBACK / SAVEPOINT / XA。 */
    TCL,
    /** 会话变量：SET（SET PASSWORD 除外，那是 DCL）。 */
    SET,
    /** 切换默认库：USE / Init DB。 */
    USE,
    /** 其余（LOCK TABLES / FLUSH / KILL / ...）。 */
    OTHER;

    /** 是否属于用户可筛选的负载类。 */
    public boolean isPayload() {
        return this == SELECT || this == DML || this == DDL || this == DCL;
    }

    /** 解析名称，不认识时返回 null（不抛异常：录制文件可能来自更新的版本）。 */
    public static StatementClass parse(String raw) {
        if (raw == null) return null;
        for (StatementClass c : values()) {
            if (c.name().equalsIgnoreCase(raw.trim())) return c;
        }
        return null;
    }
}
