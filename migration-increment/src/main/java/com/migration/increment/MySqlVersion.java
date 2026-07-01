package com.migration.increment;

import java.util.Properties;

public class MySqlVersion implements Comparable<MySqlVersion> {
    private final int major;
    private final int minor;
    private final int patch;

    public static final MySqlVersion V5_6 = new MySqlVersion(5, 6, 0);
    public static final MySqlVersion V5_7 = new MySqlVersion(5, 7, 0);
    public static final MySqlVersion V8_0 = new MySqlVersion(8, 0, 0);

    public MySqlVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    public static MySqlVersion parse(String versionStr) {
        if (versionStr == null || versionStr.trim().isEmpty()) {
            return V8_0;
        }
        String trimmed = versionStr.trim();
        String[] parts = trimmed.split("\\.");
        int maj = parts.length > 0 ? Integer.parseInt(parts[0]) : 5;
        int min = parts.length > 1 ? Integer.parseInt(parts[1]) : 7;
        int pat = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
        return new MySqlVersion(maj, min, pat);
    }

    public static MySqlVersion fromProperties(Properties props, String key) {
        return parse(props.getProperty(key, "8.0"));
    }

    public boolean isAtLeast(MySqlVersion other) {
        return this.compareTo(other) >= 0;
    }

    public boolean isBefore(MySqlVersion other) {
        return this.compareTo(other) < 0;
    }

    public boolean supportsWindowFunctions() {
        return isAtLeast(V8_0);
    }

    public boolean supportsCTE() {
        return isAtLeast(V8_0);
    }

    public boolean supportsLateral() {
        return isAtLeast(V8_0);
    }

    public boolean supportsValuesRow() {
        return isAtLeast(V8_0);
    }

    public boolean supportsJSON() {
        return isAtLeast(V5_7);
    }

    public boolean supportsGeneratedColumns() {
        return isAtLeast(V5_7);
    }

    public boolean supportsDefaultAuthPlugin() {
        return isAtLeast(V8_0);
    }

    public boolean supportsRoles() {
        return isAtLeast(V8_0);
    }

    public boolean supportsResourceGroups() {
        return isAtLeast(V8_0);
    }

    public boolean supportsInvisibleIndexes() {
        return isAtLeast(V8_0);
    }

    public boolean supportsDescendingIndexes() {
        return isAtLeast(V8_0);
    }

    public boolean supportsFunctionalIndexes() {
        return isAtLeast(V8_0);
    }

    public boolean supportsWithClause() {
        return isAtLeast(V8_0);
    }

    public boolean supportsInsertIgnoreOnDuplicate() {
        return isAtLeast(V5_6);
    }

    public boolean supportsOnlineDDL() {
        return isAtLeast(V5_6);
    }

    public boolean supportsRenameIndex() {
        return isAtLeast(V5_7);
    }

    public boolean supportsRenameColumn() {
        return isAtLeast(V8_0);
    }

    @Override
    public int compareTo(MySqlVersion other) {
        if (this.major != other.major) return Integer.compare(this.major, other.major);
        if (this.minor != other.minor) return Integer.compare(this.minor, other.minor);
        return Integer.compare(this.patch, other.patch);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MySqlVersion)) return false;
        MySqlVersion other = (MySqlVersion) obj;
        return this.major == other.major && this.minor == other.minor && this.patch == other.patch;
    }

    @Override
    public int hashCode() {
        return major * 10000 + minor * 100 + patch;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + patch;
    }

    public String toCompactString() {
        return major + "." + minor;
    }
}
