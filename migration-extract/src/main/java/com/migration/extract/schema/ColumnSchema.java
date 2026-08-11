package com.migration.extract.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 一列的结构定义——表结构时序库的最小单元。
 *
 * <p>存的是<b>解析后的类型分量</b>（类型名 / 长度 / 标度 / unsigned / 字符集 / enum 取值表），
 * 而不是 {@code COLUMN_TYPE} 那样一整串文本。原因是同一份定义在不同 MySQL 版本下渲染出的
 * {@code COLUMN_TYPE} 并不相同（8.0.19 起整数类型不再显示宽度），存文本就等于把渲染口径
 * 焊死在写入时刻；存分量则可以按目标口径重新渲染，也才能和 {@code information_schema}
 * 逐列对得上（阶段 1 的启动自检就是干这个的）。
 *
 * <p>表达式一律<b>只存原文不求值</b>：{@code DEFAULT (...)}、{@code GENERATED ALWAYS AS (...)}
 * 都只是拿来重建 DDL 或给人看的，链路上没有任何一处需要知道它算出什么。这也是 DDL 语法能
 * 裁到几百行的前提——MySQL DDL 里最难的部分正是任意表达式。
 */
public class ColumnSchema {

    /** 列名，大小写按源库原样保留。 */
    private String name;

    /** 类型名，小写，口径对齐 {@code information_schema.COLUMNS.DATA_TYPE}（如 int / varchar / enum）。 */
    private String typeName;

    /** 长度 / 精度：varchar(64) 的 64、decimal(20,4) 的 20、int(11) 的显示宽度。无则 null。 */
    private Integer length;

    /** 标度：decimal(20,4) 的 4。无则 null。 */
    private Integer scale;

    private boolean unsigned;
    private boolean zerofill;

    /** enum/set 的取值表，按声明顺序（binlog 里给的是序号，靠这张表还原字面量）。 */
    private List<String> enumValues = new ArrayList<>();

    private String charset;
    private String collation;

    private boolean nullable = true;

    /** DEFAULT 的原文（未去引号），无默认值时 null。 */
    private String defaultExpr;

    private boolean autoIncrement;

    /** 生成列：binlog 行事件带着算好的值，但目标库拒绝显式写入（3105），下游要按列名剔除。 */
    private boolean generated;
    /** 生成列的存储方式：true=STORED，false=VIRTUAL。 */
    private boolean generatedStored;
    /** 生成表达式原文。 */
    private String generationExpr;

    /** ON UPDATE 子句原文（如 CURRENT_TIMESTAMP(3)），无则 null。 */
    private String onUpdate;

    private String comment;

    public ColumnSchema() {
    }

    public ColumnSchema(String name, String typeName) {
        this.name = name;
        this.typeName = typeName == null ? null : typeName.toLowerCase(Locale.ROOT);
    }

    /**
     * 渲染成 {@code information_schema.COLUMNS.COLUMN_TYPE} 那一串。
     *
     * <p>下游用它区分 {@code tinyint(1)}（布尔）/{@code bit(8)}/{@code unsigned}——
     * {@code DATA_TYPE} 把宽度和符号都丢了，只有这一串带得全。
     *
     * @param mode 显示宽度口径，见 {@link TypeRenderMode}
     */
    public String columnType(TypeRenderMode mode) {
        StringBuilder sb = new StringBuilder();
        sb.append(typeName == null ? "" : typeName);

        if (isEnumOrSet()) {
            sb.append('(');
            for (int i = 0; i < enumValues.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('\'').append(enumValues.get(i).replace("'", "''")).append('\'');
            }
            sb.append(')');
        } else {
            Integer len = effectiveLength(mode);
            if (len != null) {
                sb.append('(').append(len);
                Integer sc = effectiveScale();
                if (sc != null) {
                    sb.append(',').append(sc);
                }
                sb.append(')');
            }
        }

        if (unsigned) {
            sb.append(" unsigned");
        }
        if (zerofill) {
            sb.append(" zerofill");
        }
        return sb.toString();
    }

    /**
     * {@code COLUMN_TYPE} 里该显示的长度——DDL 里没写时 MySQL 会补默认值，这里要补一样的。
     *
     * <p>两类规则：
     * <ul>
     *   <li><b>与版本无关</b>：{@code DECIMAL} → {@code decimal(10,0)}、{@code BIT} → {@code bit(1)}、
     *       {@code CHAR} → {@code char(1)}。这些在 5.7 和 8.0 里都会补。</li>
     *   <li><b>与版本有关</b>：整数类型与 {@code YEAR} 的显示宽度在 8.0.19 起不再回显
     *       （{@code int(11)} → {@code int}），5.7 则会补出默认宽度。<b>两个例外必须保留宽度</b>：
     *       {@code tinyint(1)}——它是 MySQL 表达 BOOLEAN 的唯一方式，驱动的 {@code tinyInt1isBit}
     *       就认这一串，抹掉宽度会让布尔列在下游变成普通整数；以及带 {@code ZEROFILL} 的列，
     *       宽度是补零位数、有实际语义。</li>
     * </ul>
     */
    private Integer effectiveLength(TypeRenderMode mode) {
        if (isIntegerType() || "year".equals(typeName)) {
            if (zerofill) {
                return length != null ? length : defaultIntegerWidth();
            }
            if ("tinyint".equals(typeName) && length != null && length == 1) {
                return 1;
            }
            if (mode == TypeRenderMode.NO_DISPLAY_WIDTH) {
                return null;
            }
            return length != null ? length : defaultIntegerWidth();
        }
        if ("decimal".equals(typeName)) {
            return length != null ? length : 10;
        }
        if ("bit".equals(typeName) || "char".equals(typeName) || "binary".equals(typeName)) {
            return length != null ? length : 1;
        }
        return length;
    }

    /** decimal 只写了精度时标度补 0（{@code decimal(20)} → {@code decimal(20,0)}）。 */
    private Integer effectiveScale() {
        if ("decimal".equals(typeName)) {
            return scale != null ? scale : 0;
        }
        return scale;
    }

    /** 5.7 口径下整数类型不写宽度时 MySQL 补的默认显示宽度（unsigned 少一位符号位）。 */
    private Integer defaultIntegerWidth() {
        switch (typeName == null ? "" : typeName) {
            case "tinyint":   return unsigned ? 3 : 4;
            case "smallint":  return unsigned ? 5 : 6;
            case "mediumint": return unsigned ? 8 : 9;
            case "int":
            case "integer":   return unsigned ? 10 : 11;
            case "bigint":    return 20;
            case "year":      return 4;
            default:          return null;
        }
    }

    public boolean isIntegerType() {
        return "tinyint".equals(typeName) || "smallint".equals(typeName)
                || "mediumint".equals(typeName) || "int".equals(typeName)
                || "integer".equals(typeName) || "bigint".equals(typeName);
    }

    public boolean isEnumOrSet() {
        return "enum".equals(typeName) || "set".equals(typeName);
    }

    public ColumnSchema copy() {
        ColumnSchema c = new ColumnSchema();
        c.name = name;
        c.typeName = typeName;
        c.length = length;
        c.scale = scale;
        c.unsigned = unsigned;
        c.zerofill = zerofill;
        c.enumValues = new ArrayList<>(enumValues);
        c.charset = charset;
        c.collation = collation;
        c.nullable = nullable;
        c.defaultExpr = defaultExpr;
        c.autoIncrement = autoIncrement;
        c.generated = generated;
        c.generatedStored = generatedStored;
        c.generationExpr = generationExpr;
        c.onUpdate = onUpdate;
        c.comment = comment;
        return c;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getTypeName() { return typeName; }
    public void setTypeName(String typeName) {
        this.typeName = typeName == null ? null : typeName.toLowerCase(Locale.ROOT);
    }

    public Integer getLength() { return length; }
    public void setLength(Integer length) { this.length = length; }

    public Integer getScale() { return scale; }
    public void setScale(Integer scale) { this.scale = scale; }

    public boolean isUnsigned() { return unsigned; }
    public void setUnsigned(boolean unsigned) { this.unsigned = unsigned; }

    public boolean isZerofill() { return zerofill; }
    public void setZerofill(boolean zerofill) { this.zerofill = zerofill; }

    public List<String> getEnumValues() { return enumValues; }
    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues == null ? new ArrayList<>() : new ArrayList<>(enumValues);
    }

    public String getCharset() { return charset; }
    public void setCharset(String charset) { this.charset = charset; }

    public String getCollation() { return collation; }
    public void setCollation(String collation) { this.collation = collation; }

    public boolean isNullable() { return nullable; }
    public void setNullable(boolean nullable) { this.nullable = nullable; }

    public String getDefaultExpr() { return defaultExpr; }
    public void setDefaultExpr(String defaultExpr) { this.defaultExpr = defaultExpr; }

    public boolean isAutoIncrement() { return autoIncrement; }
    public void setAutoIncrement(boolean autoIncrement) { this.autoIncrement = autoIncrement; }

    public boolean isGenerated() { return generated; }
    public void setGenerated(boolean generated) { this.generated = generated; }

    public boolean isGeneratedStored() { return generatedStored; }
    public void setGeneratedStored(boolean generatedStored) { this.generatedStored = generatedStored; }

    public String getGenerationExpr() { return generationExpr; }
    public void setGenerationExpr(String generationExpr) { this.generationExpr = generationExpr; }

    public String getOnUpdate() { return onUpdate; }
    public void setOnUpdate(String onUpdate) { this.onUpdate = onUpdate; }

    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnSchema)) return false;
        ColumnSchema that = (ColumnSchema) o;
        return unsigned == that.unsigned && zerofill == that.zerofill
                && nullable == that.nullable && autoIncrement == that.autoIncrement
                && generated == that.generated && generatedStored == that.generatedStored
                && Objects.equals(name, that.name) && Objects.equals(typeName, that.typeName)
                && Objects.equals(length, that.length) && Objects.equals(scale, that.scale)
                && Objects.equals(enumValues, that.enumValues)
                && Objects.equals(charset, that.charset) && Objects.equals(collation, that.collation)
                && Objects.equals(defaultExpr, that.defaultExpr)
                && Objects.equals(generationExpr, that.generationExpr)
                && Objects.equals(onUpdate, that.onUpdate) && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, typeName, length, scale, unsigned, zerofill, enumValues,
                charset, collation, nullable, defaultExpr, autoIncrement, generated,
                generatedStored, generationExpr, onUpdate, comment);
    }

    @Override
    public String toString() {
        return name + " " + columnType(TypeRenderMode.NO_DISPLAY_WIDTH);
    }
}
