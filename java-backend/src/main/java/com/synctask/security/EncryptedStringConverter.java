package com.synctask.security;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA 属性转换器：对标注字段做落库前加密 / 读取后解密，实现连接串/口令的静态加密（at-rest）。
 *
 * <p>透明生效：实体字段照常用明文，DB 里存 {@code ENC:...} 密文。兼容历史明文行
 * （{@link CredentialCipher#decrypt} 对无前缀值原样返回）。
 */
@Converter
public class EncryptedStringConverter implements AttributeConverter<String, String> {

    @Override
    public String convertToDatabaseColumn(String attribute) {
        return CredentialCipher.encrypt(attribute);
    }

    @Override
    public String convertToEntityAttribute(String dbData) {
        return CredentialCipher.decrypt(dbData);
    }
}
