package com.synctask.security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Date;

@Component
public class JwtTokenProvider {
    private static final Logger logger = LoggerFactory.getLogger(JwtTokenProvider.class);

    @Value("${jwt.secret}")
    private String jwtSecret;

    @Value("${jwt.expiration}")
    private long jwtExpiration;

    /** 启动时解析一次的签名密钥；不再每次从明文 secret 现算。 */
    private SecretKey signingKey;

    @PostConstruct
    void initSigningKey() {
        byte[] keyBytes;
        if (jwtSecret == null || jwtSecret.isBlank()) {
            // 未注入 JWT_SECRET：生成随机密钥（仅开发）。重启后旧 token 失效——生产必须注入固定密钥。
            keyBytes = new byte[64];
            new SecureRandom().nextBytes(keyBytes);
            logger.warn("未配置 JWT_SECRET，已生成随机签名密钥（仅供开发；重启后所有 token 失效）。生产环境请注入 JWT_SECRET 环境变量！");
        } else {
            // 用 SHA-256 派生固定 32 字节密钥，保证满足 HS256 的最小长度，且与 secret 原始长度无关。
            keyBytes = sha256(jwtSecret);
        }
        this.signingKey = Keys.hmacShaKeyFor(keyBytes);
    }

    private static byte[] sha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("JWT 密钥派生失败", e);
        }
    }

    private SecretKey getSigningKey() {
        return signingKey;
    }

    public String generateToken(Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        Date now = new Date();
        Date expiryDate = new Date(now.getTime() + jwtExpiration);

        return Jwts.builder()
                .subject(String.valueOf(userPrincipal.getId()))
                .claim("username", userPrincipal.getUsername())
                .claim("role", userPrincipal.getRole())
                .claim("tv", userPrincipal.getTokenVersion() != null ? userPrincipal.getTokenVersion() : 0)
                .issuedAt(now)
                .expiration(expiryDate)
                .signWith(getSigningKey())
                .compact();
    }

    /** 读取 token 里的令牌版本 claim（旧 token 无此 claim 时按 0 处理）。 */
    public int getTokenVersionFromToken(String token) {
        Claims claims = Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
        Integer tv = claims.get("tv", Integer.class);
        return tv != null ? tv : 0;
    }

    public Long getUserIdFromToken(String token) {
        Claims claims = Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();

        return Long.parseLong(claims.getSubject());
    }

    public String getUsernameFromToken(String token) {
        Claims claims = Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();

        return claims.get("username", String.class);
    }

    public boolean validateToken(String token) {
        try {
            Jwts.parser()
                    .verifyWith(getSigningKey())
                    .build()
                    .parseSignedClaims(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            return false;
        }
    }
}
