package com.synctask.config;

import org.springframework.context.annotation.Configuration;

/**
 * CORS configuration is now handled centrally in SecurityConfig
 * to avoid duplicate bean definitions and ensure consistent origin whitelisting.
 */
@Configuration
public class CorsConfig {
}
