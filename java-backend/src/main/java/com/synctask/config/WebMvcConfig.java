package com.synctask.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.CacheControl;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Duration;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    /**
     * 是否下发构建产物（{@code static/js/} 下的压缩版）而不是源文件。
     *
     * <p>默认 {@code false} = <b>与改造前逐字节相同</b>：直接 serve 根目录下的
     * {@code admin-dashboard.js} 等源文件。开发时改一行就能刷新看到效果，
     * 不需要先跑构建。
     *
     * <p>生产打开它：{@code npm run build} 之后 549 KB → 284 KB（-48%），
     * 且带 sourcemap，线上排障仍能还原到源码行。
     */
    @Value("${app.frontend.dist:false}")
    private boolean serveDist;

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // 第三方库：内容随版本固定、文件名不变，但它们本来就不会被改动，
        // 给一个较长的缓存期。此前整站 cachePeriod=0，等于每次进页面重新拉
        // 264 KB 的 SockJS + STOMP + Chart.js。
        registry.addResourceHandler("/static/vendor/**")
                .addResourceLocations("file:../static/vendor/")
                .setCacheControl(CacheControl.maxAge(Duration.ofDays(7)).cachePublic());

        if (serveDist) {
            // 产物优先、源文件兜底：Spring 按 locations 顺序查找，
            // static/js/ 里有同名文件就用产物，没有（比如新加的文件还没构建）就回落源文件。
            registry.addResourceHandler("/**")
                    .addResourceLocations("file:../static/js/", "file:../")
                    .setCacheControl(CacheControl.maxAge(Duration.ofMinutes(10)));
        } else {
            // 缺省：与改造前完全一致
            registry.addResourceHandler("/**")
                    .addResourceLocations("file:../")
                    .setCachePeriod(0);
        }
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/").setViewName("forward:/login.html");
    }
}
