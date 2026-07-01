package com.migration.thl.pipeline;

import com.migration.thl.THLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

public class PipelineConfig {

    private static final Logger logger = LoggerFactory.getLogger(PipelineConfig.class);

    private static final String PIPELINE_ENABLED = "pipeline.enabled";
    private static final String PIPELINE_FILTERS = "pipeline.filters";
    private static final String FILTER_PREFIX = "filter.";

    public static Pipeline loadFromProperties(Properties props, PipelineContext context) {
        String enabled = props.getProperty(PIPELINE_ENABLED, "false");
        if (!"true".equalsIgnoreCase(enabled)) {
            logger.info("Pipeline is disabled");
            return null;
        }

        String filtersConfig = props.getProperty(PIPELINE_FILTERS, "");
        if (filtersConfig.trim().isEmpty()) {
            logger.info("Pipeline enabled but no filters configured");
            return null;
        }

        String[] filterNames = filtersConfig.split(",");
        List<PipelineFilter> filters = new ArrayList<>();

        for (String filterName : filterNames) {
            filterName = filterName.trim();
            if (filterName.isEmpty()) continue;

            try {
                PipelineFilter filter = createFilter(filterName, props, context);
                if (filter != null) {
                    filters.add(filter);
                }
            } catch (Exception e) {
                logger.error("Failed to create filter: {}", filterName, e);
            }
        }

        if (filters.isEmpty()) {
            logger.warn("Pipeline enabled but no valid filters loaded");
            return null;
        }

        Collections.sort(filters, Comparator.comparingInt(PipelineFilter::getPriority));

        Pipeline pipeline = new Pipeline(filters, context);
        return pipeline;
    }

    private static PipelineFilter createFilter(String filterName, Properties props, PipelineContext context) throws Exception {
        String classKey = FILTER_PREFIX + filterName + ".class";
        String className = props.getProperty(classKey);

        if (className == null || className.trim().isEmpty()) {
            logger.error("Filter class not configured for filter: {}", filterName);
            return null;
        }

        logger.info("Loading filter: name={}, class={}", filterName, className);

        Class<?> filterClass = Class.forName(className);
        Object instance = filterClass.getDeclaredConstructor().newInstance();

        if (!(instance instanceof PipelineFilter)) {
            logger.error("Class {} does not implement PipelineFilter", className);
            return null;
        }

        PipelineFilter filter = (PipelineFilter) instance;

        injectProperties(filter, filterName, props);

        filter.configure(context);

        logger.info("Filter configured: name={}, class={}, priority={}", filterName, className, filter.getPriority());

        return filter;
    }

    private static void injectProperties(PipelineFilter filter, String filterName, Properties props) throws Exception {
        String prefix = FILTER_PREFIX + filterName + ".";
        Map<String, String> filterProps = new HashMap<>();

        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix) && !key.equals(prefix + "class")) {
                String propName = key.substring(prefix.length());
                filterProps.put(propName, props.getProperty(key));
            }
        }

        Class<?> filterClass = filter.getClass();
        for (Map.Entry<String, String> entry : filterProps.entrySet()) {
            String propName = entry.getKey();
            String propValue = entry.getValue();

            String setterName = "set" + Character.toUpperCase(propName.charAt(0)) + propName.substring(1);

            try {
                Method setter = findSetterMethod(filterClass, setterName);
                if (setter != null) {
                    Object convertedValue = convertValue(propValue, setter.getParameterTypes()[0]);
                    setter.invoke(filter, convertedValue);
                    logger.debug("Injected property: {}.{} = {}", filterName, propName, propValue);
                } else {
                    logger.warn("No setter found for property: {} on filter {}", propName, filterName);
                }
            } catch (Exception e) {
                logger.warn("Failed to inject property: {}.{} = {}: {}", filterName, propName, propValue, e.getMessage());
            }
        }
    }

    private static Method findSetterMethod(Class<?> clazz, String setterName) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(setterName) && method.getParameterCount() == 1) {
                return method;
            }
        }
        return null;
    }

    private static Object convertValue(String value, Class<?> targetType) {
        if (targetType == String.class) {
            return value;
        } else if (targetType == int.class || targetType == Integer.class) {
            return Integer.parseInt(value);
        } else if (targetType == long.class || targetType == Long.class) {
            return Long.parseLong(value);
        } else if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(value);
        } else if (targetType == double.class || targetType == Double.class) {
            return Double.parseDouble(value);
        } else if (targetType == float.class || targetType == Float.class) {
            return Float.parseFloat(value);
        } else {
            return value;
        }
    }
}
