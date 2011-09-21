package com.movile.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @author Daniel A. Panhan (daniel.panhan@movile.com)
 * @author Igor Hjelmstrom Vinhas Ribeiro (igor.ribeiro@movile.com)
 */
public class SmartProperties {

    private Properties properties;
    private Logger logger;

    public SmartProperties(Properties properties, Logger logger) {
        this.properties = properties;
        this.logger = logger;
    }

    public String getString(String property, String defaultValue) {
        String value = properties.getProperty(property);
        if (value == null) {
            logger.info("Property " + property + " not set. Assuming the default value: " + defaultValue);
            return defaultValue;
        } else {
            logger.trace("Property " + property + " - returning configured value: " + value);
            return value;
        }
    }

    public Integer getInt(String property, Integer defaultValue) {
        String value = properties.getProperty(property);
        if (value == null) {
            logger.info("Property " + property + " not set. Assuming the default value: " + defaultValue);
            return defaultValue;
        }
        try {
            Integer result = new Integer(value);
            logger.trace("Property " + property + " - returning configured value: " + result);
            return result;
        } catch (Exception e) {
            logger.warn("Property " + property + " set to invalid value (" + value + "). Ignoring and assuming the default value: " + defaultValue);
            return defaultValue;
        }
    }

    public Integer getInt(String property, Integer defaultValue, Integer minValue, Integer maxValue) {
        Integer value = getInt(property, defaultValue);
        if (value < minValue || value > maxValue) {
            logger.warn("Property " + property + " set to out of range [" + minValue + "," + maxValue + "] value (" + value
                    + "). Ignoring and assuming the default value: " + defaultValue);
            return defaultValue;
        }
        return value;
    }

    public Long getLong(String property, Long defaultValue) {
        String value = properties.getProperty(property);
        if (value == null) {
            logger.info("Property " + property + " not set. Assuming the default value: " + defaultValue);
            return defaultValue;
        }
        try {
            Long result = new Long(value);
            logger.trace("Property " + property + " - returning configured value: " + result);
            return result;
        } catch (Exception e) {
            logger.warn("Property " + property + " set to invalid value (" + value + "). Ignoring and assuming the default value: " + defaultValue);
            return defaultValue;
        }
    }

    public Long getLong(String property) {
        Long result = getLong(property, null);
        if (result == null)
            throw new RuntimeException("Missing value for mandatory property " + property);
        else
            return result;
    }

    public Long getLong(String property, Long defaultValue, Long minValue, Long maxValue) {
        Long value = getLong(property, defaultValue);
        if (value < minValue || value > maxValue) {
            logger.warn("Property " + property + " set to out of range [" + minValue + "," + maxValue + "] value. Ignoring and assuming the default value: "
                    + defaultValue);
            return defaultValue;
        }
        return value;
    }

    public Boolean getBoolean(String property, Boolean defaultValue) {
        String value = properties.getProperty(property);
        if (value == null) {
            logger.info("Property " + property + " not set. Assuming the default value: " + defaultValue);
            return defaultValue;
        }
        try {
            Boolean result = Boolean.valueOf(value);
            logger.trace("Property " + property + " - returning configured value: " + result);
            return result;
        } catch (Exception e) {
            logger.warn("Property " + property + " set to invalid value (" + value + "). Ignoring and assuming the default value: " + defaultValue);
            return defaultValue;
        }
    }

    public Double getDouble(String property, Double defaultValue) {
        String value = properties.getProperty(property);
        if (value == null) {
            logger.info("Property " + property + " not set. Assuming the default value: " + defaultValue);
            return defaultValue;
        }
        try {
            Double result = new Double(value);
            logger.trace("Property " + property + " - returning configured value: " + result);
            return result;
        } catch (Exception e) {
            logger.warn("Property " + property + " set to invalid value (" + value + "). Ignoring and assuming the default value: " + defaultValue);
            return defaultValue;
        }
    }

    public void loadProperties(String filename) throws IOException {
        File file = new File(filename);
        FileInputStream is = new FileInputStream(file);
        properties.load(is);
        is.close();
    }

    public void loadProperties(InputStream inputStream) throws IOException {
        properties.clear();

        properties.load(inputStream);
        inputStream.close();
    }

    public void loadProperties(Properties p) throws IOException {
        if (p == null) {
            throw new IllegalArgumentException();
        }

        properties.putAll(p);
    }

    public String getString(String s) {
        String result = getString(s, null);

        if (result == null) {
            logger.info("Missing value for property " + s);
        }

        return result;
    }

    public int getInt(String s) {
        Integer result = getInt(s, null);

        if (result == null) {
            logger.info("Missing value for property " + s);
        }

        return result;
    }

    public boolean getBoolean(String s) {
        Boolean result = getBoolean(s, null);

        if (result == null) {
            logger.info("Missing value for property " + s);
        }

        return result;
    }

    public double getDouble(String s) {
        Double result = getDouble(s, null);

        if (result == null) {
            logger.info("Missing value for property " + s);
        }

        return result;
    }

    public Properties getUnderlyingProperties() {
        return properties;
    }
}
