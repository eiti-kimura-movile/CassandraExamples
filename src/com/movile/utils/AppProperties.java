package com.movile.utils;

import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Responsible for maintaining the system properties.
 * @author Daniel A. Panhan (daniel.panhan@compera.com.br)
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 */
public final class AppProperties {

    private static Logger log = Logger.getLogger("system");
    
    // using the new property component
    private static SmartProperties defaultInstance = new SmartProperties(new Properties(), log);

    public static SmartProperties getDefaultInstance() {
        return defaultInstance;
    }
}
