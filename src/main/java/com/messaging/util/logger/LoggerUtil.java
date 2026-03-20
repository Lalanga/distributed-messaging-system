package com.messaging.util.logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public final class LoggerUtil {
    private LoggerUtil() {}

    public static Logger getLogger(Class<?> clazz) {
        return LogManager.getLogger(clazz);
    }

    public static void setLogLevel(LogLevel level) {
        Configurator.setRootLevel(level.getLevel());
    }
}
