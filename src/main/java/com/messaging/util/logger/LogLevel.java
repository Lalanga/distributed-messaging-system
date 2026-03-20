package com.messaging.util.logger;

import org.apache.logging.log4j.Level;

public enum LogLevel {
    DEBUG(Level.DEBUG),
    INFO(Level.INFO),
    WARN(Level.WARN),
    ERROR(Level.ERROR);

    private final Level level;

    LogLevel(Level level) { this.level = level; }

    public Level getLevel() { return level; }
}
