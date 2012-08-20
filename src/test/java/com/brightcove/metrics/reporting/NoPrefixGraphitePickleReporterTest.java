package com.brightcove.metrics.reporting;

/**
 * Tests of the reporter if there is no prefix specified
 */
public class NoPrefixGraphitePickleReporterTest extends GraphitePickleReporterTest {
    @Override
    public String getPrefix() {
        return null;
    }
}
