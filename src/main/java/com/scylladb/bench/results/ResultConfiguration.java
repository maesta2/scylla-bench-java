package com.scylladb.bench.results;

import org.HdrHistogram.Histogram;

/**
 * Global configuration for result tracking / HDR histograms.
 * Single instance shared across all threads.
 */
public class ResultConfiguration {

    public boolean measureLatency = false;
    public long latencyScale = 1; // 1=ns, 1000=us, 1_000_000=ms
    public long minHistogramValue = 0;
    public long maxHistogramValue = Long.MAX_VALUE / 2;
    public int histogramSignificantFigures = 3;
    public int latencyTypeToPrint = LATENCY_TYPE_RAW; // raw or co-fixed
    public int concurrency = 1;
    public String hdrLatencyFile = "";

    public static final int LATENCY_TYPE_RAW = 0;
    public static final int LATENCY_TYPE_CO_FIXED = 1;

    public Histogram newHistogram() {
        return new Histogram(minHistogramValue, maxHistogramValue, histogramSignificantFigures);
    }

    public void setHdrLatencyUnits(String units) {
        switch (units) {
            case "ns":
                latencyScale = 1;
                break;
            case "us":
                latencyScale = 1_000L;
                break;
            case "ms":
                latencyScale = 1_000_000L;
                break;
            default:
                throw new IllegalArgumentException("Unknown hdr-latency-units: " + units);
        }
    }

    public void setHistogramConfiguration(long min, long max, int sigFig) {
        minHistogramValue = min / latencyScale;
        maxHistogramValue = max / latencyScale;
        histogramSignificantFigures = sigFig;
    }

    /** Estimate memory usage in bytes for all HDR histograms across all threads. */
    public int estimateHdrMemory() {
        Histogram h = newHistogram();
        return h.getEstimatedFootprintInBytes() * concurrency * 4;
    }
}
