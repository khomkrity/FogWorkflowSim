package com.mfu.fog.constant.device;

public enum EndDeviceConstants {
    DEFAULT();
    public final String HOST_NAME = "mobile";
    public final double MIPS_COST_RATE = 0;
    public final double MEMORY_COST_RATE = 0.05;
    public final double STORAGE_COST_RATE = 0.1;
    public final double BANDWIDTH_COST_RATE = 0.1;
    public final double UPLINK_LATENCY = 2;
    public final double BUSY_POWER = 700;
    public final double IDLE_POWER = 30;
    public final long UPLINK_BANDWIDTH = 2_048;
    public final long DOWNLINK_BANDWIDTH = 4_096;
    public final int RAM = 1_000;
    public final int LEVEL = 2;
}
