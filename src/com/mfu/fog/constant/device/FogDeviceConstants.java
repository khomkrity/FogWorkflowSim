package com.mfu.fog.constant.device;

public enum FogDeviceConstants {
    DEFAULT();
    public final String HOST_NAME = "fog";
    public final double MIPS_COST_RATE = 0.48;
    public final double MEMORY_COST_RATE = 0.05;
    public final double STORAGE_COST_RATE = 0.1;
    public final double BANDWIDTH_COST_RATE = 0.1;
    public final double UPLINK_LATENCY = 4;
    public final double BUSY_POWER = 700;
    public final double IDLE_POWER = 30;
    public final long UPLINK_BANDWIDTH = 10_000;
    public final long DOWNLINK_BANDWIDTH = 10_000;
    public final int RAM = 4_000;
    public final int LEVEL = 1;
    public final int NUMBER_OF_MOBILE_PER_FOG = 1;
}
