package com.mfu.fog.constant.device;

public enum CloudConstants {
    DEFAULT();
    public final String HOST_NAME = "cloud";
    public final double MIPS_COST_RATE = 0.96;
    public final double MEMORY_COST_RATE = 0.05;
    public final double STORAGE_COST_RATE = 0.1;
    public final double BANDWIDTH_COST_RATE = 0.2;
    public final double BUSY_POWER = 1_648;
    public final double IDLE_POWER = 1_332;
    public final long UPLINK_BANDWIDTH = 100;
    public final long DOWNLINK_BANDWIDTH = 10_000;
    public final int RAM = 40_000;
    public final int LEVEL = 0;
    public final int NUMBER_OF_FOG_PER_CLOUD = 1;
    public final int PARENT_ID = -1;
}
