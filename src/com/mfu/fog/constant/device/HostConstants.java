package com.mfu.fog.constant.device;

public enum HostConstants {
    DEFAULT(7.0, 1_000_000, 10_000);

    public final double TIME_ZONE;
    public final long STORAGE_SIZE;
    public final int BANDWIDTH_SIZE;
    public final String SYSTEM_ARCHITECTURE = "x86";
    public final String OPERATING_SYSTEM = "Linux";
    public final String VIRTUAL_MACHINE_MONITOR = "Xen";
    public final double SCHEDULING_INTERVAL = 10;
    public final double UPLINK_LATENCY = 0;
    public final double PROCESSING_COST = 3.0;

    private HostConstants(double TIME_ZONE, long STORAGE_SIZE, int BANDWIDTH_SIZE){
        this.TIME_ZONE = TIME_ZONE;
        this.STORAGE_SIZE = STORAGE_SIZE;
        this.BANDWIDTH_SIZE = BANDWIDTH_SIZE;
    }
}
