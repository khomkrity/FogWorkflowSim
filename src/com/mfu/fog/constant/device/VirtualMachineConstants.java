package com.mfu.fog.constant.device;

public enum VirtualMachineConstants {
    DEFAULT();
    public final long IMAGE_SIZE = 10_000;
    public final int RAM = 512;
    public final long BANDWIDTH = 1_000;
    public final int NUMBER_OF_CPU = 1;
    public final String VIRTUAL_MACHINE_MONITOR = "Xen";
}
