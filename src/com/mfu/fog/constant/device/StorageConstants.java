package com.mfu.fog.constant.device;

public enum StorageConstants {
    DEFAULT(1e12);

    public final double DRIVE_CAPACITY;

    private StorageConstants(double DRIVE_CAPACITY){
        this.DRIVE_CAPACITY = DRIVE_CAPACITY;
    }
}
