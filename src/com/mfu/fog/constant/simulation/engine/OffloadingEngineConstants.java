package com.mfu.fog.constant.simulation.engine;

import org.fog.offloading.OffloadingStrategy;
import org.fog.offloading.OffloadingStrategyAllinCloud;
import org.fog.offloading.OffloadingStrategyAllinFog;
import org.fog.offloading.OffloadingStrategySimple;

public enum OffloadingEngineConstants {
    DEFAULT(null),
    SIMPLE(new OffloadingStrategySimple()),
    FOG(new OffloadingStrategyAllinFog()),
    CLOUD(new OffloadingStrategyAllinCloud());

    public final OffloadingStrategy OFFLOADING_STRATEGY;

    private OffloadingEngineConstants(OffloadingStrategy OFFLOADING_STRATEGY) {
        this.OFFLOADING_STRATEGY = OFFLOADING_STRATEGY;
    }
}
