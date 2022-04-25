package com.mfu.fog.constant.simulation.file;

import org.workflowsim.utils.ReplicaCatalog;

public enum ReplicaCatalogConstants {
    DEFAULT();
    public final ReplicaCatalog.FileSystem FILE_SYSTEM = ReplicaCatalog.FileSystem.SHARED;
}
