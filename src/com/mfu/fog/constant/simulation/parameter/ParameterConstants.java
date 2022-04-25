package com.mfu.fog.constant.simulation.parameter;

import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;

public enum ParameterConstants {
    DEFAULT();
    public final Parameters.Optimization OPTIMIZATION_OBJECTIVE = Parameters.Optimization.Time;
    public final OverheadParameters OVERHEAD_PARAMETERS = new OverheadParameters(0, null, null, null, null, 0);
    public final ClusteringParameters CLUSTERING_PARAMETERS = new ClusteringParameters(0, 0,
            ClusteringParameters.ClusteringMethod.NONE, null);
    public final String RUNTIME = null;
    public final String DATA_SIZE = null;
    public final String REDUCER_MODE = null;
    public final long DEADLINE = 0;
}
