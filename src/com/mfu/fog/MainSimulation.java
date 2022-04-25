package com.mfu.fog;

import com.mfu.fog.constant.simulation.CloudSimConstants;
import com.mfu.fog.constant.simulation.engine.ControllerConstants;
import com.mfu.fog.constant.simulation.engine.OffloadingEngineConstants;
import com.mfu.fog.constant.simulation.engine.WorkflowEngineConstants;
import com.mfu.fog.constant.simulation.engine.WorkflowPlannerConstants;
import com.mfu.fog.constant.simulation.file.ReplicaCatalogConstants;
import com.mfu.fog.constant.simulation.parameter.ParameterConstants;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.entities.Controller;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

public class MainSimulation {
    private static final CloudSimConstants cloudSimConstants = CloudSimConstants.DEFAULT;
    private static final WorkflowEngineConstants workflowEngineConstants = WorkflowEngineConstants.DEFAULT;
    private static final WorkflowPlannerConstants workflowPlannerConstants = WorkflowPlannerConstants.DEFAULT;
    private static final OffloadingEngineConstants offloadingEngineConstants = OffloadingEngineConstants.DEFAULT;
    private static final ControllerConstants controllerConstants = ControllerConstants.DEFAULT;
    private static final ReplicaCatalogConstants replicaCatalogConstants = ReplicaCatalogConstants.DEFAULT;
    private static final ParameterConstants parameterConstants = ParameterConstants.DEFAULT;
    private static final String inputPath = "config/dax/";
    private static final SchedulingResult schedulingResult = new SchedulingResult();

    public static void main(String[] args) {
        System.out.println("Starting the Simulation...");
        try {
            UserInput userInput = new UserInput();
            userInput.readSimulationInput(inputPath);
            HostEnvironment hostEnvironment = new HostEnvironment(userInput);
            for (String dagPath : userInput.getDagPaths()) {
                for (String algorithmName : userInput.getAlgorithmNames()) {
                    startSimulation(userInput, hostEnvironment, dagPath, algorithmName);
                }
            }
            SimulationOutputPrinter simulationOutputPrinter = new SimulationOutputPrinter(userInput.getPortDelay(),
                    hostEnvironment.getVirtualMachines(),
                    schedulingResult.getSchedulingResultsByAlgorithmsEachDag());
            simulationOutputPrinter.printSchedulingResults();
            simulationOutputPrinter.exportSchedulingResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startSimulation(UserInput userInput, HostEnvironment hostEnvironment, String dagPath, String algorithmName) throws Exception {
        CloudSim.init(cloudSimConstants.NUMBER_OF_USER, cloudSimConstants.CALENDAR_INSTANCE, cloudSimConstants.TRACE_FLAG);
        hostEnvironment.createComputingDevices(workflowEngineConstants.WORKFLOW_ENGINE_ID);
        Parameters.init(hostEnvironment.getNumberOfVirtualMachine(), dagPath,
                parameterConstants.RUNTIME,
                parameterConstants.DATA_SIZE,
                parameterConstants.OVERHEAD_PARAMETERS,
                parameterConstants.CLUSTERING_PARAMETERS,
                userInput.getSchedulingAlgorithm(algorithmName),
                parameterConstants.OPTIMIZATION_OBJECTIVE,
                userInput.getPlanningAlgorithm(algorithmName),
                parameterConstants.REDUCER_MODE,
                parameterConstants.DEADLINE);
        ReplicaCatalog.init(replicaCatalogConstants.FILE_SYSTEM);

        WorkflowPlanner workflowPlanner = new WorkflowPlanner(workflowPlannerConstants.WORKFLOW_PLANNER_NAME,
                workflowPlannerConstants.NUMBER_OF_SCHEDULER);
        WorkflowEngine workflowEngine = workflowPlanner.getWorkflowEngine();
        workflowEngine.getoffloadingEngine().setOffloadingStrategy(offloadingEngineConstants.OFFLOADING_STRATEGY);
        workflowEngine.submitVmList(hostEnvironment.getVirtualMachines(), workflowEngineConstants.SCHEDULER_ID);

        Controller controller = new Controller(controllerConstants.NAME, hostEnvironment.getFogDevices(), workflowEngine);
        for (FogDevice fogdevice : controller.getFogDevices()) {
            workflowEngine.bindSchedulerDatacenter(fogdevice.getId(), workflowEngineConstants.SCHEDULER_ID);
        }

        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        schedulingResult.addResult(userInput.getDagName(dagPath),
                algorithmName,
                userInput.getPortDelay(),
                workflowEngine.getJobsReceivedList(),
                FogBroker.getJobSubmissionOrders());
        Log.enable();
    }
}
