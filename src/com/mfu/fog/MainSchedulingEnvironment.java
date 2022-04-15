package com.mfu.fog;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.fog.entities.Controller;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.jetbrains.annotations.NotNull;
import org.workflowsim.*;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class MainSchedulingEnvironment {

    /**
     * Input Area
     */
    private static Scanner scanner;
    private static final String INPUT_PATH = "config/dax/";
    private static final List<String> algorithmNames;

    static {
        algorithmNames = new ArrayList<>(
                Arrays.asList("MINMIN", "MAXMIN", "FCFS", "ROUNDROBIN", "STATIC"));
    }

    private static double portDelay;
    private static int numberOfCloud;
    private static int numberOfFog;
    private static int numberOfMobile;
    private static final Map<String, List<Long>> hostMips = new HashMap<>();
    private static final Map<String, List<Double>> hostCosts = new HashMap<>();
    private static List<FogDevice> fogDevices;
    private static List<CondorVM> virtualMachines;
    private final static int numOfDept = 1;
    private final static int numOfMobilesPerDept = 1;

    private static final List<Double[]> record = new ArrayList<>();
    private static final Map<String, Map<String, List<Job>>> schedulingResults = new HashMap<>();

    // TODO: calculate transfer cost of each task's dependency for every algorithms
    // to use
    public static void main(String[] args) {
        System.out.println("Starting the Simulation...");
        try {
            scanner = new Scanner(System.in);
            readEnvironmentSetting();
            readPortConstraint();
            scanner.close();
            List<String> dagPaths = getDagPaths();
            for (String dagPath : dagPaths) {
                for (String algorithmName : algorithmNames) {
                    startSimulation(dagPath, algorithmName);
                }
            }
            SchedulingResult schedulingResult = new SchedulingResult(schedulingResults, virtualMachines);
            schedulingResult.printSchedulingResults();
            schedulingResult.exportResult();

        } catch (Exception e) {
            System.out.println("Unwanted errors happen");
            e.printStackTrace();
        }
    }

    private static void readPortConstraint() throws NumberFormatException, ArithmeticException {
        System.out.println();
        System.out.println("Additional Constraint");
        System.out.print("I/O Port Communication Delay: ");
        portDelay = Double.parseDouble(scanner.next());
        if (portDelay < 0) {
            throw new ArithmeticException("delay cannot be lower than zero");
        }
    }

    private static void readEnvironmentSetting() throws NumberFormatException, ArithmeticException {
        System.out.println();
        System.out.println("Environment Settings");

        System.out.print("Number of Cloud: ");
        numberOfCloud = getNumberOfHost();
        readHostSpec("cloud", numberOfCloud);

        System.out.print("Number of Fog: ");
        numberOfFog = getNumberOfHost();
        readHostSpec("fog", numberOfFog);

        System.out.print("Number of Mobile: ");
        numberOfMobile = getNumberOfHost();
        readHostSpec("mobile", numberOfMobile);
    }

    private static int getNumberOfHost() throws NumberFormatException, ArithmeticException {
        int numberOfHost = Integer.parseInt(scanner.next());
        if (numberOfHost < 0) {
            throw new ArithmeticException("host cannot be less than zero");
        }
        return numberOfHost;
    }

    private static void readHostSpec(String hostName, int numberOfHost)
            throws NumberFormatException, ArithmeticException {
        List<Long> mips = new ArrayList<>();
        List<Double> costs = new ArrayList<>();
        long inputMips;
        double inputCost;
        for (int i = 0; i < numberOfHost; i++) {
            System.out.println(hostName + " id: " + i);
            System.out.print("MIPS: ");
            inputMips = Long.parseLong(scanner.next());
            System.out.print("Processing Cost: ");
            inputCost = Double.parseDouble(scanner.next());
            if (inputMips < 0 || inputCost < 0)
                throw new ArithmeticException("input cannot be less than zero");
            mips.add(inputMips);
            costs.add(inputCost);
            System.out.println();
        }
        hostMips.put(hostName, mips);
        hostCosts.put(hostName, costs);
    }

    private static @NotNull
    List<String> getDagPaths() throws FileNotFoundException {
        List<String> dagPaths = new ArrayList<>();
        File dagFolder = new File(MainSchedulingEnvironment.INPUT_PATH);
        File[] dagFiles = dagFolder.listFiles();
        assert dagFiles != null;
        for (File dagFile : dagFiles) {
            if (dagFile.getPath().contains(".xml")) {
                dagPaths.add(dagFile.getPath());
            }
        }
        if (dagPaths.isEmpty())
            throw new FileNotFoundException("no input files");
        return dagPaths;
    }

    private static String getDagName(String dagPath) {
        String[] paths = dagPath.split("\\\\");
        return paths[paths.length - 1].split(".xml")[0];
    }

    // TODO: extract methods
    private static void startSimulation(String dagPath, String algorithmName) throws Exception {
        new PortConstraint(portDelay);
        int numUser = 1;
        Calendar calendar = Calendar.getInstance();
        CloudSim.init(numUser, calendar, false);

        fogDevices = new ArrayList<>();
        String appId = "workflow";
        createFogDevices();

        List<? extends Host> hosts = new ArrayList<>();
        int numberOfVirtualMachine = 0;
        for (FogDevice device : fogDevices) {
            numberOfVirtualMachine += device.getHostList().size();
            hosts.addAll(device.getHostList());
        }

        File dagFile = new File(dagPath);
        if (!dagFile.exists()) {
            throw new IOException("file does not exist");
        }

        String objective = "Time";
        Parameters.SchedulingAlgorithm schedulingMethod = Parameters.SchedulingAlgorithm.valueOf(algorithmName);
        Parameters.Optimization optimizationObjective = Parameters.Optimization.valueOf(objective);
        Parameters.PlanningAlgorithm planningMethod = Parameters.PlanningAlgorithm.HEFT;
        ReplicaCatalog.FileSystem fileSystem = ReplicaCatalog.FileSystem.SHARED;
        OverheadParameters overheadParameters = new OverheadParameters(0, null, null, null, null, 0);

		/*
		  No Clustering
		 */
        ClusteringParameters.ClusteringMethod clusteringMethod = ClusteringParameters.ClusteringMethod.NONE;
        int clusteringNumber = 0;
        int clusteringSize = 0;
        ClusteringParameters clusteringParameters = new ClusteringParameters(clusteringNumber, clusteringSize,
                clusteringMethod, null);

		/*
		  Initialize static parameters
		 */
        Parameters.init(numberOfVirtualMachine, dagPath, null, null, overheadParameters, clusteringParameters,
                schedulingMethod, optimizationObjective, planningMethod, null, 0);
        ReplicaCatalog.init(fileSystem);

        /*
         * Create a WorkflowPlanner with one scheduler.
         */
        String workflowPlannerName = "planner";
        int numberOfScheduler = 1;
        WorkflowPlanner workflowPlanner = new WorkflowPlanner(workflowPlannerName, numberOfScheduler);

        /*
         * Create a WorkflowEngine.
         */
        WorkflowEngine workflowEngine = workflowPlanner.getWorkflowEngine();
        workflowEngine.getoffloadingEngine().setOffloadingStrategy(null);
        int schedulerId = 0;
        virtualMachines = createVM(workflowEngine.getSchedulerId(schedulerId), numberOfVirtualMachine, hosts);
        workflowEngine.submitVmList(virtualMachines, schedulerId);

        /*
         * Binds the data centers with the scheduler.
         */
        Controller controller = new Controller("master-controller", fogDevices, workflowEngine);
        for (FogDevice fogdevice : controller.getFogDevices()) {
            workflowEngine.bindSchedulerDatacenter(fogdevice.getId(), schedulerId);
        }

        String dagName = getDagName(dagPath);
        if (algorithmName.equals("STATIC"))
            algorithmName = planningMethod.toString();
        CloudSim.startSimulation();
        List<Job> scheduledJobs = workflowEngine.getJobsReceivedList();
        List<Integer> jobArrivalOrders = FogBroker.jobArrivalOrders;
        CloudSim.stopSimulation();
        List<Job> orderedJobs = getOrderedJobs(scheduledJobs, jobArrivalOrders);
        System.out.println("algo: " + algorithmName);
        System.out.println("order: " + jobArrivalOrders);
        setTaskFinishTime(orderedJobs);
        setPortDelayToJobs(orderedJobs);
        setSchedulingResult(dagName, algorithmName, orderedJobs);
        Log.enable();
    }


    private static void createFogDevices() throws Exception {
        String hostName = "cloud";
        List<Long> mips = hostMips.get(hostName);
        List<Double> costs = hostCosts.get(hostName);
        double mipsCostRate = 0.96;
        double memoryCostRate = 0.05;
        double storageCostRate = 0.1;
        double bandwidthCostRate = 0.2;
        double busyPower = 1_648;
        double idlePower = 1_332;
        long uplinkBandwidth = 100;
        long downlinkBandwidth = 10_000;
        int ram = 40_000;
        int level = 0;
        int parentId = -1;

        FogDevice cloud = createFogDevice(hostName, numberOfCloud, mips, costs, ram, uplinkBandwidth, downlinkBandwidth,
                level, mipsCostRate, busyPower, idlePower, memoryCostRate, storageCostRate, bandwidthCostRate);
        cloud.setParentId(parentId);

        fogDevices.add(cloud);

        for (int i = 0; i < numOfDept; i++) {
            addFogNode(i + "", fogDevices.get(0).getId());
        }

    }

    private static FogDevice createFogDevice(String hostName, int numberOfHost, List<Long> hostMips,
                                             List<Double> hostCostsPerMips, int ram, long uplinkBandwidth, long downlinkBandwidth, int level,
                                             double mipsCostRate, double busyPower, double idlePower, double memoryCostRate, double storageCostRate,
                                             double bandwidthCostRate) throws Exception {

        List<Host> hosts = new ArrayList<>();
        LinkedList<Storage> storages = new LinkedList<>();
        double timeZone = 7.0;
        long storageSize = 1_000_000;
        int bandwidthSize = 10_000;
        int peId = 0;

        for (int i = 0; i < numberOfHost; i++) {
            List<Pe> processingElements = new ArrayList<>();
            int hostId = FogUtils.generateEntityId();
            long mips = hostMips.get(i);
            double costPerMips = hostCostsPerMips.get(i);

            PeProvisionerSimple processingProvisioner = new PeProvisionerSimple(mips);
            Pe processingElement = new Pe(peId, processingProvisioner);
            processingElements.add(processingElement);

            RamProvisionerSimple ramProvisioner = new RamProvisionerSimple(ram);
            BwProvisionerSimple bandwidthProvisioner = new BwProvisionerSimple(bandwidthSize);
            VmSchedulerSpaceShared virtualMachineSchedulerPolicy = new VmSchedulerSpaceShared(processingElements);
            FogLinearPowerModel fogLinearPowerModel = new FogLinearPowerModel(busyPower, idlePower);

            PowerHost host = new PowerHost(hostId, costPerMips, ramProvisioner, bandwidthProvisioner, storageSize,
                    processingElements, virtualMachineSchedulerPolicy, fogLinearPowerModel);

            hosts.add(host);
        }

        String systemArchitecture = "x86";
        String operatingSystem = "Linux";
        String virtualMachineMonitor = "Xen";
        double processingCost = 3.0;
        double driveCapacity = 1e12;
        double schedulingInterval = 10;
        double uplinkLatency = 0;

        FogDeviceCharacteristics fogDeviceCharacteristics = new FogDeviceCharacteristics(systemArchitecture,
                operatingSystem, virtualMachineMonitor, hosts, timeZone, processingCost, memoryCostRate,
                storageCostRate, bandwidthCostRate);

        HarddriveStorage harddriveStorage = new HarddriveStorage(hostName, driveCapacity);
        storages.add(harddriveStorage);

        VmAllocationPolicySimple virtualMachineAllocationPolicy = new VmAllocationPolicySimple(hosts);
        FogDevice fogdevice = new FogDevice(hostName, fogDeviceCharacteristics, virtualMachineAllocationPolicy,
                storages, schedulingInterval, uplinkBandwidth, downlinkBandwidth, uplinkLatency, mipsCostRate);
        fogdevice.setLevel(level);

        return fogdevice;
    }

    private static void addFogNode(String fogId, int parentId) throws Exception {
        String hostName = "fog";
        List<Long> mips = hostMips.get(hostName);
        List<Double> costs = hostCosts.get(hostName);
        double mipsCostRate = 0.48;
        double memoryCostRate = 0.05;
        double storageCostRate = 0.1;
        double bandwidthCostRate = 0.1;
        double uplinkLatency = 4;
        double busyPower = 700;
        double idlePower = 30;
        long uplinkBandwidth = 10_000;
        long downlinkBandwidth = 10_000;
        int ram = 4_000;
        int level = 1;

        FogDevice fogNode = createFogDevice(hostName + "-" + fogId, numberOfFog, mips, costs, ram, uplinkBandwidth,
                downlinkBandwidth, level, mipsCostRate, busyPower, idlePower, memoryCostRate, storageCostRate,
                bandwidthCostRate);
        fogDevices.add(fogNode);
        fogNode.setParentId(parentId);
        fogNode.setUplinkLatency(uplinkLatency);

        if (numberOfMobile > 0) {
            for (int i = 0; i < numOfMobilesPerDept; i++) {
                String mobileId = fogId + "-" + i;
                double mobileUplinkLatency = 2;
                FogDevice mobile = addMobile(mobileId, fogNode.getId());
                mobile.setUplinkLatency(mobileUplinkLatency);
                fogDevices.add(mobile);
            }
        }

    }

    private static FogDevice addMobile(String id, int parentId) throws Exception {
        String hostName = "mobile";
        List<Long> mips = hostMips.get(hostName);
        List<Double> costs = hostCosts.get(hostName);
        double mipsCostRate = 0;
        double memoryCostRate = 0.05;
        double storageCostRate = 0.1;
        double bandwidthCostRate = 0.3;
        double busyPower = 700;
        double idlePower = 30;
        long uplinkBandwidth = 2_048;
        long downlinkBandwidth = 4_096;
        int ram = 1_000;
        int level = 2;

        FogDevice mobile = createFogDevice(hostName + "-" + id, numberOfMobile, mips, costs, ram, uplinkBandwidth,
                downlinkBandwidth, level, mipsCostRate, busyPower, idlePower, memoryCostRate, storageCostRate,
                bandwidthCostRate);
        mobile.setParentId(parentId);
        return mobile;
    }

    private static List<CondorVM> createVM(int userId, int numberOfVirtualMachine, List<? extends Host> hosts) {
        LinkedList<CondorVM> virtualMachines = new LinkedList<>();
        CondorVM[] vm = new CondorVM[numberOfVirtualMachine];
        long imageSize = 10_000;
        int ram = 512;
        long bandwidth = 1_000;
        int numberOfCpu = 1;
        String virtualMachineMonitor = "Xen";

        for (int i = 0; i < numberOfVirtualMachine; i++) {
            int mips = hosts.get(i).getTotalMips();
            CloudletSchedulerSpaceShared cloudletSchedulerPolicy = new CloudletSchedulerSpaceShared();
            vm[i] = new CondorVM(i, userId, mips, numberOfCpu, ram, bandwidth, imageSize, virtualMachineMonitor,
                    cloudletSchedulerPolicy);
            virtualMachines.add(vm[i]);
        }
        return virtualMachines;
    }

    private static void setSchedulingResult(String dagName, String algorithmName, List<Job> jobs) {
        if (schedulingResults.containsKey(dagName)) {
            schedulingResults.get(dagName).put(algorithmName, jobs);
        } else {
            Map<String, List<Job>> newSchedulingResult = new HashMap<>();
            newSchedulingResult.put(algorithmName, jobs);
            schedulingResults.put(dagName, newSchedulingResult);
        }
    }

    private static @NotNull List<Job> getOrderedJobs(List<Job> scheduledJobs, List<Integer> jobArrivalOrders) {
        List<Job> orderedJobs = new ArrayList<>();
        for (int jobArrivalId : jobArrivalOrders) {
            for (Job job : scheduledJobs) {
                int jobId = job.getCloudletId();
                if (jobArrivalId == jobId) {
                    orderedJobs.add(job);
                    break;
                }
            }
        }
        return orderedJobs;
    }

    private static void setTaskFinishTime(@NotNull List<Job> scheduledJobs) {
        for (Job job : scheduledJobs) {
            job.setTaskFinishTime(job.getFinishTime());
        }
    }

    private static void setPortDelayToJobs(@NotNull List<Job> orderedJobs) {
        if (!PortConstraint.hasPortDelay()) return;
        Set<Double> eventTimes = new HashSet<>();
        for (int i = 0; i < orderedJobs.size(); i++) {
            Job job = orderedJobs.get(i);
            double inputDelay = PortConstraint.getPortDelay();
            double startTime = job.getExecStartTime();
            double finishTime = job.getTaskFinishTime();

            // check the latest finish time by the same vm
            double latestFinishTime = Double.MIN_VALUE;
            for (int j = 0; j < i; j++) {
                Job previousJob = orderedJobs.get(j);
                if (previousJob.getVmId() == job.getVmId()) {
                    latestFinishTime = previousJob.getTaskFinishTime();
                }
            }

            while (startTime <= latestFinishTime) {
                startTime += inputDelay;
                finishTime += inputDelay;
            }

            // check parent time
            for (Task parent : job.getParentList()) {
                double parentStartTime = parent.getExecStartTime();
                double parentFinishTime = parent.getTaskFinishTime();
                while (startTime <= parentStartTime || startTime <= parentFinishTime) {
                    startTime += inputDelay;
                    finishTime += inputDelay;
                }
            }

            // check all event time
            while (eventTimes.contains(startTime)) {
                startTime += inputDelay;
                finishTime += inputDelay;
            }

            job.setExecStartTime(startTime);
            job.setTaskFinishTime(finishTime);
            eventTimes.add(startTime);
            eventTimes.add(finishTime);
        }
    }

    private static int getDAGSize(String path) throws IOException {
        StringBuilder fileSize = new StringBuilder();
        if (!path.isBlank()) {
            for (char character : path.toCharArray()) {
                if (Character.isDigit(character))
                    fileSize.append(character);
            }
            return Integer.parseInt(fileSize.toString());
        } else {
            throw new IOException("invalid file name: no dag size within file name.");
        }
    }

}
