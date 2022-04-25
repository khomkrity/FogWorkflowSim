package com.mfu.fog;

import com.mfu.fog.constant.device.CloudConstants;
import com.mfu.fog.constant.device.EndDeviceConstants;
import com.mfu.fog.constant.device.FogDeviceConstants;
import org.workflowsim.utils.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

class UserInput {

    private int numberOfCloud;
    private int numberOfFog;
    private int numberOfMobile;
    private double portDelay;
    private final List<String> algorithmNames;
    private final Map<String, List<Long>> hostMips;
    private final Map<String, List<Double>> hostCosts;
    private List<String> dagPaths;

    UserInput() {
        hostMips = new HashMap<>();
        hostCosts = new HashMap<>();
        algorithmNames = new ArrayList<>(List.of("MINMIN", "MAXMIN", "FCFS", "ROUNDROBIN", "HEFT"));
    }

    public void readSimulationInput(String inputPath) throws FileNotFoundException {
        Scanner scanner = new Scanner(System.in);
        readEnvironmentSetting(scanner);
        readPortConstraint(scanner);
        readDagPaths(inputPath);
        scanner.close();
    }

    private void readEnvironmentSetting(Scanner scanner) throws NumberFormatException, ArithmeticException {
        System.out.println();
        System.out.println("Environment Settings");

        System.out.print("Number of Cloud: ");
        this.numberOfCloud = readNumberOfHost(scanner);
        readHostSpec(scanner, CloudConstants.DEFAULT.HOST_NAME, getNumberOfCloud());

        System.out.print("Number of Fog: ");
        this.numberOfFog = readNumberOfHost(scanner);
        readHostSpec(scanner, FogDeviceConstants.DEFAULT.HOST_NAME, getNumberOfFog());

        System.out.print("Number of Mobile: ");
        this.numberOfMobile = readNumberOfHost(scanner);
        readHostSpec(scanner, EndDeviceConstants.DEFAULT.HOST_NAME, getNumberOfMobile());
    }

    private void readPortConstraint(Scanner scanner) throws NumberFormatException, ArithmeticException {
        System.out.println();
        System.out.println("Additional Constraint");
        System.out.print("I/O Port Communication Delay: ");
        portDelay = Double.parseDouble(scanner.next());
        if (getPortDelay() < 0) {
            throw new ArithmeticException("delay cannot be lower than zero");
        }
    }

    private int readNumberOfHost(Scanner scanner) throws NumberFormatException, ArithmeticException {
        int numberOfHost = Integer.parseInt(scanner.next());
        if (numberOfHost < 0) {
            throw new ArithmeticException("host cannot be less than zero");
        }
        return numberOfHost;
    }

    private void readHostSpec(Scanner scanner, String hostName, int numberOfHost)
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
        getHostMips().put(hostName, mips);
        getHostCosts().put(hostName, costs);
    }

    private void readDagPaths(String inputPath) throws FileNotFoundException {
        List<String> dagPaths = new ArrayList<>();
        File dagFolder = new File(inputPath);
        File[] dagFiles = dagFolder.listFiles();
        assert dagFiles != null;
        for (File dagFile : dagFiles) {
            if (dagFile.getPath().contains(".xml")) {
                dagPaths.add(dagFile.getPath());
            }
        }
        if (dagPaths.isEmpty())
            throw new FileNotFoundException("no input files");
        this.dagPaths = dagPaths;
    }

    public List<String> getDagPaths() {
        return dagPaths;
    }

    public String getDagName(String dagPath) {
        String[] paths = dagPath.split("\\\\");
        return paths[paths.length - 1].split(".xml")[0];
    }

    public Parameters.SchedulingAlgorithm getSchedulingAlgorithm(String algorithmName) {
        for (Parameters.SchedulingAlgorithm schedulingAlgorithm : Parameters.SchedulingAlgorithm.values()) {
            if (schedulingAlgorithm.name().equals(algorithmName)) {
                return schedulingAlgorithm;
            }
        }
        return Parameters.SchedulingAlgorithm.STATIC;
    }

    public Parameters.PlanningAlgorithm getPlanningAlgorithm(String algorithmName) {
        for (Parameters.PlanningAlgorithm planningAlgorithm : Parameters.PlanningAlgorithm.values()) {
            if (planningAlgorithm.name().equals(algorithmName)) {
                return planningAlgorithm;
            }
        }
        return Parameters.PlanningAlgorithm.INVALID;
    }

    public boolean hasPortDelay() {
        return portDelay != 0;
    }

    public int getNumberOfCloud() {
        return numberOfCloud;
    }

    public int getNumberOfFog() {
        return numberOfFog;
    }

    public int getNumberOfMobile() {
        return numberOfMobile;
    }

    public Map<String, List<Long>> getHostMips() {
        return hostMips;
    }

    public Map<String, List<Double>> getHostCosts() {
        return hostCosts;
    }

    public double getPortDelay() {
        return portDelay;
    }

    public List<String> getAlgorithmNames() {
        return algorithmNames;
    }
}
