package com.mfu.fog;

import com.mfu.fog.constant.device.*;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.workflowsim.CondorVM;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

class HostEnvironment {
    private final UserInput userInput;
    private final List<FogDevice> fogDevices;
    private final List<? extends Host> hosts;
    private static List<CondorVM> virtualMachines;
    private int numberOfVirtualMachine;

    HostEnvironment(UserInput userInput) {
        this.userInput = userInput;
        fogDevices = new ArrayList<>();
        hosts = new ArrayList<>();
        virtualMachines = new ArrayList<>();
    }

    public void createComputingDevices(int userId) throws Exception {
        resetComputingDevices();
        createFogDevices();
        setHosts(fogDevices);
        setNumberOfVirtualMachines(fogDevices);
        createVirtualMachines(userId, numberOfVirtualMachine, hosts);
    }

    private void resetComputingDevices() {
        fogDevices.clear();
        hosts.clear();
        virtualMachines.clear();
        numberOfVirtualMachine = 0;
    }

    private void createFogDevices() throws Exception {
        CloudConstants cloudConstants = CloudConstants.DEFAULT;
        String hostName = cloudConstants.HOST_NAME;
        List<Long> mips = userInput.getHostMips().get(hostName);
        List<Double> costs = userInput.getHostCosts().get(hostName);
        int numberOfCloud = userInput.getNumberOfCloud();
        FogDevice cloud = createFogDevice(hostName,
                numberOfCloud, mips, costs,
                cloudConstants.RAM,
                cloudConstants.UPLINK_BANDWIDTH,
                cloudConstants.DOWNLINK_BANDWIDTH,
                cloudConstants.LEVEL,
                cloudConstants.MIPS_COST_RATE,
                cloudConstants.BUSY_POWER,
                cloudConstants.IDLE_POWER,
                cloudConstants.MEMORY_COST_RATE,
                cloudConstants.STORAGE_COST_RATE,
                cloudConstants.BANDWIDTH_COST_RATE);
        cloud.setParentId(cloudConstants.PARENT_ID);
        getFogDevices().add(cloud);
        for (int i = 0; i < cloudConstants.NUMBER_OF_FOG_PER_CLOUD; i++) {
            createFogNode(i + "", getFogDevices().get(0).getId());
        }
    }

    private FogDevice createFogDevice(String hostName, int numberOfHost, List<Long> hostMips,
                                      List<Double> hostCostsPerMips, int ram, long uplinkBandwidth, long downlinkBandwidth, int level,
                                      double mipsCostRate, double busyPower, double idlePower, double memoryCostRate, double storageCostRate,
                                      double bandwidthCostRate) throws Exception {
        List<Host> hosts = new ArrayList<>();
        LinkedList<Storage> storages = new LinkedList<>();
        HostConstants hostConstants = HostConstants.DEFAULT;
        StorageConstants storageConstants = StorageConstants.DEFAULT;
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
            BwProvisionerSimple bandwidthProvisioner = new BwProvisionerSimple(hostConstants.BANDWIDTH_SIZE);
            VmSchedulerSpaceShared virtualMachineSchedulerPolicy = new VmSchedulerSpaceShared(processingElements);
            FogLinearPowerModel fogLinearPowerModel = new FogLinearPowerModel(busyPower, idlePower);

            PowerHost host = new PowerHost(hostId, costPerMips, ramProvisioner, bandwidthProvisioner, hostConstants.STORAGE_SIZE,
                    processingElements, virtualMachineSchedulerPolicy, fogLinearPowerModel);

            hosts.add(host);
        }

        FogDeviceCharacteristics fogDeviceCharacteristics = new FogDeviceCharacteristics(hostConstants.SYSTEM_ARCHITECTURE,
                hostConstants.OPERATING_SYSTEM,
                hostConstants.VIRTUAL_MACHINE_MONITOR,
                hosts,
                hostConstants.TIME_ZONE,
                hostConstants.PROCESSING_COST,
                memoryCostRate, storageCostRate, bandwidthCostRate);

        HarddriveStorage harddriveStorage = new HarddriveStorage(hostName, storageConstants.DRIVE_CAPACITY);
        storages.add(harddriveStorage);

        VmAllocationPolicySimple virtualMachineAllocationPolicy = new VmAllocationPolicySimple(hosts);
        FogDevice fogdevice = new FogDevice(hostName, fogDeviceCharacteristics,
                virtualMachineAllocationPolicy,
                storages,
                hostConstants.SCHEDULING_INTERVAL,
                uplinkBandwidth,
                downlinkBandwidth,
                hostConstants.UPLINK_LATENCY,
                mipsCostRate);
        fogdevice.setLevel(level);
        return fogdevice;
    }

    private void createFogNode(String fogId, int parentId) throws Exception {
        FogDeviceConstants fogDeviceConstants = FogDeviceConstants.DEFAULT;
        EndDeviceConstants endDeviceConstants = EndDeviceConstants.DEFAULT;
        String hostName = fogDeviceConstants.HOST_NAME;
        List<Long> mips = userInput.getHostMips().get(hostName);
        List<Double> costs = userInput.getHostCosts().get(hostName);
        int numberOfFog = userInput.getNumberOfFog();
        int numberOfMobile = userInput.getNumberOfMobile();
        FogDevice fogNode = createFogDevice(hostName + "-" + fogId,
                numberOfFog, mips, costs,
                fogDeviceConstants.RAM,
                fogDeviceConstants.UPLINK_BANDWIDTH,
                fogDeviceConstants.DOWNLINK_BANDWIDTH,
                fogDeviceConstants.LEVEL,
                fogDeviceConstants.MIPS_COST_RATE,
                fogDeviceConstants.BUSY_POWER,
                fogDeviceConstants.IDLE_POWER,
                fogDeviceConstants.MEMORY_COST_RATE,
                fogDeviceConstants.STORAGE_COST_RATE,
                fogDeviceConstants.BANDWIDTH_COST_RATE);
        getFogDevices().add(fogNode);
        fogNode.setParentId(parentId);
        fogNode.setUplinkLatency(fogDeviceConstants.UPLINK_LATENCY);
        if (numberOfMobile > 0) {
            for (int i = 0; i < fogDeviceConstants.NUMBER_OF_MOBILE_PER_FOG; i++) {
                String mobileId = fogId + "-" + i;
                double mobileUplinkLatency = endDeviceConstants.UPLINK_LATENCY;
                FogDevice mobile = createMobile(mobileId, fogNode.getId());
                mobile.setUplinkLatency(mobileUplinkLatency);
                getFogDevices().add(mobile);
            }
        }
    }

    private FogDevice createMobile(String id, int parentId) throws Exception {
        EndDeviceConstants endDeviceConstants = EndDeviceConstants.DEFAULT;
        String hostName = endDeviceConstants.HOST_NAME;
        List<Long> mips = userInput.getHostMips().get(hostName);
        List<Double> costs = userInput.getHostCosts().get(hostName);
        int numberOfMobile = userInput.getNumberOfMobile();
        FogDevice mobile = createFogDevice(hostName + "-" + id,
                numberOfMobile, mips, costs,
                endDeviceConstants.RAM,
                endDeviceConstants.UPLINK_BANDWIDTH,
                endDeviceConstants.DOWNLINK_BANDWIDTH,
                endDeviceConstants.LEVEL,
                endDeviceConstants.MIPS_COST_RATE,
                endDeviceConstants.BUSY_POWER,
                endDeviceConstants.IDLE_POWER,
                endDeviceConstants.MEMORY_COST_RATE,
                endDeviceConstants.STORAGE_COST_RATE,
                endDeviceConstants.BANDWIDTH_COST_RATE);
        mobile.setParentId(parentId);
        return mobile;
    }

    private void createVirtualMachines(int userId, int numberOfVirtualMachine, List<? extends Host> hosts) {
        CondorVM[] condorVMS = new CondorVM[numberOfVirtualMachine];
        VirtualMachineConstants virtualMachineConstants = VirtualMachineConstants.DEFAULT;
        for (int i = 0; i < numberOfVirtualMachine; i++) {
            int mips = hosts.get(i).getTotalMips();
            CloudletSchedulerSpaceShared cloudletSchedulerPolicy = new CloudletSchedulerSpaceShared();
            condorVMS[i] = new CondorVM(i, userId, mips,
                    virtualMachineConstants.NUMBER_OF_CPU,
                    virtualMachineConstants.RAM,
                    virtualMachineConstants.BANDWIDTH,
                    virtualMachineConstants.IMAGE_SIZE,
                    virtualMachineConstants.VIRTUAL_MACHINE_MONITOR,
                    cloudletSchedulerPolicy);
            getVirtualMachines().add(condorVMS[i]);
        }
    }

    private void setHosts(List<FogDevice> fogDevices) {
        for (FogDevice fogDevice : fogDevices) {
            getHosts().addAll(fogDevice.getHostList());
        }
    }

    private void setNumberOfVirtualMachines(List<FogDevice> fogDevices) {
        for (FogDevice device : fogDevices) {
            numberOfVirtualMachine = getNumberOfVirtualMachine() + device.getHostList().size();
        }
    }

    /**
     * Calculates the average available bandwidth among all VMs in Megabit/second
     *
     * @return Average available bandwidth in Megabit/second
     */
    public static double getAverageBandwidth() {
        double averageBandwidth = 0.0;
        for (CondorVM virtualMachine : virtualMachines) {
            averageBandwidth += virtualMachine.getBw();
        }
        return averageBandwidth / virtualMachines.size();
    }

    public List<FogDevice> getFogDevices() {
        return fogDevices;
    }

    public List<? extends Host> getHosts() {
        return hosts;
    }

    public int getNumberOfVirtualMachine() {
        return numberOfVirtualMachine;
    }

    public List<CondorVM> getVirtualMachines() {
        return virtualMachines;
    }
}
