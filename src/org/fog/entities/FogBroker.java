package org.fog.entities;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;
import org.workflowsim.*;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.planning.BasePlanningAlgorithm;
import org.workflowsim.scheduling.*;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.SchedulingAlgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class FogBroker extends PowerDatacenterBroker {

    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;
    /**
     * the start time of algorithm
     */
    public long startTime;
    public static int count = 0;// 初始化时，计数当前根据哪个粒子来为job分配虚拟机
    public static int count2 = 0;// 更新粒子时，计数当前根据哪个粒子来为job分配虚拟机
    public static int initIndexForGA = 0;
    public static int tempChildrenIndex = 0;
    public static double totalDelay;
    private static List<Integer> jobSubmissionOrders;

    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by Sim_entity
     *             class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     */
    public FogBroker(String name) throws Exception {
        super(name);
        List<CondorVM> scheduledVmList = new ArrayList<>();
        jobSubmissionOrders = new ArrayList<>();
        totalDelay = 0.0;
    }

    /**
     * Binds this scheduler to a datacenter
     *
     * @param datacenterId data center id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {

        this.workflowEngineId = workflowEngineId;
    }

    /**
     * Process an event
     *
     * @param ev a simEvent obj
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case WorkflowSimTags.CLOUDLET_CHECK:
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;
            case CloudSimTags.CLEAR:
                clearVmProcessing(ev);
                break;
            case CloudSimTags.CLEARCONSUMPTION:
                clearConsumption(ev);
                break;
            case WorkflowSimTags.CLOUDLET_UPDATE:
                switch (Parameters.getSchedulingAlgorithm()) {
                    case PSO:
                        if (WorkflowEngine.updateFlag == 0 && WorkflowEngine.startlastSchedule == 0) {
                            processCloudletUpdateForPSOInit(ev);
                        } else if (WorkflowEngine.startlastSchedule == 0) {
                            processCloudletUpdateForPSOUpdate(ev);
                        } else {
                            processCloudletUpdateForPSOGbest(ev);
                        }
                        break;
                    case GA:
                        if (WorkflowEngine.gaFlag == 0 && WorkflowEngine.findBestSchedule == 0) {
                            processCloudletUpdateForGAInit(ev);
                        } else if (WorkflowEngine.gaFlag == 1 && WorkflowEngine.findBestSchedule == 0) {
                            processCloudletUpdateForGA(ev);
                        }
                        if (WorkflowEngine.findBestSchedule == 1)
                            processCloudletUpdateForGABest(ev);
                        break;
                    case MINMIN:
                    case MAXMIN:
                    case FCFS:
                    case MCT:
                    case STATIC:
                    case DATA:
                    case ROUNDROBIN:
                        processCloudletUpdate(ev);
                        break;
                    default:
                        break;
                }
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void clearConsumption(SimEvent ev) {
        // TODO Auto-generated method stub
        for (int datacenterId : this.datacenterIdsList) {
            schedule(datacenterId, 0, CloudSimTags.CLEARCONSUMPTION, null);
        }

    }

    private void clearVmProcessing(SimEvent ev) {
        for (int datacenterId : this.datacenterIdsList) {
            schedule(datacenterId, 0, CloudSimTags.CLEAR, null);
        }
    }

    /**
     * Switch between multiple schedulers. Based on algorithm method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(SchedulingAlgorithm name) {

		/*
		 choose which algorithm to use. Make sure you have added related enum in
		 Parameters.java
		*/
        return switch (name) {
            // by default, it is Static
            case FCFS -> new FCFSSchedulingAlgorithm();
            case MINMIN -> new MinMinSchedulingAlgorithm();
            case MAXMIN -> new MaxMinSchedulingAlgorithm();
            case MCT -> new MCTSchedulingAlgorithm();
            case DATA -> new DataAwareSchedulingAlgorithm();
            case ROUNDROBIN -> new RoundRobinSchedulingAlgorithm();
            default -> new StaticSchedulingAlgorithm();
        };
    }

    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            /*
             * Fix a bug of cloudsim Don't add a null to getVmsCreatedList() June 15, 2013
             */
            Vm vm = VmList.getById(getVmList(), vmId);
            if (vm != null) {
                getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
                Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
                        + " has been created in Datacenter #" + datacenterId + ", Host #"
                        + vm.getHost().getId());
            }
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId + " failed in Datacenter #"
                    + datacenterId);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }

    /**
     * Update a cloudlet (job)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {
        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        List<Cloudlet> cloudlets = getCloudletList();
        if (!Parameters.getPlanningAlgorithm().equals(Parameters.PlanningAlgorithm.INVALID)
                && Parameters.getSchedulingAlgorithm().equals(SchedulingAlgorithm.STATIC)) {
            cloudlets = getOrderedCloudletsFromPlanningAlgorithm(cloudlets);
        }
        scheduler.setCloudletList(cloudlets);
        List<? extends Vm> vmlist = getVmsCreatedList();
        Collections.reverse(vmlist);
        scheduler.setVmList((List<CondorVM>) vmlist);

        try {
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }
        List<Cloudlet> scheduledList = scheduler.getScheduledList();
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            getJobSubmissionOrders().add(cloudlet.getCloudletId());
            double delay = getDelayFromTransferCost((Task) cloudlet);
            totalDelay += delay;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();

    }

    private List<Cloudlet> getOrderedCloudletsFromPlanningAlgorithm(List<Cloudlet> cloudlets) {
        List<Cloudlet> orderedCloudlets = new ArrayList<>();
        List<Integer> orders = BasePlanningAlgorithm.getTaskOrders();
        while (orderedCloudlets.size() != cloudlets.size()) {
            for (int cloudletId : orders) {
                for (Cloudlet cloudlet : cloudlets) {
                    if (cloudletId == cloudlet.getCloudletId()) {
                        orderedCloudlets.add(cloudlet);
                        break;
                    }
                }
            }
        }
        return orderedCloudlets;
    }

    /**
     * Update a cloudlet (job)
     * 每接收到一个任务以后，调用此方法，设置提交到调度机上的任务，以及调度机绑定的虚拟机，并为cloudlets分配虚拟机
     * 每处理完一个任务以后，调用此方法，更新提交到调度机上的任务，以及调度机绑定的虚拟机
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdateForPSOInit(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        if (PsoScheduling.initFlag == 0) {
            startTime = System.currentTimeMillis();
            WorkflowEngine engine = (WorkflowEngine) CloudSim.getEntity(workflowEngineId);
            PsoScheduling.init(WorkflowEngine.jobList.size(), getVmList().size());
        }
        List<Cloudlet> scheduledList = new ArrayList<>();
        List<int[]> schedules = PsoScheduling.schedules;
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = schedules.get(count)[cloudletId];
            value.setVmId(vmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    protected void processCloudletUpdateForPSOUpdate(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        if (WorkflowEngine.updateFlag2 == 1 && cloudletList.size() != 0) {
            PsoScheduling.updateParticles();
        }
        List<Cloudlet> scheduledList = new ArrayList<>();
        List<int[]> newSchedules = PsoScheduling.newSchedules;
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = newSchedules.get(count2)[cloudletId];
            value.setVmId(vmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        // 把Cloudlets交由数据中心处理以后，从CloudletList中移除这些任务，并向CloudletSubmittedList中添加这些任务
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    protected void processCloudletUpdateForPSOGbest(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        List<Cloudlet> scheduledList = new ArrayList<>();
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = PsoScheduling.gbest_schedule[cloudletId];
            value.setVmId(vmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        // 把Cloudlets交由数据中心处理以后，从CloudletList中移除这些任务，并向CloudletSubmittedList中添加这些任务
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    protected void processCloudletUpdateForGAInit(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        if (GASchedulingAlgorithm.initFlag == 0) {
            startTime = System.currentTimeMillis();
            WorkflowEngine engine = (WorkflowEngine) CloudSim.getEntity(workflowEngineId);
            GASchedulingAlgorithm.initPopsRandomly(WorkflowEngine.jobList.size(), getVmList().size());
        }
        List<Cloudlet> scheduledList = new ArrayList<>();
        List<int[]> schedules = GASchedulingAlgorithm.schedules;
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = schedules.get(initIndexForGA)[cloudletId];
            int scheduledVmId = ChooseVm(value, vmId);
            value.setVmId(scheduledVmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        // 把Cloudlets交由数据中心处理以后，从CloudletList中移除这些任务，并向CloudletSubmittedList中添加这些任务
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    protected void processCloudletUpdateForGA(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        if (WorkflowEngine.gaFlag2 == 0 && cloudletList.size() != 0) {
            GASchedulingAlgorithm.GA();
        }
        List<Cloudlet> scheduledList = new ArrayList<>();
        List<int[]> schedules = GASchedulingAlgorithm.tempChildren;
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = schedules.get(tempChildrenIndex)[cloudletId];
            int scheduledVmId = ChooseVm(value, vmId);
            value.setVmId(scheduledVmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        // 把Cloudlets交由数据中心处理以后，从CloudletList中移除这些任务，并向CloudletSubmittedList中添加这些任务
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    protected void processCloudletUpdateForGABest(SimEvent ev) {
        List<Cloudlet> cloudletList = getCloudletList();
        List<CondorVM> vmList = getVmsCreatedList();
        List<Cloudlet> scheduledList = new ArrayList<Cloudlet>();
        for (Cloudlet value : cloudletList) {
            int cloudletId = value.getCloudletId();
            int vmId = GASchedulingAlgorithm.gbestSchedule[cloudletId];
            int scheduledVmId = ChooseVm(value, vmId);
            value.setVmId(scheduledVmId);
            // setVmState(vmId);
            scheduledList.add(value);
        }
        for (Cloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        // 把Cloudlets交由数据中心处理以后，从CloudletList中移除这些任务，并向CloudletSubmittedList中添加这些任务
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    /**
     * Process a cloudlet (job) return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        Job job = (Job) cloudlet;
        // double delay = getCloudletDelay(getCloudletReceivedList(), cloudlet);
        double delay = 0;
        /*
         * Generate a failure if failure rate is not zeros.
         */
        FailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);

        CondorVM vm = (CondorVM) getVmsCreatedList().get(cloudlet.getVmId());
        // so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);
        vm.setlastUtilizationUpdateTime(CloudSim.clock());

        WorkflowEngine wfEngine = (WorkflowEngine) CloudSim.getEntity(workflowEngineId);
        Controller controller = wfEngine.getController();
        if (Parameters.getOverheadParams().getPostDelay() != null) {
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }

        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        // not really update right now, should wait 1 s until many jobs have returned
        // schedule(getVmsToDatacentersMap().get(cloudlet.getVmId()), delay, WorkflowSimTags.CLOUDLET_UPDATE, cloudlet);
        schedule(this.getId(), 0, WorkflowSimTags.CLOUDLET_UPDATE);

    }

    /**
     * Start this entity (WorkflowScheduler)
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        // int gisID = CloudSim.getEntityId(regionalCisName);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }

    /**
     * Terminate this entity (WorkflowScheduler)
     */
    @Override
    public void shutdownEntity() {
        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");
    }

    /**
     * Submit cloudlets (jobs) to the created VMs. Scheduling is here
     */
    @Override
    protected void submitCloudlets() {
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }

    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> jobs = (List) ev.getData();
        for(Job job : jobs){
            job.setTransferCosts(job.getTaskList().get(0).getTransferCosts());
            job.setSendingLatency(job.getTaskList().get(0).getSendingLatency());
            job.setReceivingLatency(job.getTaskList().get(0).getReceivingLatency());
        }
        getCloudletList().addAll(jobs);
        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        setDatacenterCharacteristicsList(new HashMap<>());
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");
        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }

    private void setVmState(int id) {
        for (Vm vm : getVmList()) {
            if (vm.getId() == id) {
                CondorVM vm2 = (CondorVM) vm;
                vm2.setState(WorkflowSimTags.VM_STATUS_BUSY);
            }
        }
    }


    /**
     * 根据任务卸载决策的结果，对智能算法所生成的虚拟机编号进行转换
     *
     * @param cloudlet 任务
     * @param vmId     智能算法生成的虚拟机编号
     * @return 根据卸载决策结果所选择的虚拟机编号
     */
    private int ChooseVm(Cloudlet cloudlet, int vmId) {
        List<CondorVM> list = new ArrayList<CondorVM>();
        int chooseVmId = -1;
        Job job = (Job) cloudlet;
        if (job.getoffloading() == -1) {
            return vmId;
        } else {
            for (Vm vm : getVmList()) {
                CondorVM cvm = (CondorVM) vm;
                if (job.getoffloading() == cvm.getHost().getDatacenter().getId())
                    list.add(cvm);
            }
            for (Vm vm : list) {
                if (vmId == vm.getId()) {
                    return vmId;
                }
            }
            chooseVmId = list.get(0).getId() + vmId % list.size();
        }
        list.clear();
        return chooseVmId;
    }

    /**
     * 根据任务卸载决策的结果得到相应数据中心的虚拟机列表
     *
     * @param cloudlet 任务
     * @return 决策后相应数据中心的虚拟机列表
     */
    private List<CondorVM> getScheduledVmList(Cloudlet cloudlet) {
        Job job = (Job) cloudlet;
        if (job.getoffloading() == -1) {
            Log.printLine("no offloading strategy is defined");
            return getVmList();
        }
        List<CondorVM> list = new ArrayList<>();
        for (Vm vm : getVmList()) {
            CondorVM cvm = (CondorVM) vm;
            if (job.getoffloading() == cvm.getHost().getDatacenter().getId())
                list.add(cvm);
        }
        return list;
    }

    private double getDelayFromTransferCost(Task child) {
        if(child.getTransferCosts().isEmpty()) return 0;
        double latestFinishTime = 0;
        double minReadyTime = 0;
        for (Task parent : child.getParentList()) {
            double readyTime = parent.getFinishTime();
            latestFinishTime = Math.max(latestFinishTime, readyTime);
            if (parent.getVmId() != child.getVmId()) {
                readyTime += child.getTransferCosts().get(parent.getCloudletId());
            }
            minReadyTime = Math.max(minReadyTime, readyTime);
        }
        return minReadyTime - latestFinishTime;
    }

    public static List<Integer> getJobSubmissionOrders() {
        return jobSubmissionOrders;
    }
}