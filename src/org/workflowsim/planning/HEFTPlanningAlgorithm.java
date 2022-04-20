/*
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.planning;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

import java.util.*;

/**
 * The HEFT planning algorithm.
 *
 * @author Pedro Paulo Vezzá Campos
 * @since Oct 12, 2013
 */
public class HEFTPlanningAlgorithm extends BasePlanningAlgorithm {

    private final Map<Task, Map<CondorVM, Double>> computationCosts;
    private Map<Task, Double> averageComputationCosts;
    private final Map<Task, Map<Task, Double>> transferCosts;
    private final Map<Task, Double> rank;
    private final Map<CondorVM, List<Event>> schedules;
    private final Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    private static class Event {

        public double startTime;
        public double finishTime;
        public Task task;

        public Event(double startTime, double finishTime) {
            this.startTime = startTime;
            this.finishTime = finishTime;
        }

        public Event(Task task, double startTime, double finishTime) {
            this.task = task;
            this.startTime = startTime;
            this.finishTime = finishTime;
        }
    }

    private static class TaskRank implements Comparable<TaskRank> {

        public Task task;
        public Double rank;

        public TaskRank(Task task, Double rank) {
            this.task = task;
            this.rank = rank;
        }

        @Override
        public int compareTo(TaskRank o) {
            return o.rank.compareTo(rank);
        }
    }

    public HEFTPlanningAlgorithm() {
        computationCosts = new HashMap<>();
        transferCosts = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
        schedules = new HashMap<>();
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Log.printLine("HEFT planner running with " + getTaskList().size() + " tasks.");

        averageBandwidth = calculateAverageBandwidth();

        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            schedules.put(vm, new ArrayList<>());
        }

        // Prioritization phase
        calculateComputationCosts();
        calculateTransferCosts();
        calculateRanks();

        // Selection phase
        allocateTasks();

        // result
        print();
    }

    /**
     * Calculates the average available bandwidth among all VMs in Mbit/s
     *
     * @return Average available bandwidth in Mbit/s
     */
    private double calculateAverageBandwidth() {
        double avg = 0.0;
        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            avg += vm.getBw();
        }
        return avg / getVmList().size();
    }

    /**
     * Populates the computationCosts field with the time in seconds to compute a
     * task in a vm.
     */
    private void calculateComputationCosts() {
        for (Task task : getTaskList()) {
            Map<CondorVM, Double> costsVm = new HashMap<>();
            for (Object vmObject : getVmList()) {
                CondorVM vm = (CondorVM) vmObject;
                if (vm.getNumberOfPes() < task.getNumberOfPes()) {
                    costsVm.put(vm, Double.MAX_VALUE);
                } else {
                    costsVm.put(vm, task.getCloudletTotalLength() / vm.getMips());
                }
            }
            computationCosts.put(task, costsVm);
        }
    }

    /**
     * Populates the transferCosts map with the time in seconds to transfer all
     * files from each parent to each child
     */
    private void calculateTransferCosts() {
        // Initializing the matrix
        for (Task task1 : getTaskList()) {
            Map<Task, Double> taskTransferCosts = new HashMap<>();
            for (Task task2 : getTaskList()) {
                taskTransferCosts.put(task2, 0.0);
            }
            transferCosts.put(task1, taskTransferCosts);
        }

        // Calculating the actual values
        for (Task parent : getTaskList()) {
            for (Task child : parent.getChildList()) {
                transferCosts.get(parent).put(child, calculateTransferCost(parent, child));
            }
        }
    }

    /**
     * Accounts the time in seconds necessary to transfer all files described
     * between parent and child
     *
     * @param parent parent task
     * @param child  child task
     * @return Transfer cost in seconds
     */
    private double calculateTransferCost(Task parent, Task child) {
        List<FileItem> parentFiles = parent.getFileList();
        List<FileItem> childFiles = child.getFileList();

        double transferCost = 0.0;

        for (FileItem parentFile : parentFiles) {
            if (parentFile.getType() != Parameters.FileType.OUTPUT) {
                continue;
            }

            for (FileItem childFile : childFiles) {
                Parameters.FileType childFileType = childFile.getType();
                String parentFileName = parentFile.getName();
                String childFileName = childFile.getName();
                boolean isInputFile = childFileType == Parameters.FileType.INPUT;
                boolean hasSameFile = childFileName.equals(parentFileName);
                if (isInputFile && hasSameFile) {
                    transferCost += childFile.getSize();
                    break;
                }
            }
        }

        // convert from byte to megabyte
        transferCost = transferCost / Consts.MILLION;

        // convert from megabyte to megabyte per second
        transferCost = transferCost * 8 / averageBandwidth;

        return transferCost;
    }

    /**
     * Invokes calculateRank for each task to be scheduled
     */
    private void calculateRanks() {
        for (Task task : getTaskList()) {
            calculateRank(task);
        }
    }

    /**
     * Populates rank.get(task) with the rank of task as defined in the HEFT paper.
     *
     * @param task The task have the rank calculates
     * @return The rank
     */
    private double calculateRank(Task task) {
        if (rank.containsKey(task)) {
            return rank.get(task);
        }

        double averageComputationCost = 0.0;

        for (Double cost : computationCosts.get(task).values()) {
            averageComputationCost += cost;
        }

        averageComputationCost /= computationCosts.get(task).size();

        double max = 0.0;
        for (Task child : task.getChildList()) {
            double childCost = transferCosts.get(task).get(child) + calculateRank(child);
            max = Math.max(max, childCost);
        }

        rank.put(task, averageComputationCost + max);

        return rank.get(task);
    }

    /**
     * Allocates all tasks to be scheduled in non-ascending order of schedule.
     */
    private void allocateTasks() {
        List<TaskRank> taskRank = new ArrayList<>();
        for (Task task : rank.keySet()) {
            taskRank.add(new TaskRank(task, rank.get(task)));
        }

        // Sorting in non-ascending order of rank
        Collections.sort(taskRank);
        for (TaskRank rank : taskRank) {
            allocateTask(rank.task);
        }

    }

    /**
     * Schedules the task given in one of the VMs minimizing the earliest finish
     * time
     *
     * @param task The task to be scheduled
     * @pre All parent tasks are already scheduled
     */
    private void allocateTask(Task task) {
        CondorVM chosenVM = null;
        double earliestFinishTime = Double.MAX_VALUE;
        double bestReadyTime = 0.1;
        double finishTime;

        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            double minReadyTime = 0.0;

            for (Task parent : task.getParentList()) {
                double readyTime = earliestFinishTimes.get(parent);
                if (parent.getVmId() != vm.getId()) {
                    readyTime += transferCosts.get(parent).get(task);
                }
                minReadyTime = Math.max(minReadyTime, readyTime);
            }

            finishTime = findFinishTime(task, vm, minReadyTime, false);

            if (finishTime < earliestFinishTime) {
                bestReadyTime = minReadyTime;
                earliestFinishTime = finishTime;
                chosenVM = vm;
            }
        }

        findFinishTime(task, chosenVM, bestReadyTime, true);
        earliestFinishTimes.put(task, earliestFinishTime);

        assert chosenVM != null;
        task.setVmId(chosenVM.getId());
    }

    /**
     * Finds the best time slot available to minimize the finish time of the given
     * task in the vm with the constraint of not scheduling it before readyTime. If
     * occupySlot is true, reserves the time slot in the schedule.
     *
     * @param task       The task to have the time slot reserved
     * @param vm         The vm that will execute the task
     * @param readyTime  The first moment that the task is available to be scheduled
     * @param occupySlot If true, reserves the time slot in the schedule.
     * @return The minimal finish time of the task in the vmn
     */
    private double findFinishTime(Task task, CondorVM vm, double readyTime, boolean occupySlot) {
        List<Event> currentVmSchedules = schedules.get(vm);
        double computationCost = computationCosts.get(task).get(vm);
        double startTime, finishTime;
        int index;

        if (currentVmSchedules.isEmpty()) {
            if (occupySlot) {
                currentVmSchedules.add(new Event(task, readyTime, readyTime + computationCost));
            }
            return readyTime + computationCost;
        }

        if (currentVmSchedules.size() == 1) {
            if (readyTime >= currentVmSchedules.get(0).finishTime) {
                index = 1;
                startTime = readyTime;
            } else if (readyTime + computationCost <= currentVmSchedules.get(0).startTime) {
                index = 0;
                startTime = readyTime;
            } else {
                index = 1;
                startTime = currentVmSchedules.get(0).finishTime;
            }

            if (occupySlot) {
                currentVmSchedules.add(index, new Event(task, startTime, startTime + computationCost));
            }
            return startTime + computationCost;
        }

        // Trivial case: Start after the latest task scheduled
        startTime = Math.max(readyTime, currentVmSchedules.get(currentVmSchedules.size() - 1).finishTime);
        finishTime = startTime + computationCost;
        int currentIndex = currentVmSchedules.size() - 1;
        int previousIndex = currentVmSchedules.size() - 2;
        index = currentIndex + 1;
        while (previousIndex >= 0) {
            Event currentEvent = currentVmSchedules.get(currentIndex);
            Event previousEvent = currentVmSchedules.get(previousIndex);

            if (readyTime > previousEvent.finishTime) {
                if (readyTime + computationCost <= currentEvent.startTime) {
                    startTime = readyTime;
                    finishTime = readyTime + computationCost;
                }

                break;
            }
            if (previousEvent.finishTime + computationCost <= currentEvent.startTime) {
                startTime = previousEvent.finishTime;
                finishTime = previousEvent.finishTime + computationCost;
                index = currentIndex;
            }
            currentIndex--;
            previousIndex--;
        }

        if (readyTime + computationCost <= currentVmSchedules.get(0).startTime) {
            index = 0;
            startTime = readyTime;

            if (occupySlot) {
                currentVmSchedules.add(index, new Event(task, startTime, startTime + computationCost));
            }
            return startTime + computationCost;
        }
        if (occupySlot) {
            currentVmSchedules.add(index, new Event(task, startTime, finishTime));
        }
        return finishTime;
    }

    private void print() {
        System.out.println("HEFT Algorithm");
        System.out.println("=====================");
        printVMSpec();
        printAverageBW();
        System.out.println("Prioritization Phase");
        System.out.println();
        printComputationCosts();
        printTransferCosts();
        printTaskRanks();
        printSchedules();
        System.out.println("=====================");
    }

    private void printVMSpec() {
        System.out.println("VM Specs:");
        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            System.out.print("VM ID: " + vm.getId() + " ");
            System.out.print("MIPS: " + vm.getMips() + " ");
            System.out.print("BW: " + vm.getBw() + " ");
            System.out.println();
        }
    }

    private void printAverageBW() {
        System.out.println("Average BW: " + averageBandwidth);
        System.out.println();
    }

    private void printComputationCosts() {
        System.out.println("Calculate Computation Costs of Each Task to Every VMs");
        for (Map.Entry<Task, Map<CondorVM, Double>> tasks : computationCosts.entrySet()) {
            Task task = tasks.getKey();
            int taskId = task.getCloudletId();
            double taskLength = task.getCloudletTotalLength();
            System.out.println("Task ID: " + taskId);
            System.out.println("Cloudlet Length: " + taskLength);
            for (Map.Entry<CondorVM, Double> vms : tasks.getValue().entrySet()) {
                CondorVM vm = vms.getKey();
                int vmId = vm.getId();
                double mips = vm.getMips();
                double computationCost = vms.getValue();
                System.out.println("VM ID: " + vmId + " MIPS: " + mips);
                System.out.println("Computation Cost (Length / MIPS): " + computationCost);
            }
            System.out.println();
        }
    }

    private void printTransferCosts() {
        System.out.println("Calcualte Time Taken in Transfering Files from Each Parent to Childs");
        for (Map.Entry<Task, Map<Task, Double>> parentTasks : transferCosts.entrySet()) {
            Task parentTask = parentTasks.getKey();
            int parentTaskId = parentTask.getCloudletId();
            System.out.println("Parent Task ID: " + parentTaskId);
            for (Map.Entry<Task, Double> childTasks : parentTasks.getValue().entrySet()) {
                Task childTask = childTasks.getKey();
                int childTaskId = childTask.getCloudletId();
                double transferCost = childTasks.getValue();
                if (transferCost == 0)
                    continue;
                System.out.println("Child Task ID: " + childTaskId);
                System.out.println("Transfer Cost ((File Size / Million) * 8) / AverageBW: " + transferCost + " Sec.");
            }
            System.out.println();
        }
    }

    private void printTaskRanks() {
        System.out.println("Calculate Rank of Each Task");
        for (Map.Entry<Task, Double> ranks : rank.entrySet()) {
            Task task = ranks.getKey();
            int taskId = task.getCloudletId();
            double rank = ranks.getValue();
            System.out.println("Task Id: " + taskId);
            System.out.println("Rank (average computation cost + max transfer cost): " + rank);
            System.out.println();
        }
    }

    private void printSchedules() {
        System.out.println("Scheduling Results (kind of) Grouped By VM");
        // private Map<CondorVM, List<Event>> schedules;
        for (Map.Entry<CondorVM, List<Event>> schedule : schedules.entrySet()) {
            CondorVM vm = schedule.getKey();
            int vmId = vm.getId();
            System.out.println("VM ID: " + vmId);
            List<Event> events = schedule.getValue();
            for (Event event : events) {
                Task task = event.task;
                int taskId = task.getCloudletId();
                double startTime = event.startTime;
                double finishTime = event.finishTime;
                double executionTime = finishTime - startTime;
                System.out.println("Task Id: " + taskId);
                System.out.println("Start Time: " + startTime);
                System.out.println("Finish Time: " + finishTime);
                System.out.println("Execution Time: " + executionTime);
            }
            System.out.println();
        }
    }

}
