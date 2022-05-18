package org.workflowsim.planning;

import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Event;
import org.workflowsim.Task;
import org.workflowsim.TaskRank;

import java.util.*;

public class OCSPlanningAlgorithm extends BasePlanningAlgorithm {

    private final Map<Task, Map<CondorVM, Double>> computationCosts;
    private final Map<Task, Map<Task, Double>> transferCosts;
    private static final List<Integer> taskOrders = new ArrayList<>();
    private final Map<Task, Double> rank;
    private final Map<CondorVM, List<Event>> schedules;
    private final Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    private List<List<Task>> paths;
    private List<Task> criticalPath;
    private final Set<Double> eventTimes;
    private Set<Task> scheduledTasks;

    public OCSPlanningAlgorithm() {
        computationCosts = new LinkedHashMap<>();
        transferCosts = new LinkedHashMap<>();
        rank = new LinkedHashMap<>();
        earliestFinishTimes = new LinkedHashMap<>();
        schedules = new LinkedHashMap<>();
        paths = new ArrayList<>();
        criticalPath = new ArrayList<>();
        eventTimes = new HashSet<>();
        scheduledTasks = new HashSet<>();
    }

    @Override
    public void run() throws Exception {
        Log.printLine("OCS planner running with " + getTaskList().size() + " tasks.");

        averageBandwidth = calculateAverageBandwidth();

        for (CondorVM vm : getVmList()) {
            schedules.put(vm, new ArrayList<>());
        }

        // Prioritization phase
        calculateComputationCosts();
        calculateTransferCosts();
        //calculateRanks();
        paths = findAllPaths();
        System.out.println("all path: " + paths);
        criticalPath = findCriticalPath(paths);
        System.out.println("critical path: " + criticalPath);
        calculateSchedulingTime(criticalPath);
        for (Task task : getTaskList()) {
            System.out.println("id: " + task + " start: " + task.getEstimatedStartTime() + " finish: " + task.getEstimatedFinishTime());
        }
        // Selection phase
        allocateTasks();
        // set task orders for later simulation
        setTaskOrders(taskOrders);
    }

    private List<List<Task>> findAllPaths() {
        List<List<Task>> preprocessPaths = new ArrayList<>();
        List<List<Task>> pathsWithMultipleExits = new ArrayList<>();
        List<List<Task>> validPaths = new ArrayList<>();
        List<Task> roots = getTaskList()
                .stream()
                .filter(task -> task.getParentList().isEmpty())
                .toList();

        for (Task root : roots) {
            for (Task child : root.getChildList()) {
                preprocessPaths.add(findPath(new ArrayList<>(List.of(root, child)), child));
            }
        }

        for (List<Task> path : preprocessPaths) {
            int numberOfExitTask = 0;
            for (Task task : path) {
                if (task.getChildList().isEmpty()) numberOfExitTask++;
            }
            if (numberOfExitTask > 1) pathsWithMultipleExits.add(path);
            else validPaths.add(path);
        }

        // [1, 2, 8, 10, 9, 10]
        //[1, 5, 11, 12, 13, 17, 18, 19, 20, 14, 17, 18, 19, 20, 15, 17, 18, 19, 20, 16, 17, 18, 19, 20]
        // 1 5 11 12 13 17 18 21 13 17 19 21 13 17 20 21 14 17 18 21 14 17 19 21 14 17 20 21 15 17 18 21 15 17 19 21 15 17 20 21
        for (List<Task> path : pathsWithMultipleExits) {
            Task root = path.get(0);
            int startIndex = 1;
            Task currentTask = path.get(startIndex);
            System.out.println(path);
            List<Task> pathBeforeMultipleBranches = new ArrayList<>(List.of(root, currentTask));
            while (currentTask.getChildList().size() == 1) {
                currentTask = currentTask.getChildList().get(0);
                pathBeforeMultipleBranches.add(currentTask);
                startIndex++;
            }
            List<Task> newPath = new ArrayList<>(List.copyOf(pathBeforeMultipleBranches));
            for (int i = startIndex + 1; i < path.size(); i++) {
                Task nextTask = path.get(i);
                if (nextTask.getParentList().contains(newPath.get(newPath.size() - 1))) {
                    newPath.add(nextTask);
                }
                if (nextTask.getChildList().isEmpty()) {
                    validPaths.add(newPath);
                    newPath = new ArrayList<>(List.copyOf(pathBeforeMultipleBranches));
                }
            }
        }
        return validPaths;
    }

    private List<Task> findPath(List<Task> path, Task task) {
        for (Task child : task.getChildList()) {
            path.add(child);
            findPath(path, child);
        }
        return path;
    }

    private List<Task> findCriticalPath(List<List<Task>> paths) {
        List<Task> criticalPath = new ArrayList<>();
        double maxCost = 0;
        for (List<Task> path : paths) {
            double cost = 0;
            for (int i = 0; i < path.size() - 1; i++) {
                Task parent = path.get(i);
                Task child = path.get(i + 1);
                double parentCost = getAverageComputationCost(computationCosts.get(parent).values());
                double childCost = getAverageComputationCost(computationCosts.get(child).values());
                double transferCost = transferCosts.get(parent).get(child);
                cost += parentCost + transferCost;
                if (child.getChildList().isEmpty()) {
                    cost += childCost;
                }
            }
            if (cost > maxCost) {
                criticalPath = path;
                maxCost = cost;
            }
        }
        return criticalPath;
    }

    private double getAverageComputationCost(Collection<Double> costs) {
        return costs.stream().mapToDouble(Double::doubleValue).sum() / costs.size();
    }

    private void calculateSchedulingTime(List<Task> criticalPath) {
        for (Task task : criticalPath) {
            List<Task> parents = task.getParentList().stream().filter(parent -> !parent.isEstimated()).toList();
            if (parents.isEmpty()) {
                estimateSchedulingTime(task, true);
            }
            findAncestors(task);
        }
    }

    private void findAncestors(Task task) {
        if (task.isEstimated()) return;
        List<Task> parentSiblings = task.getParentList()
                .stream()
                .filter(sibling -> !sibling.isEstimated())
                .toList();
        List<Task> siblings = task.getChildList().size() == 1 ? task.getChildList().get(0).getParentList()
                .stream()
                .filter(sibling -> !sibling.isEstimated() && !criticalPath.contains(sibling))
                .toList()
                : new ArrayList<>();
        if (isReadyToFindTaskPermutation(parentSiblings)) {
            estimateTaskPermutationSchedulingTime(parentSiblings);
        } else {
            // TODO: find ancestors based on lower path rank
            for (Task parent : task.getParentList()) {
                findAncestors(parent);
            }
//            System.out.println(task);
//            estimateSchedulingTime(task, true);
            if(isReadyToFindTaskPermutation(siblings)){
                estimateTaskPermutationSchedulingTime(siblings);
            }else{
                estimateSchedulingTime(task, true);
            }
        }
    }

    private void estimateTaskPermutationSchedulingTime(List<Task> siblings){
        List<List<Task>> taskPermutation = findTaskPermutation(siblings);
        List<Task> minTaskArrangement = new ArrayList<>();
        double estimatedFinishTime;
        double minEstimatedFinishTime = Double.MAX_VALUE;
        for (List<Task> taskArrangement : taskPermutation) {
            for (Task task : taskArrangement) {
                estimateSchedulingTime(task, false);
            }
            estimatedFinishTime = taskArrangement.get(taskArrangement.size() - 1).getEstimatedFinishTime();
            if (estimatedFinishTime < minEstimatedFinishTime) {
                minEstimatedFinishTime = estimatedFinishTime;
                minTaskArrangement = taskArrangement;
            }
        }
        for (Task task : minTaskArrangement) {
            estimateSchedulingTime(task, true);
        }
        System.out.println("total permutation: " + taskPermutation.size());
        System.out.println("all arrangement: " + taskPermutation);
        System.out.println("shortest arrangement: " + minTaskArrangement);
    }

    private boolean isReadyToFindTaskPermutation(List<Task> tasks) {
        if (tasks.size() <= 1) return false;
        for (Task task : tasks) {
            for (Task parent : task.getParentList()) {
                if (!parent.isEstimated()) return false;
            }
        }
        return true;
    }

    private List<List<Task>> findTaskPermutation(List<Task> tasks) {
        List<List<Task>> taskPermutation = new ArrayList<>();
        permute(taskPermutation, new ArrayList<>(), tasks);
        return taskPermutation;
    }

    private void permute(List<List<Task>> taskPermutation, List<Task> currentArrangement, List<Task> tasks) {
        if (currentArrangement.size() == tasks.size()) {
            taskPermutation.add(new ArrayList<>(currentArrangement));
        } else {
            for (int i = 0; i < tasks.size(); i++) {
                if (currentArrangement.contains(tasks.get(i))) {
                    continue;
                }
                currentArrangement.add(tasks.get(i));
                permute(taskPermutation, currentArrangement, tasks);
                currentArrangement.remove(currentArrangement.size() - 1);
            }
        }
    }

    private void estimateSchedulingTime(Task task, boolean allocateTimeSlot) {
        double startTime = Double.MIN_VALUE;
        double finishTime;
        for (Task parent : task.getParentList()) {
            startTime = Math.max(startTime, parent.getEstimatedFinishTime());
        }
        if (startTime == Double.MIN_VALUE) startTime = 0.1;
        finishTime = startTime + getAverageComputationCost(computationCosts.get(task).values());
        while (eventTimes.contains(startTime) || eventTimes.contains(finishTime)) {
            startTime += task.getSendingLatency();
            finishTime += task.getSendingLatency();
        }
        if (allocateTimeSlot) {
            eventTimes.add(startTime);
            eventTimes.add(finishTime);
            task.setEstimated(true);
        }
        task.setEstimatedStartTime(startTime);
        task.setEstimatedFinishTime(finishTime);
//        scheduledTasks.add(task);
    }

    /**
     * Calculates the average available bandwidth among all VMs in Megabit/s
     *
     * @return Average available bandwidth in Megabit/s
     */
    private double calculateAverageBandwidth() {
        double averageBandwidth = 0.0;
        for (CondorVM vm : getVmList()) {
            averageBandwidth += vm.getBw();
        }
        return averageBandwidth / getVmList().size();
    }

    /**
     * Populates the computationCosts field with the time in seconds to compute a
     * task in a vm.
     */
    private void calculateComputationCosts() {
        for (Task task : getTaskList()) {
            Map<CondorVM, Double> costsVm = new HashMap<>();
            for (CondorVM vm : getVmList()) {
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
                transferCosts.get(parent).put(child, calculateStaticTransferCost(parent, child));
            }
        }
    }

    /**
     * Accounts the time in seconds necessary to transfer all files described
     * between parent and child (as statically defined in DAG)
     *
     * @param parent parent task
     * @param child  child task
     * @return Transfer cost in seconds
     */
    private double calculateStaticTransferCost(Task parent, Task child) {
        return child.getTransferCosts().isEmpty() ? 0 : child.getTransferCosts().get(parent.getCloudletId());
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
//        System.out.print(task.getCloudletId()+" ");
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

        for (CondorVM vm : getVmList()) {
            double minReadyTime = 0.1;
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
        taskOrders.add(task.getCloudletId());
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
}
