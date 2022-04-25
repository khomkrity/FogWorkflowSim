package com.mfu.fog;

import org.workflowsim.Job;
import org.workflowsim.Task;

import java.util.*;

class SchedulingResult {
    private final Map<String, Map<String, List<Job>>> schedulingResultsByAlgorithmsEachDag;

    SchedulingResult() {
        this.schedulingResultsByAlgorithmsEachDag = new HashMap<>();
    }

    public void addResult(String dagName, String algorithmName, double portDelay, List<Job> scheduledJobs, List<Integer> jobSubmissionOrders) {
        List<Job> orderedJobs = getOrderedJobs(scheduledJobs, jobSubmissionOrders);
        setTaskFinishTime(orderedJobs);
        setPortDelayToJobs(portDelay, orderedJobs);
        setSchedulingResult(dagName, algorithmName, orderedJobs);
    }

    private List<Job> getOrderedJobs(List<Job> scheduledJobs, List<Integer> jobSubmissionOrders) {
        List<Job> orderedJobs = new ArrayList<>();
        for (int jobArrivalId : jobSubmissionOrders) {
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

    private void setTaskFinishTime(List<Job> jobs) {
        for (Job job : jobs) {
            job.setTaskFinishTime(job.getFinishTime());
        }
    }

    private void setPortDelayToJobs(double portDelay, List<Job> jobs) {
        if (portDelay == 0) return;
        Set<Double> eventTimes = new HashSet<>();
        for (int i = 0; i < jobs.size(); i++) {
            Job job = jobs.get(i);
            double startTime = job.getExecStartTime();
            double finishTime = job.getTaskFinishTime();

            // check the latest finish time by the same vm
            double latestFinishTime = Double.MIN_VALUE;
            for (int j = 0; j < i; j++) {
                Job previousJob = jobs.get(j);
                if (previousJob.getVmId() == job.getVmId()) {
                    latestFinishTime = previousJob.getTaskFinishTime();
                }
            }

            while (startTime <= latestFinishTime) {
                startTime += portDelay;
                finishTime += portDelay;
            }

            // check parent's finish time
            for (Task parent : job.getParentList()) {
                double parentFinishTime = parent.getTaskFinishTime();
                while (startTime <= parentFinishTime) {
                    startTime += portDelay;
                    finishTime += portDelay;
                }
            }

            // check all event time
            while (eventTimes.contains(startTime)) {
                startTime += portDelay;
                finishTime += portDelay;
            }

            job.setExecStartTime(startTime);
            job.setTaskFinishTime(finishTime);
            eventTimes.add(startTime);
            eventTimes.add(finishTime);
        }
    }

    private void setSchedulingResult(String dagName, String algorithmName, List<Job> jobs) {
        if (getSchedulingResultsByAlgorithmsEachDag().containsKey(dagName)) {
            getSchedulingResultsByAlgorithmsEachDag().get(dagName).put(algorithmName, jobs);
        } else {
            Map<String, List<Job>> newSchedulingResult = new HashMap<>();
            newSchedulingResult.put(algorithmName, jobs);
            getSchedulingResultsByAlgorithmsEachDag().put(dagName, newSchedulingResult);
        }
    }

    public Map<String, Map<String, List<Job>>> getSchedulingResultsByAlgorithmsEachDag() {
        return schedulingResultsByAlgorithmsEachDag;
    }
}
