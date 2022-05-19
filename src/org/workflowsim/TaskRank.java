package org.workflowsim;

public class TaskRank implements Comparable<TaskRank> {

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