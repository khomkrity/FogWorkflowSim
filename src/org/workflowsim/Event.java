package org.workflowsim;

public class Event implements Comparable<Event> {

    public Double startTime;
    public Double finishTime;
    public Task task;

    public Event(Double startTime, Double finishTime) {
        this.startTime = startTime;
        this.finishTime = finishTime;
    }

    public Event(Task task, Double startTime, Double finishTime) {
        this.task = task;
        this.startTime = startTime;
        this.finishTime = finishTime;
    }

    @Override
    public int compareTo(Event o) {
        return startTime.compareTo(o.startTime);
    }
}
