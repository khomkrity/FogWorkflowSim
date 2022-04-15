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
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;

/**
 * The base scheduler has implemented the basic features. Every other scheduling method
 * should extend from BaseSchedulingAlgorithm but should not directly use it. 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public abstract class BaseSchedulingAlgorithm implements SchedulingAlgorithmInterface {

    /**
     * the job list.
     */
    private List<Cloudlet> cloudletList;
    /**
     * the vm list.
     */
    private List<CondorVM> vmList;
    /**
     * the scheduled job list.
     */
    private List<Cloudlet> scheduledList;

    /*
     * Initialize a BaseSchedulingAlgorithm
     */
    public BaseSchedulingAlgorithm() {
        this.scheduledList = new ArrayList<>();
    }

    /**
     * Sets the job list.
     *
     * @param list list of cloudlets
     */
    @Override
    public void setCloudletList(List<Cloudlet> list) {
        this.cloudletList = list;
    }

    /**
     * Sets the vm list
     *
     * @param list list of vms
     */
    @Override
    public void setVmList(List<CondorVM> list) {
        this.vmList = list;
    }

    /**
     * Gets the job list.
     *
     * @return the job list
     */
    @Override
    public List<Cloudlet> getCloudletList() {
        return this.cloudletList;
    }

    /**
     * Gets the vm list
     *
     * @return the vm list
     */
    @Override
    public List<CondorVM> getVmList() {
        return this.vmList;
    }

    /**
     * The main function
     * @throws java.lang.Exception scheduling error
     */
    @Override
    public abstract void run() throws Exception;

    /**
     * Gets the scheduled job list
     *
     * @return job list
     */
    @Override
    public List<Cloudlet> getScheduledList() {
        return this.scheduledList;
    }

}
