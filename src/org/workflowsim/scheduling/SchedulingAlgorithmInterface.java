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

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;

import java.util.List;

/**
 * The Scheduler interface
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public interface SchedulingAlgorithmInterface {

    /**
     * Sets the job list.
     * @param list list of cloudlets
     */
    void setCloudletList(List<Cloudlet> list);

    /**
     * Sets the vm list.
     * @param list list of vms
     */
    void setVmList(List<CondorVM> list);

    /**
     * Gets the job list.
     */
    List<Cloudlet> getCloudletList();

    /**
     * Gets the vm list.
     */
    List<CondorVM> getVmList();

    /**
     * the main function.
     * @throws java.lang.Exception scheduling error
     */
    void run() throws Exception;

    /**
     * Gets the scheduled jobs.
     */
    List<Cloudlet> getScheduledList();
}
