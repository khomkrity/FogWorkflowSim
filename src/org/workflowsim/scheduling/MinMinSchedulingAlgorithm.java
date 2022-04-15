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
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowSimTags;

import java.util.ArrayList;
import java.util.List;

/**
 * MinMin algorithm.
 *
 * @author Weiwei Chen
 * @date Apr 9, 2013
 * @since WorkflowSim Toolkit 1.0
 */
public class MinMinSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public MinMinSchedulingAlgorithm() {
        super();
    }

    private final List<Boolean> hasChecked = new ArrayList<>();

    @Override
    public void run() {

        int size = getCloudletList().size();
        List<Cloudlet> cloudlets = getCloudletList();
        hasChecked.clear();
        for (int t = 0; t < size; t++) {
            hasChecked.add(false);
        }
        while (!cloudlets.isEmpty()) {
            int minIndex = 0;
            Cloudlet minCloudlet = null;
            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = cloudlets.get(j);
                if (!hasChecked.get(j)) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                    break;
                }
            }
            if (minCloudlet == null) {
                break;
            }


            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = cloudlets.get(j);
                if (hasChecked.get(j)) {
                    continue;
                }
                long length = cloudlet.getCloudletLength();
                if (length < minCloudlet.getCloudletLength()) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                }
            }
            hasChecked.set(minIndex, true);

            Job job = (Job) minCloudlet;
            List<CondorVM> vlist = getVmList();
            List<CondorVM> schedulableVmList = new ArrayList<>();
            if (job.getoffloading() == -1) {
                schedulableVmList.addAll(vlist);
            } else {
                for (CondorVM vm : vlist) {
                    if (job.getoffloading() == vm.getHost().getDatacenter().getId())
                        schedulableVmList.add(vm);
                }
            }
            int vmSize = schedulableVmList.size();
            CondorVM firstIdleVm = null;
            for (CondorVM vm : schedulableVmList) {
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    firstIdleVm = vm;
                    break;
                }
            }
            if (firstIdleVm == null) {
                CondorVM fast = schedulableVmList.get(0);
                for (CondorVM vm : schedulableVmList) {
                    if (vm.getMips() > fast.getMips())
                        fast = vm;
                }
                firstIdleVm = fast;
            } else {
                for (CondorVM vm : schedulableVmList) {
                    if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)
                            && vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips()) {
                        firstIdleVm = vm;
                    }
                }
            }
            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            minCloudlet.setVmId(firstIdleVm.getId());
            getScheduledList().add(minCloudlet);
            cloudlets.remove(minCloudlet);
            size = cloudlets.size();
        }
    }
}
