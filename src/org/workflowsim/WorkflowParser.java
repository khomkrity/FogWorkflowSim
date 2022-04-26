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
package org.workflowsim;

import org.cloudbus.cloudsim.Log;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.FileType;
import org.workflowsim.utils.ReplicaCatalog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WorkflowParser parse a DAX into tasks so that WorkflowSim can manage them
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @since Aug 23, 2013
 * @since Nov 9, 2014
 */
public final class WorkflowParser {

    /**
     * The path to DAX file.
     */
    private final String daxPath;
    /**
     * The path to DAX files.
     */
    private final List<String> daxPaths;
    /**
     * All tasks.
     */
    private List<Task> taskList;
    /**
     * User id. used to create a new task.
     */
    private final int userId;

    /**
     * current job id. In case multiple workflow submission
     */
    private int jobIdStartsFrom;

    /**
     * Gets the task list
     *
     * @return the task list
     */
    public List<Task> getTaskList() {
        return taskList;
    }

    /**
     * Sets the task list
     *
     * @param taskList the task list
     */
    private void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }

    /**
     * Map from task name to task.
     */
    private final Map<String, Task> taskByName;

    /**
     * Initialize a WorkflowParser
     *
     * @param userId the user id. Currently, we have just checked single user mode
     */
    public WorkflowParser(int userId) {
        this.userId = userId;
        this.taskByName = new HashMap<>();
        this.daxPath = Parameters.getDaxPath();
        this.daxPaths = Parameters.getDAXPaths();
        this.jobIdStartsFrom = 1;

        setTaskList(new ArrayList<>());
    }

    /**
     * Start to parse a workflow which is a xml file(s).
     */
    public void parse() {
        if (this.daxPath != null) {
            parseXmlFile(this.daxPath);
        } else if (this.daxPaths != null) {
            for (String path : this.daxPaths) {
                parseXmlFile(path);
            }
        }
    }

    /**
     * Sets the depth of a task
     *
     * @param task  the task
     * @param depth the depth
     */
    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task childTask : task.getChildList()) {
            setDepth(childTask, task.getDepth() + 1);
        }
    }

    /**
     * Parse a DAX file with jdom
     */
    public void parseXmlFile(String path) {
        try {
            SAXBuilder saxBuilder = new SAXBuilder();
            // parse using builder to get DOM representation of the XML file
            Document document = saxBuilder.build(new File(path));
            Element rootElement = document.getRootElement();
            List<Element> rootElementChildren = rootElement.getChildren();
            for (Element element : rootElementChildren) {
                switch (element.getName().toLowerCase()) {
                    case "job" -> {
                        long cloudletLength = 0;
                        String nodeName = element.getAttributeValue("id");
                        String nodeType = element.getAttributeValue("name");
                        double runtime;
                        if (element.getAttributeValue("runtime") != null) {
                            String nodeRuntime = element.getAttributeValue("runtime");
                            runtime = 1000 * Double.parseDouble(nodeRuntime);
                            if (runtime < 100) {
                                runtime = 100;
                            }
                            cloudletLength = (long) runtime;
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }
                        cloudletLength *= Parameters.getRuntimeScale();
                        List<Element> fileElements = element.getChildren();
                        List<FileItem> fileItems = new ArrayList<>();
                        for (Element fileElement : fileElements) {
                            if (fileElement.getName().equalsIgnoreCase("uses")) {
                                String fileName = fileElement.getAttributeValue("name");// DAX version 3.3
                                if (fileName == null) {
                                    fileName = fileElement.getAttributeValue("file");// DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }
                                String fileLink = fileElement.getAttributeValue("link");
                                double size = 0.0;
                                String fileSize = fileElement.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /* / 1024 */;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }
                                if (size == 0) {
                                    size++;
                                }
                                FileType type = FileType.NONE;
                                switch (fileLink) {
                                    case "input" -> type = FileType.INPUT;
                                    case "output" -> type = FileType.OUTPUT;
                                    default -> Log.printLine("Parsing Error");
                                }
                                FileItem fileItem;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == FileType.OUTPUT) {
                                    fileItem = new FileItem(fileName, size);
                                } else if (ReplicaCatalog.containsFile(fileName)) {
                                    fileItem = ReplicaCatalog.getFile(fileName);
                                } else {
                                    fileItem = new FileItem(fileName, size);
                                    ReplicaCatalog.setFile(fileName, fileItem);
                                }
                                fileItem.setType(type);
                                fileItems.add(fileItem);
                            }
                        }
                        Task task;
                        // In case of multiple workflow submission. Make sure the jobIdStartsFrom is
                        // consistent.
                        synchronized (this) {
                            task = new Task(this.jobIdStartsFrom, cloudletLength);
                            this.jobIdStartsFrom++;
                        }
                        task.setType(nodeType);
                        task.setUserId(userId);
                        taskByName.put(nodeName, task);
                        for (FileItem fileItem : fileItems) {
                            task.addRequiredFile(fileItem.getName());
                        }
                        task.setFileList(fileItems);
                        this.getTaskList().add(task);
                    }

                    /*
                     * Add dependencies info.
                     */
                    case "child" -> {
                        List<Element> parentNodeElements = element.getChildren();
                        String childName = element.getAttributeValue("ref");
                        if (taskByName.containsKey(childName)) {
                            Task childTask = taskByName.get(childName);
                            for (Element parentNodeElement : parentNodeElements) {
                                String parentName = parentNodeElement.getAttributeValue("ref");
                                if (taskByName.containsKey(parentName)) {
                                    Task parentTask = taskByName.get(parentName);
                                    parentTask.addChild(childTask);
                                    childTask.addParent(parentTask);
                                }
                            }
                        }
                    }
                }
            }
            /*
             * If a task has no parent, then it is root task.
             */
            ArrayList<Task> roots = new ArrayList<>();
            for (Task task : taskByName.values()) {
                task.setDepth(0);
                if (task.getParentList().isEmpty()) {
                    roots.add(task);
                }
            }

            /*
             * Add depth from top to bottom.
             */
            for (Task task : roots) {
                setDepth(task, 1);
            }
            /*
             * Clean them to save memory. Parsing workflow may take much memory
             */
            this.taskByName.clear();

        } catch (JDOMException jdomException) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");
        } catch (IOException ioException) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");
        } catch (Exception exception) {
            exception.printStackTrace();
            Log.printLine("Parsing Exception");
        }
    }
}
