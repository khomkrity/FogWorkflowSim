package com.mfu.fog;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.power.PowerHost;
import org.jetbrains.annotations.NotNull;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters.ClassType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

//TODO: add draw chart UI method
public class SchedulingResult {
    private final Workbook WORKBOOK;
    private final Map<String, Map<String, List<Job>>> schedulingResults;
    private final List<CondorVM> virtualMachines;
    private final List<String> RESULT_HEADER_NAMES = new ArrayList<>(Arrays.asList("Job ID", "Task ID", "Status",
            "Datacenter ID", "VM ID", "Start Time", "Finish Time", "Execution Time", "Depth", "Parent", "Cost"));
    private final double PORT_DELAY = PortConstraint.getPortDelay();
    private final CellStyle HEADER_STYLE;
    private final CellStyle CONTENT_STYLE;

    public SchedulingResult(Map<String, Map<String, List<Job>>> schedulingResults, List<CondorVM> virtualMachines) {
        this.schedulingResults = schedulingResults;
        this.virtualMachines = virtualMachines;
        WORKBOOK = new XSSFWorkbook();
        XSSFFont headerFont = createFont(WORKBOOK, true);
        XSSFFont contentFont = createFont(WORKBOOK, false);
        HEADER_STYLE = createCellStyle(WORKBOOK, headerFont, true);
        CONTENT_STYLE = createCellStyle(WORKBOOK, contentFont, false);
    }

    public void exportResult() throws IOException {
        System.out.println("writing results to an excel file...");
        Sheet inputSheet = WORKBOOK.createSheet("Environment Setting");
        int totalWidth = 3;
        int COLUMN_WIDTH = 3_500;
        writeEnvironmentSetting(inputSheet, totalWidth, COLUMN_WIDTH);

        for (Map.Entry<String, Map<String, List<Job>>> schedulingResult : schedulingResults.entrySet()) {
            int startingRowIndex = 0;
            String dagName = schedulingResult.getKey();
            Map<String, List<Job>> algorithmResults = schedulingResult.getValue();
            Sheet sheet = WORKBOOK.createSheet(dagName);
            setColumnWidth(COLUMN_WIDTH, sheet, RESULT_HEADER_NAMES.size());

            for (Map.Entry<String, List<Job>> algorithmResult : algorithmResults.entrySet()) {
                String algorithmName = algorithmResult.getKey();
                List<Job> jobs = algorithmResult.getValue();

                Row algorithmNameRow = sheet.createRow(startingRowIndex);
                writeAlgorithmName(algorithmName, algorithmNameRow);

                Row resultHeaderRow = sheet.createRow(++startingRowIndex);
                writeSchedulingHeader(resultHeaderRow);

                for (int i = 0, currentRow = 1; currentRow <= jobs.size(); currentRow++, i++) {
                    Row outputRow = sheet.createRow(startingRowIndex + currentRow);
                    Job job = jobs.get(i);
                    writeSchedulingResult(outputRow, job, CONTENT_STYLE);
                }
                startingRowIndex += jobs.size() + 2;
            }
        }
        exportExcelFile();
    }

    private @NotNull XSSFFont createFont(Workbook workbook, boolean isHeaderStyle) {
        XSSFFont font = ((XSSFWorkbook) workbook).createFont();
        font.setFontName("IBM Plex Sans Condensed");
        if (isHeaderStyle)
            font.setBold(true);
        return font;
    }

    private @NotNull CellStyle createCellStyle(@NotNull Workbook workbook, XSSFFont font, boolean isHeaderStyle) {
        CellStyle cellStyle = workbook.createCellStyle();

        cellStyle.setWrapText(true);
        cellStyle.setFont(font);
        if (isHeaderStyle) {
            cellStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        }
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        cellStyle.setBorderTop(BorderStyle.THIN);
        cellStyle.setBorderRight(BorderStyle.THIN);
        cellStyle.setBorderBottom(BorderStyle.THIN);
        cellStyle.setBorderLeft(BorderStyle.THIN);

        return cellStyle;
    }

    private void setColumnWidth(int columnWidth, @NotNull Sheet sheet, int numberOfColumn) {
        for (int i = 0; i < numberOfColumn; i++) {
            sheet.setColumnWidth(i, columnWidth);
        }
    }

    private void writeEnvironmentSetting(@NotNull Sheet inputSheet, int totalWidth, int COLUMN_WIDTH) {
        setColumnWidth(COLUMN_WIDTH, inputSheet, totalWidth);
        Row inputConstraintRow = inputSheet.createRow(0);
        Cell inputConstraintHeaderCell = inputConstraintRow.createCell(0);
        inputConstraintHeaderCell.setCellValue("I/O Port Delay");
        inputConstraintHeaderCell.setCellStyle(HEADER_STYLE);
        Cell inputConstraintCell = inputConstraintRow.createCell(1);
        inputConstraintCell.setCellValue(PORT_DELAY);
        inputConstraintCell.setCellStyle(CONTENT_STYLE);

        Row inputHeaderRow = inputSheet.createRow(1);
        Cell hostCell = inputHeaderRow.createCell(0);
        hostCell.setCellValue("Host");
        hostCell.setCellStyle(HEADER_STYLE);
        Cell vmIdCell = inputHeaderRow.createCell(1);
        vmIdCell.setCellValue("VM ID");
        vmIdCell.setCellStyle(HEADER_STYLE);
        Cell specCell = inputHeaderRow.createCell(2);
        specCell.setCellValue("Spec");
        specCell.setCellStyle(HEADER_STYLE);
        Cell specEmptyCell = inputHeaderRow.createCell(3);
        specEmptyCell.setCellStyle(HEADER_STYLE);
        CellRangeAddress specCellRangeAddress = new CellRangeAddress(inputHeaderRow.getRowNum(),
                inputHeaderRow.getRowNum(), specCell.getColumnIndex(), specEmptyCell.getColumnIndex());
        inputSheet.addMergedRegion(specCellRangeAddress);

        for (int row = 2, i = 0; i < virtualMachines.size(); row += 2, i++) {
            CondorVM virtualMachine = virtualMachines.get(i);
            Row inputContentRow = inputSheet.createRow(row);
            int nextRow = row + 1;
            int firstColumn = 0, lastColumn = 0;

            Cell inputHostCell = inputContentRow.createCell(0);
            inputHostCell.setCellValue(virtualMachine.getHost().getDatacenter().getName());
            inputHostCell.setCellStyle(CONTENT_STYLE);

            Cell inputVmIdCell = inputContentRow.createCell(1);
            inputVmIdCell.setCellValue(virtualMachine.getId());
            inputVmIdCell.setCellStyle(CONTENT_STYLE);

            Cell inputMipsHeaderCell = inputContentRow.createCell(2);
            inputMipsHeaderCell.setCellValue("MIPS");
            inputMipsHeaderCell.setCellStyle(HEADER_STYLE);

            Cell inputMipsContentCell = inputContentRow.createCell(3);
            inputMipsContentCell.setCellValue(virtualMachine.getMips());
            inputMipsContentCell.setCellStyle(CONTENT_STYLE);

            Row nextInputContentRow = inputSheet.createRow(nextRow);
            Cell inputHostEmptyCell = nextInputContentRow.createCell(0);
            inputHostEmptyCell.setCellStyle(CONTENT_STYLE);
            Cell inputVmIdEmptyCell = nextInputContentRow.createCell(1);
            inputVmIdEmptyCell.setCellStyle(CONTENT_STYLE);

            Cell inputCostHeaderCell = nextInputContentRow.createCell(2);
            inputCostHeaderCell.setCellValue("Cost Per MIPS");
            inputCostHeaderCell.setCellStyle(HEADER_STYLE);

            Cell inputCostContentCell = nextInputContentRow.createCell(3);
            PowerHost powerHost = (PowerHost) virtualMachine.getHost();
            inputCostContentCell.setCellValue(powerHost.getcostPerMips());
            inputCostContentCell.setCellStyle(CONTENT_STYLE);

            CellRangeAddress hostCellRangeAddress = new CellRangeAddress(row, nextRow, firstColumn, lastColumn);
            CellRangeAddress vmCellRangeAddress = new CellRangeAddress(row, nextRow, firstColumn + 1, lastColumn + 1);
            inputSheet.addMergedRegion(hostCellRangeAddress);
            inputSheet.addMergedRegion(vmCellRangeAddress);
        }
    }

    private void writeAlgorithmName(String algorithmName, @NotNull Row algorithmNameRow) {
        Cell algorithmCell = algorithmNameRow.createCell(0);
        algorithmCell.setCellValue("Algorithm");
        algorithmCell.setCellStyle(HEADER_STYLE);
        Cell algorithmNameCell = algorithmNameRow.createCell(1);
        algorithmNameCell.setCellValue(algorithmName);
        algorithmNameCell.setCellStyle(CONTENT_STYLE);
    }

    private void writeSchedulingHeader(@NotNull Row resultHeaderRow) {
        for (int i = 0; i < RESULT_HEADER_NAMES.size(); i++) {
            Cell headerCell = resultHeaderRow.createCell(i);
            headerCell.setCellValue(RESULT_HEADER_NAMES.get(i));
            headerCell.setCellStyle(HEADER_STYLE);
        }
    }

    private void writeSchedulingResult(@NotNull Row row, @NotNull Job job, @NotNull CellStyle cellStyle) {
        int jobId = job.getCloudletId();
        int taskId = job.getTaskList().get(0).getCloudletId();
        int resourceId = job.getResourceId();
        String jobStatus = job.getCloudletStatusString();
        String resourceName = job.getResourceName(resourceId);
        int vmId = job.getVmId();
        double startTime = job.getExecStartTime();
        double finishTime = job.getTaskFinishTime();
        double executionTime = finishTime - startTime;
        int depth = job.getDepth();
        double processingCost = job.getProcessingCost();

        Cell jobIdCell = row.createCell(0);
        jobIdCell.setCellValue(jobId);
        jobIdCell.setCellStyle(cellStyle);

        Cell taskIdCell = row.createCell(1);
        taskIdCell.setCellValue(taskId);
        taskIdCell.setCellStyle(cellStyle);

        Cell statusCell = row.createCell(2);
        statusCell.setCellValue(jobStatus);
        statusCell.setCellStyle(cellStyle);

        Cell datacenterCell = row.createCell(3);
        datacenterCell.setCellValue(resourceName);
        datacenterCell.setCellStyle(cellStyle);

        Cell vmIdCell = row.createCell(4);
        vmIdCell.setCellValue(vmId);
        vmIdCell.setCellStyle(cellStyle);

        Cell startTimeCell = row.createCell(5);
        startTimeCell.setCellValue(startTime);
        startTimeCell.setCellStyle(cellStyle);

        Cell finishTimeCell = row.createCell(6);
        finishTimeCell.setCellValue(finishTime);
        finishTimeCell.setCellStyle(cellStyle);

        Cell executionTimeCell = row.createCell(7);
        executionTimeCell.setCellValue(executionTime);
        executionTimeCell.setCellStyle(cellStyle);

        Cell depthCell = row.createCell(8);
        depthCell.setCellValue(depth);
        depthCell.setCellStyle(cellStyle);

        List<Task> parentTasks = job.getParentList();
        StringBuilder parent = new StringBuilder();
        for (Task parentTask : parentTasks) {
            int parentTaskId = parentTask.getCloudletId();
            parent.append(parentTaskId).append(",");
        }
        if (parent.length() == 0)
            parent = new StringBuilder("-");
        Cell parentCell = row.createCell(9);
        parentCell.setCellValue(parent.toString());
        parentCell.setCellStyle(cellStyle);

        Cell costCell = row.createCell(10);
        costCell.setCellValue(processingCost);
        costCell.setCellStyle(cellStyle);
    }

    private void exportExcelFile() throws IOException {
        long currentTime = System.currentTimeMillis();
        String fileName = "result-" + currentTime + ".xlsx";
        File file = new File("results/scheduling-results/" + fileName);
        String fileAbsolutePath = file.getAbsolutePath();

        FileOutputStream outputStream = new FileOutputStream(fileAbsolutePath);

        WORKBOOK.write(outputStream);
        WORKBOOK.close();

        System.out.println("complete export: " + file.getAbsolutePath());
    }

    public void printSchedulingResults() {
        Formatter formatter = new Formatter(System.out);
        String indent = "    ";
        for (Map.Entry<String, Map<String, List<Job>>> schedulingResult : schedulingResults.entrySet()) {
            String dagName = schedulingResult.getKey();
            Map<String, List<Job>> algorithmResults = schedulingResult.getValue();
            for (Map.Entry<String, List<Job>> algorithmResult : algorithmResults.entrySet()) {
                String algorithmName = algorithmResult.getKey();
                List<Job> jobs = algorithmResult.getValue();
                Log.printLine();
                Log.printLine("=================== OUTPUT ===================");
                Log.printLine("DAG: " + dagName);
                Log.printLine("Algorithm: " + algorithmName);
                formatter.format("%-8s\t%-12s\t%-8s\t%-17s\t%-10s\t%-8s\t%-12s\t%-13s\t%-10s\t%-10s\n", "Job ID",
                        "Task ID", "STATUS", "Data center ID", "VM ID", "Time", "Start Time", "Finish Time", "Depth",
                        "Cost");
                DecimalFormat decimalFormatter = new DecimalFormat("###.###");
                for (Job job : jobs) {
                    int jobId = job.getCloudletId();
                    int resourceId = job.getResourceId();
                    String resourceName = job.getResourceName(resourceId);
                    int vmId = job.getVmId();
                    double startTime = job.getExecStartTime();
                    double finishTime = job.getTaskFinishTime();
                    double executionTime = finishTime - startTime;
                    int depth = job.getDepth();
                    double processingCost = job.getProcessingCost();
                    formatter.format("  %-8d\t", jobId);

                    if (job.getClassType() == ClassType.STAGE_IN.value) {
                        formatter.format("%-10s\t", "Stage-in");
                    }

                    for (Task task : job.getTaskList()) {
                        int taskId = task.getCloudletId();
                        formatter.format("%-10d\t", taskId);
                    }

                    if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                        formatter.format(" SUCCESS\t%-16s\t%-9d\t%-10.2f\t%-12.2f\t%-13.2f\t%-8d\t%-12.2f\t",
                                resourceName, vmId, executionTime,
                                startTime, finishTime, depth, processingCost);

                        List<Task> tasks = job.getParentList();
                        for (Task task : tasks) {
                            int taskId = task.getCloudletId();
                            System.out.print(taskId + ",");
                        }

                        System.out.println();

                    } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                        Log.print("FAILED");
                        Log.printLine(indent + indent + resourceId + indent + indent + indent + vmId
                                + indent + indent + indent + decimalFormatter.format(executionTime) + indent
                                + indent + decimalFormatter.format(startTime) + indent + indent + indent
                                + decimalFormatter.format(finishTime) + indent + indent + indent
                                + depth);
                    }
                }
            }
        }
        formatter.close();
    }
}
