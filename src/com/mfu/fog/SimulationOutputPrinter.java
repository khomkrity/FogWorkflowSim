package com.mfu.fog;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.power.PowerHost;
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
class SimulationOutputPrinter {
    private final Workbook WORKBOOK;
    private final Map<String, Map<String, List<Job>>> schedulingResults;
    private final List<CondorVM> virtualMachines;
    private final List<String> RESULT_HEADER_NAMES = new ArrayList<>(Arrays.asList("Job ID", "Task ID", "Status",
            "Datacenter ID", "VM ID", "Start Time", "Finish Time", "Execution Time", "Depth", "Parent", "Cost"));
    private final double PORT_DELAY;
    private final XSSFFont CONTENT_FONT;
    private final CellStyle HEADER_STYLE;
    private final CellStyle CONTENT_STYLE;

    enum Style {
        HEADER, CONTENT, PORT_CONSTRAINT_ON, PORT_CONSTRAINT_OFF
    }

    public SimulationOutputPrinter(double PORT_DELAY, List<CondorVM> virtualMachines, Map<String, Map<String, List<Job>>> schedulingResults) {
        this.PORT_DELAY = PORT_DELAY;
        this.schedulingResults = schedulingResults;
        this.virtualMachines = virtualMachines;
        WORKBOOK = new XSSFWorkbook();
        XSSFFont HEADER_FONT = createFont(Style.HEADER);
        CONTENT_FONT = createFont(Style.CONTENT);
        HEADER_STYLE = createCellStyle(HEADER_FONT, Style.HEADER);
        CONTENT_STYLE = createCellStyle(CONTENT_FONT, Style.CONTENT);
    }

    public void exportSchedulingResult() throws IOException {
        System.out.println("writing results to an excel file...");
        Sheet environmentSettingSheet = WORKBOOK.createSheet("Environment Setting");
        int totalWidth = 3;
        int columnWidth = 3_500;
        writeEnvironmentSetting(environmentSettingSheet, totalWidth, columnWidth);
        for (Map.Entry<String, Map<String, List<Job>>> schedulingResult : schedulingResults.entrySet()) {
            int startingRowIndex = 0;
            String dagName = schedulingResult.getKey();
            Map<String, List<Job>> algorithmResults = schedulingResult.getValue();
            Sheet schedulingResultSheet = WORKBOOK.createSheet(dagName);
            setColumnWidth(columnWidth, schedulingResultSheet, RESULT_HEADER_NAMES.size());

            for (Map.Entry<String, List<Job>> algorithmResult : algorithmResults.entrySet()) {
                String algorithmName = algorithmResult.getKey();
                List<Job> jobs = algorithmResult.getValue();

                Row algorithmNameRow = schedulingResultSheet.createRow(startingRowIndex);
                writeAlgorithmName(algorithmName, algorithmNameRow);

                Row resultHeaderRow = schedulingResultSheet.createRow(++startingRowIndex);
                writeSchedulingHeader(resultHeaderRow);

                for (int i = 0, currentRow = 1; currentRow <= jobs.size(); currentRow++, i++) {
                    Row outputRow = schedulingResultSheet.createRow(startingRowIndex + currentRow);
                    Job job = jobs.get(i);
                    writeSchedulingResult(outputRow, job, CONTENT_STYLE);
                }
                startingRowIndex += jobs.size() + 2;
            }
        }
        exportExcelFile();
    }

    private XSSFFont createFont(Style style) {
        XSSFFont font = ((XSSFWorkbook) WORKBOOK).createFont();
        font.setFontName("IBM Plex Sans Condensed");
        if (style.equals(Style.HEADER))
            font.setBold(true);
        return font;
    }

    private CellStyle createCellStyle(XSSFFont font, Style style) {
        CellStyle cellStyle = WORKBOOK.createCellStyle();
        cellStyle.setWrapText(true);
        cellStyle.setFont(font);
        setCellFillColor(style, cellStyle);
        setCellAlignment(cellStyle);
        setCellBorder(cellStyle);
        return cellStyle;
    }

    private void setCellFillColor(Style style, CellStyle cellStyle) {
        switch (style) {
            case HEADER -> {
                cellStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
                cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            }
            case CONTENT -> {
            }
            case PORT_CONSTRAINT_ON -> {
                cellStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
                cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            }
            case PORT_CONSTRAINT_OFF -> {
                cellStyle.setFillForegroundColor(IndexedColors.RED.getIndex());
                cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            }
            default -> throw new IllegalStateException("Unexpected value: " + style);
        }
    }

    private void setCellBorder(CellStyle cellStyle) {
        cellStyle.setBorderTop(BorderStyle.THIN);
        cellStyle.setBorderRight(BorderStyle.THIN);
        cellStyle.setBorderBottom(BorderStyle.THIN);
        cellStyle.setBorderLeft(BorderStyle.THIN);
    }

    private void setCellAlignment(CellStyle cellStyle) {
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
    }

    private void setColumnWidth(int columnWidth, Sheet sheet, int numberOfColumn) {
        for (int i = 0; i < numberOfColumn; i++) {
            sheet.setColumnWidth(i, columnWidth);
        }
    }

    private void writeEnvironmentSetting(Sheet inputSheet, int totalWidth, int columnWidth) {
        setColumnWidth(columnWidth, inputSheet, totalWidth);
        int inputConstraintRowIndex = 0;
        int inputHeaderRowIndex = 1;
        writePortConstraint(inputSheet, inputConstraintRowIndex);
        writeInputHeader(inputSheet, inputHeaderRowIndex);

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

    private void writeInputHeader(Sheet inputSheet, int inputHeaderRowIndex) {
        Row inputHeaderRow = inputSheet.createRow(inputHeaderRowIndex);
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
    }

    private void writePortConstraint(Sheet inputSheet, int inputConstraintRowIndex) {
        Row inputConstraintRow = inputSheet.createRow(inputConstraintRowIndex);
        Cell inputConstraintHeaderCell = inputConstraintRow.createCell(0);
        inputConstraintHeaderCell.setCellValue("I/O Port Delay");
        inputConstraintHeaderCell.setCellStyle(HEADER_STYLE);
        Cell inputConstraintCell = inputConstraintRow.createCell(1);
        inputConstraintCell.setCellValue(PORT_DELAY);
        Style portConstraintStyle = PORT_DELAY != 0 ? Style.PORT_CONSTRAINT_ON : Style.PORT_CONSTRAINT_OFF;
        inputConstraintCell.setCellStyle(createCellStyle(CONTENT_FONT, portConstraintStyle));
    }

    private void writeAlgorithmName(String algorithmName, Row algorithmNameRow) {
        Cell algorithmCell = algorithmNameRow.createCell(0);
        algorithmCell.setCellValue("Algorithm");
        algorithmCell.setCellStyle(HEADER_STYLE);
        Cell algorithmNameCell = algorithmNameRow.createCell(1);
        algorithmNameCell.setCellValue(algorithmName);
        algorithmNameCell.setCellStyle(CONTENT_STYLE);
    }

    private void writeSchedulingHeader(Row resultHeaderRow) {
        for (int i = 0; i < RESULT_HEADER_NAMES.size(); i++) {
            Cell headerCell = resultHeaderRow.createCell(i);
            headerCell.setCellValue(RESULT_HEADER_NAMES.get(i));
            headerCell.setCellStyle(HEADER_STYLE);
        }
    }

    private void writeSchedulingResult(Row row, Job job, CellStyle cellStyle) {
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
