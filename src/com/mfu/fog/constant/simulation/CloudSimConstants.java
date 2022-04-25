package com.mfu.fog.constant.simulation;

import java.util.Calendar;

public enum CloudSimConstants {
    DEFAULT();
    public int NUMBER_OF_USER = 1;
    public Calendar CALENDAR_INSTANCE = Calendar.getInstance();
    public boolean TRACE_FLAG = false;
    public String APP_ID = "workflow";
}
