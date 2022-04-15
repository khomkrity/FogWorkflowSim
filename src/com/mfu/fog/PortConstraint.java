package com.mfu.fog;

public class PortConstraint {
    private static double portDelay;

    PortConstraint(double portDelay) {
        PortConstraint.portDelay = portDelay;
    }

    public static double getPortDelay() {
        return portDelay;
    }

    public static boolean hasPortDelay() {
        return portDelay != 0;
    }

}
