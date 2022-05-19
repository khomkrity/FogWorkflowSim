package test.com.mfu.fog;

import com.mfu.fog.UserInput;
import com.mfu.fog.constant.device.CloudConstants;
import com.mfu.fog.constant.device.EndDeviceConstants;
import com.mfu.fog.constant.device.FogDeviceConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

class UserInputTest {
    private final CloudConstants cloudConstants = CloudConstants.DEFAULT;
    private final FogDeviceConstants fogDeviceConstants = FogDeviceConstants.DEFAULT;
    private final EndDeviceConstants endDeviceConstants = EndDeviceConstants.DEFAULT;
    private final UserInput userInput = new UserInput();
    private final String emptyInputPath = "src/test/com/mfu/fog/empty";
    private final String dagInputPath = "src/test/com/mfu/fog/dax";
    private final int numberOfCloud = 1;
    private final long cloudMips = 1_600;
    private final double cloudProcessingCost = 1;
    private final int numberOfFog = 1;
    private final long fogMips = 1_300;
    private final double fogProcessingCost = 1;
    private final int numberOfMobile = 1;
    private final long mobileMips = 1_000;
    private final double mobileProcessingCost = 1;
    private final double portDelay = 1;
    private final byte[] validInputBytes = (numberOfCloud + System.lineSeparator()
            + cloudMips + System.lineSeparator()
            + cloudProcessingCost + System.lineSeparator()
            + numberOfFog + System.lineSeparator()
            + fogMips + System.lineSeparator()
            + fogProcessingCost + System.lineSeparator()
            + numberOfMobile + System.lineSeparator()
            + mobileMips + System.lineSeparator()
            + mobileProcessingCost + System.lineSeparator()
            + portDelay + System.lineSeparator()).getBytes();

    @Nested
    @DisplayName(value = "Given valid input format")
    class GivenValidInput {

        @BeforeEach
        @DisplayName(value = "Type in valid input")
        void init() {
            ByteArrayInputStream input = new ByteArrayInputStream(validInputBytes);
            System.setIn(input);
            Scanner scanner = new Scanner(input);
            assertDoesNotThrow(() -> userInput.readSimulationInput(scanner));
        }

        @Test
        @DisplayName(value = "Return correct values as input")
        void Should_Has_Correct_Amount_Of_Input() {
            Map<String, List<Long>> hostMips = userInput.getHostMips();
            Map<String, List<Double>> hostCosts = userInput.getHostCosts();
            assertEquals(numberOfCloud, userInput.getNumberOfCloud());
            assertEquals(numberOfFog, userInput.getNumberOfFog());
            assertEquals(numberOfMobile, userInput.getNumberOfMobile());
            assertEquals(cloudMips, hostMips.get(cloudConstants.HOST_NAME).get(0));
            assertEquals(fogMips, hostMips.get(fogDeviceConstants.HOST_NAME).get(0));
            assertEquals(mobileMips, hostMips.get(endDeviceConstants.HOST_NAME).get(0));
            assertEquals(cloudProcessingCost, hostCosts.get(cloudConstants.HOST_NAME).get(0));
            assertEquals(fogProcessingCost, hostCosts.get(fogDeviceConstants.HOST_NAME).get(0));
            assertEquals(mobileProcessingCost, hostCosts.get(endDeviceConstants.HOST_NAME).get(0));
            assertEquals(portDelay, UserInput.getPortDelay());
        }

        @Test
        @DisplayName(value = "Throw FileNotFound exception when the input folder is empty")
        void Should_Throw_FileNotFoundException_When_Given_Empty_Folder() {
            assertThrows(FileNotFoundException.class, () -> userInput.readDagPaths(emptyInputPath));
        }

        @Test
        @DisplayName(value = "Return only valid DAG paths that have .xml extension")
        void Should_Return_Only_Valid_Format_Files_When_Given_DAG_Folder() {
            assertDoesNotThrow(() -> userInput.readDagPaths(dagInputPath));
            String expectedExtension = ".xml";
            for (String actualPath : userInput.getDagPaths()) {
                String actualExtension = actualPath.substring(actualPath.lastIndexOf("."));
                assertEquals(expectedExtension, actualExtension);
            }
        }
    }

    @Nested
    @DisplayName(value = "Given wrong input format")
    class GivenInvalidInput {
        @Test
        @DisplayName(value = "Throw NumberFormat exception after alphabetic characters input")
        void Should_Throw_NumberFormatException_When_Given_Alphabets() {
            ByteArrayInputStream input = new ByteArrayInputStream("hello".getBytes());
            System.setIn(input);
            Scanner scanner = new Scanner(input);
            assertThrows(NumberFormatException.class, () -> userInput.readSimulationInput(scanner));
        }
    }
}