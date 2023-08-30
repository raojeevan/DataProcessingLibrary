package DataProcessingLibraryTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import DataProcessingLibrary.DataFrame;

public class DataFrameTest {

	private DataFrame dataFrame;

	@BeforeEach
	public void setUp() {
		// Initialize your DataFrame for testing
		List<Map<String, Object>> testData = new ArrayList<>();
		Map<String, Object> row1 = new HashMap<>();
		row1.put("numericColumn", 10);
		testData.add(row1);

		Map<String, Object> row2 = new HashMap<>();
		row2.put("numericColumn", 20);
		testData.add(row2);

		// Initialize dataFrame with test data and columns
		List<String> columns = Arrays.asList("numericColumn");
		dataFrame = new DataFrame(testData, columns);
	}

	@Test
	public void testCalculateMean() {
		double mean = dataFrame.calculateMean("numericColumn");
		assertEquals(15.0, mean, 0.001);
	}

	@Test
	public void testCalculateMedian() {
		Object median = dataFrame.calculateMedian("numericColumn");
		assertEquals(15, median);
	}

	@Test
	public void testCalculateStandardDeviation() {
		double stdDev = dataFrame.calculateStandardDeviation("numericColumn");
		assertEquals(7.071, stdDev, 0.001);
	}

	@Test
	public void testReadAndWriteCSV() throws IOException {
		// Write the DataFrame to a CSV file
		String csvFileName = "test.csv";
		dataFrame.writeCSV(csvFileName);

		// Read the DataFrame from the CSV file
		DataFrame readDataFrame = DataFrame.readCSV(csvFileName);

		// Assert that the read DataFrame has the same content as the original DataFrame
		assertEquals(dataFrame.getRowCount(), readDataFrame.getRowCount());
		assertEquals(dataFrame.getColumnCount(), readDataFrame.getColumnCount());
		// Add more assertions based on your implementation
	}

	@Test
	public void testReadAndWriteJSON() throws IOException {
		// Write the DataFrame to a JSON file
		String jsonFileName = "test.json";
		dataFrame.writeJSON(jsonFileName);

		// Read the DataFrame from the JSON file
		DataFrame readDataFrame = DataFrame.readJSON(jsonFileName);

		// Assert that the read DataFrame has the same content as the original DataFrame
		assertEquals(dataFrame.getRowCount(), readDataFrame.getRowCount());
		assertEquals(dataFrame.getColumnCount(), readDataFrame.getColumnCount());
		// Add more assertions based on your implementation
	}

	@Test
	public void testGetRowCount() {
		int rowCount = dataFrame.getRowCount();
		assertEquals(2, rowCount);
	}

	@Test
	public void testGetColumnCount() {
		int columnCount = dataFrame.getColumnCount();
		assertEquals(2, columnCount);
	}

}
