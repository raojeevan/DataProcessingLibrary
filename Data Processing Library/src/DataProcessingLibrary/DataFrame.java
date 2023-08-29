package DataProcessingLibrary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class DataFrame {

	private List<Map<String, Object>> data;
	private List<String> columns;

	public DataFrame(List<Map<String, Object>> data, List<String> columns) {
		this.data = data;
		this.columns = columns;
	}

	public void displayRows(int n) {
		int rowsToDisplay = Math.min(n, data.size());
		for (int i = 0; i < rowsToDisplay; i++) {
			System.out.println(data.get(i));
		}
	}

	public void displayShape() {
		System.out.println("Rows: " + data.size() + ", Columns: " + columns.size());
	}

	public void displayDataTypes() {
		for (String column : columns) {
			System.out.println(column + ": " + inferDataType(column));
		}
	}

	private String inferDataType(String column) {
		// Simplified logic to infer data type
		// You can enhance this based on actual data type detection
		return "String";
	}

	public void renameColumn(String oldName, String newName) {
		int columnIndex = columns.indexOf(oldName);
		if (columnIndex != -1) {
			columns.set(columnIndex, newName);
		}
	}

	public void dropColumn(String columnName) {
		int columnIndex = columns.indexOf(columnName);
		if (columnIndex != -1) {
			columns.remove(columnIndex);
			for (Map<String, Object> row : data) {
				row.remove(columnName);
			}
		}
	}

	public DataFrame selectColumns(List<String> selectedColumns) {
		List<Map<String, Object>> selectedData = new ArrayList<>();
		for (Map<String, Object> row : data) {
			Map<String, Object> newRow = new HashMap<>();
			for (String column : selectedColumns) {
				if (row.containsKey(column)) {
					newRow.put(column, row.get(column));
				}
			}
			selectedData.add(newRow);
		}
		return new DataFrame(selectedData, selectedColumns);
	}

	public DataFrame filterRows(FilterCondition condition) {
		List<Map<String, Object>> filteredData = data.stream().filter(row -> condition.test(row))
				.collect(Collectors.toList());
		return new DataFrame(filteredData, columns);
	}

	@SuppressWarnings("unchecked")
	public DataFrame sortByColumn(String columnName) {
		List<Map<String, Object>> sortedData = data.stream().sorted((row1, row2) -> {
			Comparable<Object> value1 = (Comparable<Object>) row1.get(columnName);
			Comparable<Object> value2 = (Comparable<Object>) row2.get(columnName);
			return value1.compareTo(value2);
		}).collect(Collectors.toList());
		return new DataFrame(sortedData, columns);
	}

	public DataFrame groupByColumns(List<String> groupByColumns) {
		Map<List<Object>, List<Map<String, Object>>> groupedData = new HashMap<>();
		for (Map<String, Object> row : data) {
			List<Object> groupKey = groupByColumns.stream().map(row::get).collect(Collectors.toList());

			groupedData.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
		}

		List<Map<String, Object>> aggregatedData = new ArrayList<>();
		for (Map.Entry<List<Object>, List<Map<String, Object>>> entry : groupedData.entrySet()) {
			Map<String, Object> aggregatedRow = new HashMap<>();
			for (String column : columns) {
				if (!groupByColumns.contains(column)) {
					// Aggregation logic for non-grouping columns
					if (column.equals("count")) {
						aggregatedRow.put(column, entry.getValue().size());
					} else if (column.equals("sum")) {
						// ...
					} else if (column.equals("mean")) {
						if (isNumericColumn(column)) {
							double mean = calculateMean(column);
							aggregatedRow.put(column, mean);
						} else {
							aggregatedRow.put(column, null); // or some appropriate value
						}
					} else if (column.equals("median")) {
						if (isNumericColumn(column)) {
							Object median = calculateMedian(column);
							aggregatedRow.put(column, median);
						} else {
							aggregatedRow.put(column, null); // or some appropriate value
						}
					} else if (column.equals("mode")) {
						if (isNumericColumn(column)) {
							Object mode = calculateMode(column);
							aggregatedRow.put(column, mode);
						} else {
							aggregatedRow.put(column, null); // or some appropriate value
						}
					}
				} else {
					aggregatedRow.put(column, entry.getKey().get(groupByColumns.indexOf(column)));
				}
			}
			aggregatedData.add(aggregatedRow);
		}

		return new DataFrame(aggregatedData, columns);
	}

	public double calculateMean(String columnName) {
		double[] values = data.stream().map(row -> row.get(columnName)).filter(Objects::nonNull) // Exclude null values
				.mapToDouble(value -> ((Number) value).doubleValue()).toArray();

		double sum = Arrays.stream(values).sum();
		int count = values.length;

		if (count > 0) {
			return sum / count;
		} else {
			return 0.0; // Handle case where no valid values are present
		}
	}

	public Object calculateMedian(String columnName) {
		List<Object> values = data.stream().map(row -> row.get(columnName)).filter(Objects::nonNull) // Exclude null
																										// values
				.sorted(Comparator.comparingInt(value -> ((Number) value).intValue())).collect(Collectors.toList());

		int count = values.size();

		if (count == 0) {
			return null; // Handle case where no valid values are present
		}

		if (count % 2 == 0) {
			int midIndex1 = count / 2 - 1;
			int midIndex2 = count / 2;
			double midValue1 = ((Number) values.get(midIndex1)).doubleValue();
			double midValue2 = ((Number) values.get(midIndex2)).doubleValue();
			return (midValue1 + midValue2) / 2;
		} else {
			int midIndex = count / 2;
			return ((Number) values.get(midIndex)).doubleValue();
		}
	}

	public Object calculateMode(String columnName) {
		Map<Object, Integer> valueFrequency = new HashMap<>();

		data.stream().map(row -> row.get(columnName)).filter(Objects::nonNull) // Exclude null values
				.forEach(value -> valueFrequency.merge(value, 1, Integer::sum));

		int maxFrequency = 0;
		Object mode = null;

		for (Map.Entry<Object, Integer> entry : valueFrequency.entrySet()) {
			if (entry.getValue() > maxFrequency) {
				maxFrequency = entry.getValue();
				mode = entry.getKey();
			}
		}

		return mode;
	}

	private boolean isNumericColumn(String column) {
		List<Class<?>> numericTypes = Arrays.asList(Integer.class, Long.class, Float.class, Double.class
		// Add more numeric types if necessary
		);

		if (data.isEmpty()) {
			return false;
		}

		Object sampleValue = data.get(0).get(column);

		return numericTypes.contains(sampleValue.getClass());
	}

	public static DataFrame readCSV(String filename) throws IOException {
		List<Map<String, Object>> data = new ArrayList<>();
		List<String> columns = new ArrayList<>();

		try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
			// Read the first line to get column names
			String headerLine = reader.readLine();
			if (headerLine != null) {
				columns = Arrays.asList(headerLine.split(","));
			}

			// Read data rows
			String line;
			while ((line = reader.readLine()) != null) {
				String[] values = line.split(",");
				if (values.length == columns.size()) {
					Map<String, Object> rowData = IntStream.range(0, columns.size()).boxed()
							.collect(Collectors.toMap(columns::get, i -> parseValue(values[i])));
					data.add(rowData);
				}
			}
		}

		return new DataFrame(data, columns);
	}

	private static Object parseValue(String value) {
		try {
			if (value.contains(".")) {
				// Attempt to parse as double
				return Double.parseDouble(value);
			} else {
				// Attempt to parse as integer
				return Integer.parseInt(value);
			}
		} catch (NumberFormatException e) {
			// If parsing as number fails, return the value as string
			return value;
		}
	}

	public void writeCSV(String filename) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
			// Write column headers
			writer.write(String.join(",", columns));
			writer.newLine();

			// Write data rows
			for (Map<String, Object> row : data) {
				List<String> values = columns.stream().map(column -> String.valueOf(row.get(column))).toList();
				writer.write(String.join(",", values));
				writer.newLine();
			}
		}
	}

	public static DataFrame readJSON(String filename) throws IOException {
		List<Map<String, Object>> data = new ArrayList<>();
		List<String> columns = new ArrayList<>();

		try (FileReader fileReader = new FileReader(filename)) {
			JSONTokener tokener = new JSONTokener(fileReader);
			JSONArray jsonArray = new JSONArray(tokener);

			if (jsonArray.length() > 0) {
				JSONObject firstObject = jsonArray.getJSONObject(0);
				columns.addAll(firstObject.keySet());
			}

			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(i);
				Map<String, Object> rowData = new HashMap<>();
				for (String column : columns) {
					if (jsonObject.has(column)) {
						rowData.put(column, jsonObject.get(column));
					} else {
						rowData.put(column, null);
					}
				}
				data.add(rowData);
			}
		}

		return new DataFrame(data, columns);
	}

	public void writeJSON(String filename) throws IOException {
		JSONArray jsonArray = new JSONArray();

		for (Map<String, Object> row : data) {
			JSONObject jsonObject = new JSONObject(row);
			jsonArray.put(jsonObject);
		}

		String jsonString = jsonArray.toString(4); // Indentation of 4 spaces

		try (FileWriter fileWriter = new FileWriter(filename)) {
			fileWriter.write(jsonString);
		}
	}

}
