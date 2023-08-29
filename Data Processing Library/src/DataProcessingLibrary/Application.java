package DataProcessingLibrary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Application {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 List<Map<String, Object>> data = new ArrayList<>();
	        // Populate data here
	        
	        List<String> columns = new ArrayList<>();
	        // Populate column names here
	        
	        DataFrame df = new DataFrame(data, columns);

	        df.displayRows(3);
	        df.displayShape();
	        df.displayDataTypes();
	        
	        DataFrame filteredDF = df.filterRows(row -> (Integer)row.get("age") > 25);
	        filteredDF.displayRows(3);

	        DataFrame sortedDF = df.sortByColumn("name");
	        sortedDF.displayRows(3);

	        List<String> groupByCols = List.of("city");
	        DataFrame groupedDF = df.groupByColumns(groupByCols);
	        groupedDF.displayRows(3);

	}

}
