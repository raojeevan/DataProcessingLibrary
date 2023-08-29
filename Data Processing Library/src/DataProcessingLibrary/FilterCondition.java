package DataProcessingLibrary;

import java.util.Map;

public interface FilterCondition {
	boolean test(Map<String, Object> row);
}
