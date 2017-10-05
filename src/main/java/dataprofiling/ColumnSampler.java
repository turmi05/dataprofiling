package dataprofiling;

import java.util.List;

import javax.sql.DataSource;

public interface ColumnSampler {
    
    List<String> sample(DataSource dataSource, TableInfo table, String columnName, int sampleSize);

}
