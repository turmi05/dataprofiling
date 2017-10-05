package dataprofiling;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

public class OracleColumnSampler implements ColumnSampler {

    @Override
    public List<String> sample(DataSource dataSource, TableInfo table, String columnName, int sampleSize) {
        // Get some sample values (just an example)
        JdbcTemplate template = new JdbcTemplate(dataSource);
        String sql = String.format("SELECT DISTINCT %s FROM %s.%s WHERE %s IS NOT NULL AND ROWNUM <= %d", columnName, table.getSchemaName(), table.getTableName(), columnName, sampleSize);
        List<String> samples = null;
        
        try {
            samples = template.queryForList(sql, String.class);
        } catch (Exception e) {
            // Note that not all CHAR types are compatible with DISTINCT. For those don't show sample values.
        };
        
        return samples;
    }

}
