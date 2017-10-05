package dataprofiling;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.metadata.TableMetaDataContext;
import org.springframework.jdbc.core.metadata.TableMetaDataProvider;
import org.springframework.jdbc.core.metadata.TableMetaDataProviderFactory;
import org.springframework.jdbc.core.metadata.TableParameterMetaData;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

public class Main {
    
    private static Map<Integer, String> sqlTypes = new HashMap<>();
    
    static {
        Field[] types = Types.class.getFields();
        for (int i = 0; i < types.length; i++) {
            try {
                String name = types[i].getName();
                Integer value = (Integer) types[i].get(null);
                sqlTypes.put(value, name);
            } catch (IllegalAccessException e) {
            }
        }
    }
    
    public static void main(String[] args) throws MetaDataAccessException {
        
        String server = "";
        String user = "";
        String password = "";
        
        Set<String> schemaIncludes = new HashSet<>(); // Empty means include all except for the excluded one

        Set<String> schemaExcludes = new HashSet<>();
        schemaExcludes.addAll(Arrays.asList("sys"));
        
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl("jdbc:sqlserver://" + server + ":1433;DatabaseName=AdventureWorks2008R2");
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        
        System.out.println("Database Product Name: " + JdbcUtils.extractDatabaseMetaData(dataSource, "getDatabaseProductName"));
        System.out.println("Database Product Version: " + JdbcUtils.extractDatabaseMetaData(dataSource, "getDatabaseProductVersion"));
        System.out.println("\n");

        final ArrayList<TableMeta> tables = new ArrayList<>();
 
        // Extract table information for 
        JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {

            @Override
            public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                ResultSet rs = dbmd.getTables(dataSource.getCatalog(), null, null, new String[]{"TABLE"});
//                ResultSetMetaData meta = rs.getMetaData();
//                for (int i = 1; i <= meta.getColumnCount(); i++) {
//                    System.out.println(meta.getColumnName(i));
//                }
                
                while (rs.next()) {
                    String catalog = rs.getString("TABLE_CAT");
                    String schema = rs.getString("TABLE_SCHEM");
                    String name = rs.getString("TABLE_NAME");
                    tables.add(new TableMeta(catalog, schema, name));
                }
                
                return null;
            }
        });
        
        for (TableMeta table : tables) {
            if (!schemaIncludes.isEmpty() && !schemaIncludes.contains(table.schemaName)
                    || schemaExcludes.contains(table.schemaName)) {
                continue;
            }
            TableMetaDataContext context = new TableMetaDataContext();
            context.setAccessTableColumnMetaData(true);
            context.setCatalogName(dataSource.getCatalog());
            context.setSchemaName(table.schemaName);
            context.setTableName(table.tableName);
            
            System.out.println(String.format("%s.[%s]", table.schemaName, table.tableName));
            
            TableMetaDataProvider provider = TableMetaDataProviderFactory.createMetaDataProvider(dataSource, context);
            
            List<TableParameterMetaData> parameters = provider.getTableParameterMetaData();
            for (TableParameterMetaData parameter : parameters) {
                String sqlType = sqlTypes.getOrDefault(parameter.getSqlType(), "UNKNOWN");
                String columnName = parameter.getParameterName();
                String sampleList = "";
                if (sqlType.endsWith("CHAR")) {
                    // Get some sample values (just an example)
                    JdbcTemplate template = new JdbcTemplate(dataSource);
                    String sql = String.format("SELECT DISTINCT TOP (11) [%s] FROM %s.%s WHERE [%s] IS NOT NULL", columnName, table.schemaName, table.tableName, columnName);
                    List<String> samples = null;
                    
                    try {
                        samples = template.queryForList(sql, String.class);
                    } catch (Exception e) {
                        // Note that not all CHAR types are compatible with DISTINCT. For those don't show sample values.
                    };
                    
                    sampleList = toSampleList(samples);
                }
                System.out.println(String.format("  %s (%s%s) %s", columnName, sqlType, parameter.isNullable() ? ", null" : "", sampleList));
            }
        }
        
    }
    
    private static String toSampleList(List<String> list) {
        StringBuffer sampleList = new StringBuffer();
        if (list != null) {
            sampleList.append("{");
            boolean hasItem = false;
            for (String item : list) {
                if (hasItem) {
                    sampleList.append(", ");
                } else {
                    hasItem = true;
                }
                sampleList.append("'");
                sampleList.append(StringUtils.abbreviate(item, "[..]", 20));
                sampleList.append("'");
            }
            if (list.size() > 10) {
                sampleList.append(", ...");
            }
            sampleList.append("}");
        }
        return sampleList.toString();
    }
    
}

class TableMeta {
    String catalogName; // May be null
    String schemaName;
    String tableName;
    
    TableMeta(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }
}

