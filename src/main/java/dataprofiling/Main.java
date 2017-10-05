package dataprofiling;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.metadata.TableMetaDataContext;
import org.springframework.jdbc.core.metadata.TableMetaDataProvider;
import org.springframework.jdbc.core.metadata.TableMetaDataProviderFactory;
import org.springframework.jdbc.core.metadata.TableParameterMetaData;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
        
        // This is a common name like 'Microsoft SQL Server' and 'Oracle' returned by DatabaseMetaData.getDatabaseProductName
        String dbProvider = "Oracle";

        Set<String> schemaIncludes = new HashSet<>(); // Empty means include all except for the excluded one
        Set<String> schemaExcludes = new HashSet<>();
        String dbDriverClassName = null;
        String dbUrl = null;
        String dbUserName = null;
        String dbPassword = "";

        if ("Microsoft SQL Server".equals(dbProvider)) {
            schemaExcludes.addAll(Arrays.asList("sys"));
            dbDriverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            dbUrl = "jdbc:sqlserver://localhost:1433;DatabaseName=AdventureWorks2008R2";
            dbUserName = "sa";
        } else if ("Oracle".equals(dbProvider)) {
            // This is to address https://community.oracle.com/thread/943911?tstart=0&messageID=3793101
            if ("Linux".equalsIgnoreCase(System.getProperty("os.name"))) {
                System.setProperty("java.security.egd", "file:///dev/urandom");
            }
            schemaIncludes.addAll(Arrays.asList("HR", "OE", "PM", "IX", "SH", "BI"));
            dbDriverClassName = "oracle.jdbc.driver.OracleDriver";
            dbUrl = "jdbc:oracle:thin:@localhost:1521:orcl";
            dbUserName = "sys as sysdba";
        }

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(dbDriverClassName);
        dataSource.setUrl(dbUrl);
        dataSource.setUsername(dbUserName);
        dataSource.setPassword(dbPassword);

        System.out.println("Database Product Name: " + JdbcUtils.extractDatabaseMetaData(dataSource, "getDatabaseProductName"));
        System.out.println("Database Product Version: " + JdbcUtils.extractDatabaseMetaData(dataSource, "getDatabaseProductVersion"));
        System.out.println("\n");

        final List<TableInfo> tables = TableInfoFactory.getTableInfo(dataSource);

        for (TableInfo table : tables) {
            if (!schemaIncludes.isEmpty() && !schemaIncludes.contains(table.getSchemaName())
                    || schemaExcludes.contains(table.getSchemaName())) {
                continue;
            }
            TableMetaDataContext context = new TableMetaDataContext();
            context.setAccessTableColumnMetaData(true);
            context.setCatalogName(dataSource.getCatalog());
            context.setSchemaName(table.getSchemaName());
            context.setTableName(table.getTableName());

            System.out.println(String.format("%s.[%s]", table.getSchemaName(), table.getTableName()));

            TableMetaDataProvider provider = TableMetaDataProviderFactory.createMetaDataProvider(dataSource, context);

            List<TableParameterMetaData> parameters = provider.getTableParameterMetaData();
            for (TableParameterMetaData parameter : parameters) {
                String sqlType = sqlTypes.getOrDefault(parameter.getSqlType(), "UNKNOWN");
                String columnName = parameter.getParameterName();
                String sampleList = "";
                if (sqlType.endsWith("CHAR")) {
                    List<String> samples = ColumnSamplerFactory.createSampler(dbProvider).sample(dataSource, table, columnName, 11);
                    sampleList = createAbbreviatedSamples(samples);
                }
                System.out.println(String.format("  %s (%s%s) %s", columnName, sqlType, parameter.isNullable() ? ", null" : "", sampleList));
            }
        }

    }

    private static String createAbbreviatedSamples(List<String> list) {
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
