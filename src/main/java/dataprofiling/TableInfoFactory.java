package dataprofiling;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

public class TableInfoFactory {
    
    @SuppressWarnings("unchecked")
    public static List<TableInfo> getTableInfo(DriverManagerDataSource dataSource) {
        try {
            return (List<TableInfo>) JdbcUtils.extractDatabaseMetaData(dataSource, new DatabaseMetaDataCallback() {

                @Override
                public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
                    ResultSet rs = dbmd.getTables(dataSource.getCatalog(), null, null, new String[]{"TABLE"});
//                    ResultSetMetaData meta = rs.getMetaData();
//                    for (int i = 1; i <= meta.getColumnCount(); i++) {
//                        System.out.println(meta.getColumnName(i));
//                    }
                    
                    List<TableInfo> tables = new ArrayList<>();

                    while (rs.next()) {
                        String catalog = rs.getString("TABLE_CAT");
                        String schema = rs.getString("TABLE_SCHEM");
                        String name = rs.getString("TABLE_NAME");
                        tables.add(new TableInfo(catalog, schema, name));
                    }

                    return tables;
                }
            });
        } catch (MetaDataAccessException e) {
            throw new DataAccessResourceFailureException("Error retrieving database metadata", e);
        }
    }
}
