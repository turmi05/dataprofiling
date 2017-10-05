package dataprofiling;

public class TableInfo {

    private String catalogName; // May be null
    private String schemaName;
    private String tableName;
    
    TableInfo(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

}
