package dataprofiling;

public class ColumnSamplerFactory {
    
    public static ColumnSampler createSampler(String dbProvider) {
        if ("Microsoft SQL Server".equals(dbProvider)) {
            return new MsSqlColumnSampler();
        }
        if ("Oracle".equals(dbProvider)) {
            return new OracleColumnSampler();
        }
        throw new RuntimeException("Unsupported database provider: " + dbProvider);
    }
}
