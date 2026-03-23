using Dapper;
using Database_Copy.Models;
using Database_Copy.Providers.Interfaces;

namespace Database_Copy.Providers;

public class DbInfoProvider : IDbInfoProvider
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly IDataTypeProvider _dataTypeProvider;

    public DbInfoProvider(IConnectionProvider connectionProvider, IDataTypeProvider dataTypeProvider)
    {
        _connectionProvider = connectionProvider;
        _dataTypeProvider = dataTypeProvider;
    }

    public List<Schema> GetSchemas(string dbName, bool isPsqlToMssql = false)
    {
        if (isPsqlToMssql)
        {
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            var query =
                @"SELECT schema_name as OldSchemaName FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema') AND schema_name NOT LIKE 'pg_toast%';";
            var schema = conn.Query<Schema>(query).ToList();
            return schema;
        }
        else
        {
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            var query =
                "select SCHEMA_NAME OldSchemaName from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME not in('db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter');";
            var schema = conn.Query<Schema>(query).ToList();
            return schema;
        }
    }
    
  public  List<Table> GetTables(string dbName, bool isPsqlToMssql = false)
    {
        var schemas = GetSchemas(dbName, isPsqlToMssql);
        if (isPsqlToMssql)
        {
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            var query = "SELECT schemaname as OldSchemaName, tablename FROM pg_tables;";
            var tables = conn.Query<Table>(query).ToList();
            var final = (from s in schemas
                join t in tables on s.OldSchemaName equals t.OldSchemaName
                select new Table()
                {
                    OldSchemaName = s.OldSchemaName,
                    TableName = t.TableName
                }).ToList();
            return final;
        }
        else
        {
            var query = "select TABLE_SCHEMA OldSchemaName, TABLE_NAME TableName from INFORMATION_SCHEMA.TABLES;";
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            var unfilteredTables = conn.Query<Table>(query).ToList();
            var tables = (from t in unfilteredTables
                join s in schemas on t.OldSchemaName equals s.OldSchemaName
                select new Table
                {
                    OldSchemaName = t.OldSchemaName,
                    TableName = t.TableName
                }).ToList();
            return tables;
        }
    }
  
  public List<Columns> GetColumns(string dbName, bool isPsqlToMssql = false)
    {
        if (isPsqlToMssql)
        {
            var schema = GetSchemas(dbName, true);
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            var query =
                "SELECT table_schema OldSchemaName,table_name TableName,column_name ColumnName, data_type DataType, is_nullable IsNullable,is_identity IsIdentity FROM information_schema.columns c order by ordinal_position;";
            var columns = conn.Query<Columns>(query).ToList();
            var finalColumns = (from s in schema
                join c in columns on s.OldSchemaName equals c.OldSchemaName
                select new Columns()
                {
                    OldSchemaName = s.OldSchemaName,
                    TableName = c.TableName,
                    ColumnName = c.ColumnName,
                    DataType = _dataTypeProvider.GetCompatibleDataTypeForMssql(c.DataType),
                    IsNullable = c.IsNullable,
                    IsIdentity = c.IsIdentity,
                }).ToList();
            return finalColumns;
        }
        else
        {
            var schemas = GetSchemas(dbName);
            var query = @";WITH pk_columns AS (SELECT ic.object_id,
                           ic.column_id
                    FROM sys.indexes i
                             JOIN sys.index_columns ic
                                  ON i.object_id = ic.object_id
                                      AND i.index_id = ic.index_id
                    WHERE i.is_primary_key = 1)
        SELECT s.name                              AS OldSchemaName,
       o.name                              AS TableName,
       c.name                              AS ColumnName,
       ty.name                             AS DataType,
       IIF(c.is_nullable = 1, 'YES', 'NO') AS IsNullable,
       IIF(c.is_identity = 1, 'YES', 'NO') AS IsIdentity
        FROM sys.columns c
         JOIN sys.objects o
              ON c.object_id = o.object_id
         JOIN sys.schemas s
              ON o.schema_id = s.schema_id
         LEFT JOIN sys.types ty
                   ON c.user_type_id = ty.user_type_id
         LEFT JOIN pk_columns pk
                   ON c.object_id = pk.object_id
                       AND c.column_id = pk.column_id
        WHERE o.type in ('U', 'V')
        ORDER BY s.name,
         o.name,
         c.column_id;";
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            var unFilteredColumns = conn.Query<Columns>(query).ToList();
            var finalColumns = (from c in unFilteredColumns
                join s in schemas on c.OldSchemaName equals s.OldSchemaName
                select new Columns
                {
                    OldSchemaName = c.OldSchemaName,
                    TableName = c.TableName,
                    ColumnName = c.ColumnName,
                    DataType = _dataTypeProvider.GetCompatibleDataTypeForPsql(c.DataType),
                    IsNullable = c.IsNullable,
                    IsIdentity = c.IsIdentity,
                }).ToList();
            return finalColumns;
        }
    }
}