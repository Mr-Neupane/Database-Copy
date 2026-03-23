using System.Data;
using System.Diagnostics;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services.Interfaces;
using Dapper;
using Database_Copy.Models;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Database_Copy.Services;

public class DbCopyService : IDbCopyService
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly IDataTypeProvider _dataTypeProvider;

    public DbCopyService(IConnectionProvider connectionProvider, IDataTypeProvider dataTypeProvider)
    {
        _connectionProvider = connectionProvider;
        _dataTypeProvider = dataTypeProvider;
    }

    public void ValidateAndCopy(string dbName, bool isToPostgres)
    {
        if (isToPostgres)
        {
            ConvertMssqlToPsql(dbName);
        }
        else
        {
            ConvertPsqlToMssql(dbName);
        }
    }

    void ConvertPsqlToMssql(string dbName)
    {
        var psqlParent = _connectionProvider.GetPsqlConnection();
        var query = $"select exists(select 1 from pg_database where datname = \'{dbName}\');";

        using var checkIfDbExists = psqlParent.CreateCommand();
        checkIfDbExists.CommandText = query;

        var exists = (bool)checkIfDbExists.ExecuteScalar();
        if (exists)
        {
            var mssqlConn = _connectionProvider.GetMssqlConnection();
            var createQuery = $"create database [{dbName}]";
            mssqlConn.Execute(createQuery);
            var schemas = GetSchemas(dbName, true);
            CreateSchemas(dbName, schemas);
            var tables = GetTables(dbName, true);
            CreateTables(dbName, tables, true);
            MigrateTableData(dbName, tables);
        }
        else
        {
            Console.WriteLine($"Database with `{dbName}` does not exist");
            Environment.Exit(0);
        }
    }


    private void ConvertMssqlToPsql(string dbName)
    {
        var msSqlConn = _connectionProvider.GetMssqlConnection();
        var validateDb = $"select 1 from sys.databases where name='{dbName.Trim()}'";
        using var checkIfDbExists = msSqlConn.CreateCommand();
        checkIfDbExists.CommandText = validateDb;
        var res = checkIfDbExists.ExecuteScalar();
        var exists = res != null;
        if (exists)
        {
            var psqlConn = _connectionProvider.GetPsqlConnection();
            var createQuery = $"create database \"{dbName}\";";
            psqlConn.Execute(createQuery);
            var schema = GetSchemas(dbName);
            CreateSchemas(dbName, schema, true);
            var tables = GetTables(dbName);
            CreateTables(dbName, tables);
            // Console.Clear();
            MigrateTableData(dbName, tables, true);
        }
        else
        {
            Console.WriteLine($"Database with `{dbName}` does not exist");
            Environment.Exit(0);
        }
    }


    List<Schema> GetSchemas(string dbName, bool isPsqlToMssql = false)
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

    List<Table> GetTables(string dbName, bool isPsqlToMssql = false)
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

    List<Columns> GetColumns(string dbName, bool isPsqlToMssql = false)
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
                    DataType = _dataTypeProvider.GetCompatibleColumnTypeForMssql(c.DataType),
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
                    DataType = _dataTypeProvider.GetCompatibleColumnTypeForPsql(c.DataType),
                    IsNullable = c.IsNullable,
                    IsIdentity = c.IsIdentity,
                }).ToList();
            return finalColumns;
        }
    }

    void CreateSchemas(string dbName, List<Schema> schemas, bool isMssqlToPsql = false)
    {
        if (isMssqlToPsql)
        {
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            foreach (var schema in schemas)
            {
                var query = $"create schema if not exists \"{schema.NewSchemaName}\"";
                conn.Execute(query);
            }
        }
        else
        {
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            foreach (var schema in schemas.Distinct())
            {
                var createSchema = @$"
            if not exists(select 1
              from INFORMATION_SCHEMA.SCHEMATA
              where SCHEMA_NAME ='{schema.NewSchemaName}')
                begin
           EXEC('CREATE SCHEMA [{schema.NewSchemaName}]');
            end;";
                conn.Execute(createSchema);
            }
        }
    }

    void CreateTables(string dbName, List<Table> tables, bool isPsqlToMssql = false)
    {
        var columns = GetColumns(dbName, isPsqlToMssql);
        if (isPsqlToMssql)
        {
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            foreach (var t in tables.Distinct())
            {
                string columnsCreation = GetTableColumns(t.TableName, t.OldSchemaName, columns);
                var createTable = @$"if not exists(
                                        select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME='{t.TableName}' and TABLE_SCHEMA='{t.NewSchemaName}')
                                    begin
                                    create table [{t.NewSchemaName}].[{t.TableName}] ({columnsCreation})
                                    end;";
                conn.Execute(createTable);
            }
        }
        else
        {
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            foreach (var t in tables.Distinct())
            {
                var finalColumns = GetTableColumns(t.TableName, t.OldSchemaName, columns);
                var createTable = $"create table if not exists \"{t.NewSchemaName}\".\"{t.TableName}\"({finalColumns})";
                conn.Execute(createTable);
            }
        }
    }

    void MigrateTableData(string dbName, List<Table> tables, bool isDesPsql = false)
    {
        var psqlConnection = _connectionProvider.GetPsqlConnection(dbName);
        var msSqlConnection = _connectionProvider.GetMssqlConnection(dbName);
        if (isDesPsql)
        {
            foreach (var t in tables.Distinct())
            {
                var npgsqlConn = (NpgsqlConnection)psqlConnection;
                var query = $"select * from {t.OldSchemaName}.{t.TableName};";
                using var reader = msSqlConnection.ExecuteReader(query);
                using (var writer =
                       npgsqlConn.BeginBinaryImport(
                           $"COPY \"{t.NewSchemaName}\".\"{t.TableName}\" FROM STDIN (FORMAT BINARY)"))
                {
                    while (reader.Read())
                    {
                        writer.StartRow();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                writer.WriteNull();
                                continue;
                            }

                            var val = reader.GetValue(i);
                            var type = reader.GetFieldType(i);

                            switch (Type.GetTypeCode(type))
                            {
                                case TypeCode.Int32:
                                    writer.Write((int)val, NpgsqlTypes.NpgsqlDbType.Integer);
                                    break;

                                case TypeCode.Int64:
                                    writer.Write((long)val, NpgsqlTypes.NpgsqlDbType.Bigint);
                                    break;

                                case TypeCode.String:
                                    writer.Write((string)val, NpgsqlTypes.NpgsqlDbType.Text);
                                    break;

                                case TypeCode.DateTime:
                                    writer.Write((DateTime)val, NpgsqlTypes.NpgsqlDbType.Timestamp);
                                    break;

                                case TypeCode.Boolean:
                                    writer.Write((bool)val, NpgsqlTypes.NpgsqlDbType.Boolean);
                                    break;

                                case TypeCode.Decimal:
                                    writer.Write((decimal)val, NpgsqlTypes.NpgsqlDbType.Numeric);
                                    break;

                                default:
                                    writer.Write(val);
                                    break;
                            }
                        }
                    }

                    Console.WriteLine($"Data migrated from {t.OldSchemaName}.{t.TableName}");
                }
            }
        }

        else
        {
            var sqlConn = (SqlConnection)msSqlConnection;
            var stopwatch = Stopwatch.StartNew();
            foreach (var t in tables.Distinct())
            {
                var query = $"select * from \"{t.OldSchemaName}\".\"{t.TableName}\"";
                var tableData = psqlConnection.ExecuteReader(query);
                var dataTable = new DataTable();
                dataTable.Load(tableData);
                using (var txn = sqlConn.BeginTransaction())
                {
                    using (var bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, txn))
                    {
                        bulkCopy.DestinationTableName = $"{t.NewSchemaName}.{t.TableName}";
                        bulkCopy.BulkCopyTimeout = 30000;
                        bulkCopy.WriteToServer(dataTable);
                        Console.WriteLine($"Data migrated from {t.OldSchemaName}.{t.TableName}");
                    }

                    txn.Commit();
                }


                Console.WriteLine($"Data migrated from {t.OldSchemaName}.{t.TableName}");
            }

            stopwatch.Stop();

            var timeTaken = stopwatch.Elapsed;
            
            Console.WriteLine();
            if (timeTaken.TotalMinutes < 1)
            {
                Console.WriteLine(
                    $"Migration completed in {Math.Round(timeTaken.TotalSeconds, 2)} second(s)");
            }
            else
            {
                Console.WriteLine($"Migration completed in {Math.Round(timeTaken.TotalMinutes, 2)} minute(s)");
            }
        }
    }

    string GetTableColumns(string tableName, string oldSchemaName, List<Columns> columns)
    {
        string columnsCreation = "";
        var tc = columns.Where(x => x.TableName == tableName && oldSchemaName == x.OldSchemaName).Distinct()
            .ToList();
        for (int i = 0; i < tc.Count; i++)
        {
            var comma = i == tc.Count - 1 ? "" : ",";
            var nullable = tc[i].IsNullable == "YES" ? "" : "Not Null";
            var pk = tc[i].IsIdentity == "YES" ? "primary key" : "";
            var column = ValidateDoubleQuotesColumns(tc[i].ColumnName) ? $"\"{tc[i].ColumnName}\"" : tc[i].ColumnName;
            columnsCreation += $" {column}  {tc[i].DataType} {pk} {nullable} {comma} ";
        }

        return columnsCreation;
    }


    private bool ValidateDoubleQuotesColumns(string cName)
    {
        var result = new List<string>();
        result.Add("left");
        result.Add("right");
        result.Add("from");
        result.Add("to");
        result.Add("for");
        var quoteColumn = result.Any(x => x == cName.Trim().ToLower());
        return quoteColumn;
    }
}