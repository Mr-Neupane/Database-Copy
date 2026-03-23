using System.Data;
using System.Diagnostics;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services.Interfaces;
using Dapper;
using Database_Copy.Helpers.Interfaces;
using Database_Copy.Models;
using Database_Copy.Validator.Interfaces;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;

namespace Database_Copy.Services;

public class DbCopyService : IDbCopyService
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly IDataTypeProvider _dataTypeProvider;
    private readonly IValidator _validator;
    private readonly IDbInfoProvider _dbInfoProvider;
    private readonly ICreateHelper _createHelper;

    public DbCopyService(IConnectionProvider connectionProvider, IDataTypeProvider dataTypeProvider,
        IValidator validator, IDbInfoProvider dbInfoProvider, ICreateHelper createHelper)
    {
        _connectionProvider = connectionProvider;
        _dataTypeProvider = dataTypeProvider;
        _validator = validator;
        _dbInfoProvider = dbInfoProvider;
        _createHelper = createHelper;
    }

    public void ValidateAndMigrate(string dbName, bool isToPostgres)
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
            var schemas = _dbInfoProvider.GetSchemas(dbName, true);
            _createHelper.CreateSchemas(dbName, schemas);
            var tables = _dbInfoProvider.GetTables(dbName, true);
            _createHelper.CreateTables(dbName, tables, true);
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
            var schema = _dbInfoProvider.GetSchemas(dbName);
            _createHelper.CreateSchemas(dbName, schema, true);
            var tables = _dbInfoProvider.GetTables(dbName);
            _createHelper.CreateTables(dbName, tables);
            MigrateTableData(dbName, tables, true);
        }
        else
        {
            Console.WriteLine($"Database with `{dbName}` does not exist");
            Environment.Exit(0);
        }
    }


    void MigrateTableData(string dbName, List<Table> tables, bool isDesPsql = false)
    {
        var psqlConnection = _connectionProvider.GetPsqlConnection(dbName);
        var msSqlConnection = _connectionProvider.GetMssqlConnection(dbName);
        if (isDesPsql)
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var t in tables.Distinct())
            {
                var npgsqlConn = (NpgsqlConnection)psqlConnection;
                var query = $"select * from \"{t.OldSchemaName}\".\"{t.TableName}\";";
                var data = msSqlConnection.ExecuteReader(query);

                var dataTable = new DataTable();
                dataTable.Load(data);
                PsqlBulkInsert(npgsqlConn, dataTable, $"\"{t.NewSchemaName}\".\"{t.TableName}\"");
                Console.WriteLine($"Migration from {t.OldSchemaName}.{t.TableName} completed.");
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


    void PsqlBulkInsert(NpgsqlConnection connection, DataTable dataTable, string tableName)
    {
        var cmdTxt =
            $"COPY {tableName} ({string.Join(",", dataTable.Columns.Cast<DataColumn>().Select(c => _validator.ValidateDoubleQuotesColumns(c.ColumnName) ? $"\"{c.ColumnName.Trim()}\"" : c.ColumnName.Trim()))}) FROM STDIN (FORMAT BINARY)";
        var types = new List<string>();
        var count = 0;
        using (var importer = connection.BeginBinaryImport(cmdTxt))
        {
            // Add each row to the binary importer
            foreach (DataRow row in dataTable.Rows)
            {
                importer.StartRow();
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    var value = row[i];
                    var type = dataTable.Columns[i].DataType;
                    if (count == 0)
                    {
                        types.Add(type.FullName);
                    }

                    NpgsqlDbType dataType = _dataTypeProvider.GetTypeForPsql(type);
                    if (value == null || value == DBNull.Value)
                    {
                        importer.WriteNull();
                    }
                    else
                    {
                        // var byteVal = ConvertToBytes(value, type);
                        importer.Write(value, dataType);
                    }
                }

                count++;
            }


            try
            {
                importer.Complete();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine($"{string.Join(",", types)}");
                connection.Close();
                connection.Open();
            }
        }
    }
}