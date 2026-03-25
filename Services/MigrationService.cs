using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;
using Database_Copy.Models;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services.Interfaces;
using Database_Copy.Validator.Interfaces;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Npgsql;
using NpgsqlTypes;

namespace Database_Copy.Services;

public class MigrationService : IMigrationService
{
    private readonly IValidator _validator;
    private readonly IDataTypeProvider _dataTypeProvider;
    private readonly IConnectionProvider _connectionProvider;
    private readonly IDbInfoProvider _dbInfoProvider;
    private readonly IRawQueryService _rawQueryService;

    public MigrationService(IValidator validator, IDataTypeProvider dataTypeProvider,
        IConnectionProvider connectionProvider, IDbInfoProvider dbInfoProvider, IRawQueryService rawQueryService)
    {
        _validator = validator;
        _dataTypeProvider = dataTypeProvider;
        _connectionProvider = connectionProvider;
        _dbInfoProvider = dbInfoProvider;
        _rawQueryService = rawQueryService;
    }

    public void MigrateTableData(string dbName, List<DbTable> tables, bool isDesPsql = false)
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

//     public void DowngradeMssqlDb(string dbName, string queryPath)
//     {
//         var instance = _connectionProvider.GetMssqlConnection();
//         var sqlConn = instance as Microsoft.Data.SqlClient.SqlConnection;
//         _rawQueryService.ExecuteQuery(instance, queryPath);
//
//         if (sqlConn == null)
//             throw new Exception("Connection is not a SqlConnection");
//
//         var serverConnection = new ServerConnection(sqlConn);
//         var sourceServer = new Server(serverConnection);
//         var sourceDb = sourceServer.Databases[dbName];
//
//         var conn = _connectionProvider.GetLowerMssqlConnection(dbName);
//
//         conn.Execute($@"
//         IF DB_ID('{dbName}') IS NULL
//         CREATE DATABASE [{dbName}];
//     ");
//
//         var targetVersion = _dbInfoProvider.GetTargetVersion(conn);
//
//         var scripter = new Scripter(sourceServer)
//         {
//             Options =
//             {
//                 ScriptSchema = true,
//                 ScriptData = false, // schema only
//                 WithDependencies = true,
//
//                 DriAll = true, // PK, FK, constraints
//                 Indexes = true, // indexes
//                 IncludeHeaders = true,
//                 IncludeIfNotExists = true,
//                 SchemaQualify = true,
//
//                 TargetServerVersion = targetVersion
//             }
//         };
//
//         var urns = new List<Urn>();
//
// // Tables, Views, SPs, Functions
//         urns.AddRange(sourceDb.Tables.Cast<Table>()
//             .Where(t => !t.IsSystemObject)
//             .Select(t => t.Urn));
//
//         urns.AddRange(sourceDb.Views.Cast<View>()
//             .Where(v => !v.IsSystemObject)
//             .Select(v => v.Urn));
//
//         urns.AddRange(sourceDb.StoredProcedures.Cast<StoredProcedure>()
//             .Where(sp => !sp.IsSystemObject)
//             .Select(sp => sp.Urn));
//
//         urns.AddRange(sourceDb.UserDefinedFunctions.Cast<UserDefinedFunction>()
//             .Where(fn => !fn.IsSystemObject)
//             .Select(fn => fn.Urn));
//
// // Generate schema scripts
//         var schemaScripts = scripter.Script(urns.ToArray());
//
//         foreach (var script in schemaScripts)
//         {
//             conn.Execute(script);
//         }
//
//         foreach (var script in schemaScripts)
//         {
//             conn.Execute(script);
//         }
//
//         foreach (Table table in sourceDb.Tables)
//         {
//             if (table.IsSystemObject) continue;
//
//             var dataScripter = new Scripter(sourceServer)
//             {
//                 Options =
//                 {
//                     ScriptSchema = false,
//                     ScriptData = true,
//                     SchemaQualify = true,
//                     IncludeHeaders = false,
//
//                     TargetServerVersion = targetVersion
//                 }
//             };
//
//             var scripts = dataScripter.Script(new Urn[] { table.Urn });
//
//             bool hasIdentity = table.Columns.Cast<Column>().Any(c => c.Identity);
//
//             if (hasIdentity)
//             {
//                 conn.Execute($"SET IDENTITY_INSERT [{table.Schema}].[{table.Name}] ON");
//             }
//
//             foreach (var script in scripts)
//             {
//                 conn.Execute(script);
//             }
//
//             if (hasIdentity)
//             {
//                 conn.Execute($"SET IDENTITY_INSERT [{table.Schema}].[{table.Name}] OFF");
//             }
//         }
//
//         _rawQueryService.ExecuteQuery(conn, queryPath);
//     }

    public void DowngradeMssqlDb(string dbName, string queryPath)
    {
        var sourceConn = _connectionProvider.GetMssqlConnection();
        var sourceSqlConn = sourceConn as Microsoft.Data.SqlClient.SqlConnection
                            ?? throw new InvalidOperationException("Source connection is not a SqlConnection.");

        if (sourceConn.State != ConnectionState.Open)
            sourceConn.Open();

        _rawQueryService.ExecuteQuery(sourceConn, queryPath);

        var serverConn = new ServerConnection(sourceSqlConn);
        var server = new Server(serverConn);
        var sourceDb = server.Databases[dbName]
                       ?? throw new InvalidOperationException($"Database '{dbName}' not found.");

        var targetConn = _connectionProvider.GetLowerMssqlConnection(dbName);
        var targetSqlConn = targetConn as Microsoft.Data.SqlClient.SqlConnection
                            ?? throw new InvalidOperationException("Target connection is not a SqlConnection.");

        if (targetConn.State != ConnectionState.Open)
            targetConn.Open();

        targetConn.Execute($"IF DB_ID('{dbName}') IS NULL CREATE DATABASE [{dbName}]");
        targetSqlConn.ChangeDatabase(dbName);

        var targetConnStr = targetSqlConn.ConnectionString;
        var targetVersion = _dbInfoProvider.GetTargetVersion(targetConn);

        var scripter = new Scripter(server)
        {
            Options =
            {
                ScriptSchema = true,
                ScriptData = false,
                WithDependencies = false,
                DriAll = true,
                Indexes = true,
                IncludeHeaders = false,
                IncludeIfNotExists = true,
                SchemaQualify = true,
                AnsiPadding = true,
                TargetServerVersion = targetVersion
            }
        };

        void ScriptAndApply(IEnumerable<Urn> urns)
        {
            var list = urns.ToList();
            if (!list.Any()) return;

            foreach (var script in scripter.Script(list.ToArray()))
            {
                if (string.IsNullOrWhiteSpace(script)) continue;
                var cleaned = System.Text.RegularExpressions.Regex.Replace(
                    script,
                    @"\s+TEXTIMAGE_ON\s+\[?\w+\]?",
                    string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );
                try
                {
                    targetConn.Execute(cleaned);
                }
                catch (SqlException ex) when (
                    ex.Message.Contains("already exists") ||
                    ex.Message.Contains("There is already an object"))
                {
                    /* skip duplicates */
                }
            }
        }

        var allTables = sourceDb.Tables.Cast<Table>().Where(t => !t.IsSystemObject).ToList();
        var tableMap = allTables.ToDictionary(t => $"{t.Schema}.{t.Name}");
        var visited = new HashSet<string>();
        var sorted = new List<Table>();

        void Visit(Table t)
        {
            var key = $"{t.Schema}.{t.Name}";
            if (!visited.Add(key)) return;
            foreach (ForeignKey fk in t.ForeignKeys)
                if (tableMap.TryGetValue($"{fk.ReferencedTableSchema}.{fk.ReferencedTable}", out var parent))
                    Visit(parent);
            sorted.Add(t);
        }

        allTables.ForEach(Visit);

        ScriptAndApply(sourceDb.UserDefinedDataTypes.Cast<UserDefinedDataType>().Select(t => t.Urn));
        ScriptAndApply(sourceDb.UserDefinedTableTypes.Cast<UserDefinedTableType>().Select(t => t.Urn));
        ScriptAndApply(sorted.Select(t => t.Urn));
        ScriptAndApply(sourceDb.Views.Cast<View>().Where(v => !v.IsSystemObject).Select(v => v.Urn));
        ScriptAndApply(sourceDb.StoredProcedures.Cast<StoredProcedure>().Where(sp => !sp.IsSystemObject)
            .Select(sp => sp.Urn));
        ScriptAndApply(sourceDb.UserDefinedFunctions.Cast<UserDefinedFunction>().Where(fn => !fn.IsSystemObject)
            .Select(fn => fn.Urn));
        ScriptAndApply(allTables.SelectMany(t => t.Triggers.Cast<Trigger>().Select(tr => tr.Urn)));


        targetConn.Execute("EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");

        foreach (var table in sorted)
        {
            var fullName = $"[{table.Schema}].[{table.Name}]";
            bool hasIdentity = table.Columns.Cast<Column>().Any(c => c.Identity);

            var rowCount = sourceConn.ExecuteScalar<int>($"SELECT COUNT(*) FROM {fullName}");
            if (rowCount == 0) continue;

            var bulkOptions = hasIdentity
                ? Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity |
                  Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepNulls
                : Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepNulls;

            var query = $"SELECT * FROM {fullName}";
            using var reader = sourceConn.ExecuteReader(query);
            using var bulk = new Microsoft.Data.SqlClient.SqlBulkCopy(targetConnStr, bulkOptions)
            {
                DestinationTableName = fullName,
                BatchSize = 5000,
                BulkCopyTimeout = 0
            };

            var schema = reader.GetSchemaTable();
            if (schema != null)
                foreach (DataRow col in schema.Rows)
                {
                    var name = col["ColumnName"].ToString();
                    bulk.ColumnMappings.Add(name, name);
                }

            bulk.WriteToServer(reader);
            Console.WriteLine(reader);
        }

        targetConn.Execute("EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'");

        foreach (var table in sorted)
        {
            var fullName = $"[{table.Schema}].[{table.Name}]";
            try
            {
                targetConn.Execute($"ALTER INDEX ALL ON {fullName} REBUILD");
            }
            catch
            {
                /* non-fatal */
            }

            try
            {
                targetConn.Execute($"UPDATE STATISTICS {fullName}");
            }
            catch
            {
                /* non-fatal */
            }
        }

        _rawQueryService.ExecuteQuery(targetConn, queryPath);
    }
}