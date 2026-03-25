using System.Data;
using System.Text.RegularExpressions;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services.Interfaces;
using Dapper;
using Database_Copy.Helpers.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;

namespace Database_Copy.Services;

public class DbCopyService : IDbCopyService
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly IMigrationService _migrationService;
    private readonly IDbInfoProvider _dbInfoProvider;
    private readonly ICreateHelper _createHelper;


    public DbCopyService(IConnectionProvider connectionProvider, IMigrationService migrationService,
        IDbInfoProvider dbInfoProvider, ICreateHelper createHelper)
    {
        _connectionProvider = connectionProvider;
        _migrationService = migrationService;
        _dbInfoProvider = dbInfoProvider;
        _createHelper = createHelper;
    }

    public void ValidateAndMigrate(string dbName, bool isToPostgres, bool isVersioning, string queryPath)
    {
        if (isVersioning)
        {
            _migrationService.DowngradeMssqlDb(dbName, queryPath);
        }
        else
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
            _migrationService.MigrateTableData(dbName, tables);
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
            _migrationService.MigrateTableData(dbName, tables, true);
        }
        else
        {
            Console.WriteLine($"Database with `{dbName}` does not exist");
            Environment.Exit(0);
        }
    }
}