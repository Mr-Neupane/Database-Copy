using System.Data;
using Database_Copy.Providers.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Database_Copy.Providers;

public class ConnectionProvider : IConnectionProvider
{
    private readonly string _psqlSettings;
    private readonly string _msSqlSettings;


    public ConnectionProvider(IConfiguration configuration)
    {
        _msSqlSettings = configuration.GetConnectionString("MSSQL")
                         ?? throw new InvalidOperationException(
                             "SQL Server connection string is missing in configuration.");

        _psqlSettings = configuration.GetConnectionString("PostgresSQL") ??
                        throw new InvalidOperationException("Psql connection string is missing in configuration.");
    }

    public IDbConnection GetPsqlConnection(string? dbName)
    {
        var finalPsqlConnection = _psqlSettings;
        var builder = new NpgsqlConnectionStringBuilder(finalPsqlConnection)
        {
            Database = dbName ?? "postgres"
        };
        finalPsqlConnection = builder.ConnectionString;
        var connection = new NpgsqlConnection(finalPsqlConnection);
        connection.Open();
        return connection;
    }

    public IDbConnection GetMssqlConnection(string? dbname)
    {
        var finalMssqlConn = _msSqlSettings;

        var builder = new SqlConnectionStringBuilder(finalMssqlConn)
        {
            InitialCatalog = dbname ?? "master"
        };
        finalMssqlConn = builder.ConnectionString;
        var conn = new SqlConnection(finalMssqlConn);
        conn.Open();
        return conn;
    }
}