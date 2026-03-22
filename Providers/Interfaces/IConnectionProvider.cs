using System.Data;

namespace Database_Copy.Providers.Interfaces;

public interface IConnectionProvider
{
    IDbConnection GetPsqlConnection(string? dbname = null);
    IDbConnection GetMssqlConnection(string? dbname = null);
}