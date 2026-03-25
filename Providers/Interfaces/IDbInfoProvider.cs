using System.Data;
using Database_Copy.Models;
using Microsoft.SqlServer.Management.Smo;
using Schema = Database_Copy.Models.Schema;

namespace Database_Copy.Providers.Interfaces;

public interface IDbInfoProvider
{
    List<Schema> GetSchemas(string dbName, bool isPsqlToMssql = false);
    List<DbTable> GetTables(string dbName, bool isPsqlToMssql = false);
    List<Columns> GetColumns(string dbName, bool isPsqlToMssql = false, bool isDbVersioning = false);
    SqlServerVersion GetTargetVersion(IDbConnection connection);
}