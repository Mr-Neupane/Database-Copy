using Database_Copy.Models;

namespace Database_Copy.Providers.Interfaces;

public interface IDbInfoProvider
{
    List<Schema> GetSchemas(string dbName, bool isPsqlToMssql = false);
    List<Table> GetTables(string dbName, bool isPsqlToMssql = false);
    List<Columns> GetColumns(string dbName, bool isPsqlToMssql = false, bool isDbVersioning = false);
}