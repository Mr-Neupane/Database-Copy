using Database_Copy.Models;

namespace Database_Copy.Helpers.Interfaces;

public interface ICreateHelper
{
    void CreateSchemas(string dbName, List<Schema> schemas, bool isMssqlToPsql = false);
    void CreateTables(string dbName, List<DbTable> tables, bool isPsqlToMssql = false, bool isDbVersioning = false);
}