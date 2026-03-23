using Dapper;
using Database_Copy.Helpers.Interfaces;
using Database_Copy.Models;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Validator.Interfaces;

namespace Database_Copy.Helpers;

public class CreateHelper : ICreateHelper
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly IDbInfoProvider _dbInfoProvider;
    private readonly IValidator _validator;

    public CreateHelper(IConnectionProvider connectionProvider, IDbInfoProvider dbInfoProvider, IValidator validator)
    {
        _connectionProvider = connectionProvider;
        _dbInfoProvider = dbInfoProvider;
        _validator = validator;
    }

    public void CreateSchemas(string dbName, List<Schema> schemas, bool isMssqlToPsql = false)
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

    public void CreateTables(string dbName, List<Table> tables, bool isPsqlToMssql = false)
    {
        var columns = _dbInfoProvider.GetColumns(dbName, isPsqlToMssql);
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
            var column = _validator.ValidateDoubleQuotesColumns(tc[i].ColumnName)
                ? $"\"{tc[i].ColumnName}\""
                : tc[i].ColumnName;
            columnsCreation += $" {column}  {tc[i].DataType} {pk} {nullable} {comma} ";
        }

        return columnsCreation;
    }
}