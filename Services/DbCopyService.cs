using System.Data;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.InteropServices.ComTypes;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services.Interfaces;
using Dapper;
using Database_Copy.Models;

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
            var schemas = GetPsqlSchemas(dbName);
            CreateSchemas(dbName, schemas);
            var tables = GetPsqlTables(dbName);
            CreateTables(dbName, tables);
        }
    }


    private void ConvertMssqlToPsql(string dbName)
    {
        throw new NotImplementedException();
    }


    List<Schema> GetPsqlSchemas(string dbName)
    {
        var conn = _connectionProvider.GetPsqlConnection(dbName);
        var query = @"SELECT concat(schema_name,'_mig')as Name FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                    AND schema_name NOT LIKE 'pg_toast%';";
        var schema = conn.Query<Schema>(query).ToList();
        return schema;
    }

    List<Table> GetPsqlTables(string dbName)
    {
        var schemas = GetPsqlSchemas(dbName);
        var conn = _connectionProvider.GetPsqlConnection(dbName);
        var query = "SELECT schemaname, tablename FROM pg_tables;";
        var tables = conn.Query<Table>(query).ToList();
        var final = (from s in schemas
            join t in tables on s.Name equals string.Concat(t.SchemaName, "_mig")
            select new Table()
            {
                SchemaName = s.Name,
                TableName = t.TableName
            }).ToList();
        return final;
    }

    List<Columns> GetColumns(string dbName, bool isPsqlToMssql = false)
    {
        if (isPsqlToMssql)
        {
            var schema = GetPsqlSchemas(dbName);
            var conn = _connectionProvider.GetPsqlConnection(dbName);
            var query =
                "SELECT concat(c.table_schema,'_mig') SchemaName,table_name TableName,column_name ColumnName, data_type DataType, is_nullable IsNullable,is_identity IsIdentity FROM information_schema.columns c;";
            var columns = conn.Query<Columns>(query).ToList();
            var finalColumns = (from s in schema
                join c in columns on s.Name equals c.SchemaName
                select new Columns()
                {
                    SchemaName = s.Name,
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
            return null;
        }
    }

    void CreateSchemas(string dbName, List<Schema> schemas, bool isPsqlDb = false)
    {
        if (isPsqlDb)
        {
        }
        else
        {
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            foreach (var schema in schemas.Distinct())
            {
                var createSchema = @$"
            if not exists(select 1
              from INFORMATION_SCHEMA.SCHEMATA
              where SCHEMA_NAME ='{schema.Name}')
                begin
           EXEC('CREATE SCHEMA [{schema.Name}]');
            end;";
                conn.Execute(createSchema);
            }
        }
    }

    void CreateTables(string dbName, List<Table> tables, bool isPsqlDb = false)
    {
        if (isPsqlDb)
        {
        }
        else
        {
            var columns = GetColumns(dbName, true);
            var conn = _connectionProvider.GetMssqlConnection(dbName);
            foreach (var t in tables.Distinct())
            {
                string columnsCreation = "";
                var tc = columns.Where(x => x.TableName == t.TableName && t.SchemaName == x.SchemaName).Distinct()
                    .ToList();
                for (int i = 0; i < tc.Count; i++)
                {
                    var comma = i == tc.Count - 1 ? "" : ",";
                    var nullable = tc[i].IsNullable == "YES" ? "" : "Not Null";
                    var pk = tc[i].IsIdentity == "YES" ? "primary key" : "";
                    columnsCreation += $" \"{tc[i].ColumnName}\" {tc[i].DataType} {pk} {nullable} {comma} ";
                }


                var createTable = @$"if not exists(
                                        select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME='{t.TableName}' and TABLE_SCHEMA='{t.SchemaName}')
                                    begin
                                    create table {t.SchemaName}.{t.TableName} ({columnsCreation})
                                    end;";
                conn.Execute(createTable);
            }
        }
    }
}