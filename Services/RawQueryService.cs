using System.Data;
using Dapper;
using Database_Copy.Services.Interfaces;

namespace Database_Copy.Services;

public class RawQueryService : IRawQueryService
{
    public void ExecuteQuery(IDbConnection dbConnection, string queryPath)
    {
        if (File.Exists(queryPath))
        {
            var queryFile = File.ReadAllText(queryPath);
            dbConnection.Execute(queryFile);
        }
        else
        {
            Console.WriteLine("File not found: " + queryPath);
        }
    }
}