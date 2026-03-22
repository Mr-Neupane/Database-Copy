using Database_Copy.Providers;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services;
using Database_Copy.Services.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var services = new ServiceCollection();

services.AddSingleton<IConfiguration>(configuration);

services.AddScoped<IConnectionProvider, ConnectionProvider>()
    .AddTransient<IDbCopyService, DbCopyService>()
    .AddTransient<IDataTypeProvider, DataTypeProvider>();

var serviceProvider = services.BuildServiceProvider();

string dbName;


Console.Write("Enter db name: ");
dbName = Console.ReadLine()?.Trim();

if (string.IsNullOrWhiteSpace(dbName))
{
    Console.WriteLine("Db name cannot be empty.");
    return;
}

Console.WriteLine("Select server type to be moved:");
Console.WriteLine("1. Psql to Mssql");
Console.WriteLine("2. Mssql to Psql");
var type = Convert.ToInt32(Console.ReadLine()?.Trim());
if (type > 2)
{
    Console.WriteLine("Invalid copy selection.");
    return;
}

var toPostgres = type == 2;

var service = serviceProvider.GetRequiredService<IDbCopyService>();
service.ValidateAndCopy(dbName, toPostgres);