using Database_Copy.Helpers;
using Database_Copy.Helpers.Interfaces;
using Database_Copy.Providers;
using Database_Copy.Providers.Interfaces;
using Database_Copy.Services;
using Database_Copy.Services.Interfaces;
using Database_Copy.Validator;
using Database_Copy.Validator.Interfaces;
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
    .AddTransient<IDataTypeProvider, DataTypeProvider>()
    .AddTransient<IDbInfoProvider, DbInfoProvider>()
    .AddTransient<IValidator, Validator>()
    .AddTransient<ICreateHelper, CreateHelper>();

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
Console.WriteLine("3. Mssql Db versioning");
var type = Convert.ToInt32(Console.ReadLine()?.Trim());
if (type > 3 || type == 0)
{
    Console.WriteLine("Invalid copy selection.");
    return;
}

var toPostgres = type == 2;
var isVersioning = type == 3;

var service = serviceProvider.GetRequiredService<IDbCopyService>();
service.ValidateAndMigrate(dbName, toPostgres,isVersioning);