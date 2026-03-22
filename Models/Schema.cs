namespace Database_Copy.Models;

public class Schema
{
    public string Name { get; set; }
}

public class Table
{
    public string SchemaName { get; set; }
    public string TableName { get; set; }
}

public class Columns
{
    public string SchemaName { get; set; }
    public string TableName { get; set; }
    public string ColumnName { get; set; }
    public string DataType { get; set; }
    public string IsNullable { get; set; }
    public string IsIdentity { get; set; }
}