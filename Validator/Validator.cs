using Database_Copy.Validator.Interfaces;

namespace Database_Copy.Validator;

public class Validator : IValidator
{
    public bool ValidateDoubleQuotesColumns(string cName)
    {
        var result = new List<string>();
        result.Add("left");
        result.Add("right");
        result.Add("from");
        result.Add("to");
        result.Add("for");
        var quoteColumn = result.Any(x => x == cName.Trim().ToLower());
        return quoteColumn;
    }
}