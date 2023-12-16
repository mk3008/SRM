namespace InterlinkMapper.Models;

public class DbEnvironment
{
	public string PlaceHolderIdentifer { get; set; } = ":";

	public string AutoNumberTypeName { get; set; } = "serial8";

	public string NumericTypeName { get; set; } = "int8";

	public string TextTypeName { get; set; } = "text";

	public string TimestampTypeName { get; set; } = "timestamp";

	public string TimestampDefaultValue { get; set; } = "current_timestamp";

	/// <summary>
	/// Function to get the length of a string.
	/// </summary>
	/// <example>
	/// LENGTH
	/// </example>
	public string LengthFunction { get; set; } = "length";

	/// <summary>
	/// Comparison operators that take NULLs into account.
	/// If omitted, a redundant evaluation expression using standard functions will be applied.
	/// ex.
	/// WHERE column1 <> column2 OR (column1 IS NULL AND column2 IS NOT NULL) OR (column1 IS NOT NULL AND column2 IS NULL);
	/// </summary>
	/// <example>
	/// is not distinct from
	/// </example>
	public string NullSafeEqualityOperator { get; set; } = "is not distinct from";

	public int CommandTimeout { get; set; } = 60 * 15;
}
