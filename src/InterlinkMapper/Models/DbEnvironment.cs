namespace InterlinkMapper.Models;

public class DbEnvironment
{
	public string PlaceHolderIdentifer { get; set; } = ":";

	public string NumericTypeName { get; set; } = "int8";

	public string TextTypeName { get; set; } = "text";

	public string TimestampTypeName { get; set; } = "timestamp";

	public string TimestampDefaultValue { get; set; } = "current_timestamp";

	public int CommandTimeout { get; set; } = 60 * 15;
}
