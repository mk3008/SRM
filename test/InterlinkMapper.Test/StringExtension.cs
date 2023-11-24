namespace InterlinkMapper.Test;

public static class StringExtensions
{
	public static string ToValidateText(this string input)
	{
		return input.ToLowerInvariant()
					.Replace(" ", string.Empty)
					.Replace("\t", string.Empty)
					.Replace("\r", string.Empty)
					.Replace("\n", string.Empty);
	}
}