using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper;

public static class StringExtension
{
	public static string ToHash(this string source, int length = 4)
	{
		using var sha256 = SHA256.Create();

		byte[] inputBytes = Encoding.UTF8.GetBytes(source);
		byte[] hashedBytes = sha256.ComputeHash(inputBytes);
		string hashedString = BitConverter.ToString(hashedBytes).Replace("-", "").ToLower();
		return hashedString.Substring(0, length);
	}
}
