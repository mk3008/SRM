using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace PostgresTest;

public class UnitTestLogger : ILogger
{
	public required ITestOutputHelper Output { get; init; }

	public IDisposable? BeginScope<TState>(TState state) where TState : notnull
	{
		return null;
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		return true;
	}

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
	{
		Output.WriteLine($"{DateTime.UtcNow:mm:ss.fff} [{logLevel}] {formatter(state, exception)}");
	}
}