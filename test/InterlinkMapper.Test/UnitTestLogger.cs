using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class UnitTestLogger(ITestOutputHelper Output) : ILogger
{
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
		Output.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} [{logLevel}] {formatter(state, exception)}");
	}
}