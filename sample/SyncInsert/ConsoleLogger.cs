﻿using Microsoft.Extensions.Logging;

namespace SyncInsert;

public class ConsoleLogger : ILogger
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
		Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} [{logLevel}] {formatter(state, exception)}");
	}
}