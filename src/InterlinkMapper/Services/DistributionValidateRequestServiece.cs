//using Carbunql;
//using Microsoft.Extensions.Logging;
//using System.Data;

//namespace InterlinkMapper.Services;

//public class DistributionValidateRequestServiece
//{
//	public DistributionValidateRequestServiece(IDbConnection cn, ILogger? logger = null, string placeHolderIdentifer = ":")
//	{
//		Connection = cn;
//		Logger = logger;
//		PlaceHolderIdentifer = placeHolderIdentifer;
//	}

//	private readonly ILogger? Logger;

//	private IDbConnection Connection { get; init; }

//	public int CommandTimeout { get; set; } = 60 * 15;

//	private string PlaceHolderIdentifer { get; init; }

//	public int Distribution(IDestination dest)
//	{
//		var requestTable = dest.ValidateRequestTable.GetTableFullName();
//		if (string.IsNullOrEmpty(requestTable)) return 0;

//		var seq = ds.ForwardRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

//		//select * from requests inn
//		var sq = new SelectQuery();
//		var (_, r) = sq.From(requestTable).As("r");
//		sq.Select($"coalesce(max(r.{seq.ColumnName}), 0)");

//		return Connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
//	}
//}
