using InterlinkMapper.Data;

namespace InterlinkMapper;

public class Database
{
	public string TransctionTableName { get; set; } = "im_transactions";

	public string ProcessTableName { get; set; } = "im_processes";

	public string TransctionIdColumnName { get; set; } = "transaction_id";

	public string ProcessIdColumnName { get; set; } = "process_id";

	public string DestinationIdColumnName { get; set; } = "destination_id";

	public string DatasourceIdColumnName { get; set; } = "datasource_id";

	public string ArgumentsColumnName { get; set; } = "arguments";

	public string PlaceholderIdentifier { get; set; } = ":";

	public Func<Destination, string> ProcessMapNameBuilder { get; set; } = (dest) => dest.Table!.TableFullName + "__proc";

	public Func<Datasource, string> KeyMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__key_" + ds.DatasourceName;

	public Func<Datasource, string> HoldMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__hold_" + ds.DatasourceName;

	public Func<Datasource, string> RelationMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__rel_" + ds.DatasourceName;
}
