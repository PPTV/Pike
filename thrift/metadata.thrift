
namespace java com.pplive.pike.thriftgen.table

enum ColumnType{
	Boolean,
	String,
	Int,
	Double,
	Long,
	Float,
	Complex,
	Map_ObjString
	Byte,
	Short,
	Date,
	Time,
	Timestamp,
}
struct Column{
	1:string name,
	2:string title,
	3:ColumnType columnType,
	4:string columnTypeValue,
}
struct Table{
	1:string name,
	2:string title,
	3:list<Column> columns,
}

service MetadataService {
	Table getTable(1:string name) ;
	list<string> getTableNames();
	i64 getTableBytesByHour(1:string name);	
	map<string,set<string>> getTransferColumns() ;
	/* 1分钟注册一次，如果注册失败，请重试，10个周期注册失败，这个业务字段将丢弃*/
	void registColumns(1:string id,2:string tableName,3:set<string> columns) ;
}
