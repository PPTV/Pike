
namespace java com.pplive.pike.thriftgen.datatransfer

struct Row{
	1:list<string> columns,
}

service TransferService {
	bool send(1:list<Row> rows,2:i64 dateTime, 3:string id) ;
}
