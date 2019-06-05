package pb

const ProtocolVersion = 1

//NewRecord creates a record 
func NewRecord(key string, data []byte) *Record {
	var rec = new(Record)
	rec.Key = key
	rec.Data = data
	return rec
}
// NewReply creates a reply 
func NewReply(status DBReply_Status, record *Record) *DBReply {
	var reply = new(DBReply)
	reply.Version = ProtocolVersion
	reply.Status = status
	reply.Record = record
	return reply
}

//NewRequest creates a request
func NewRequest(verb DBRequest_Verb, dbName, tableName, startKey string, data []byte) *DBRequest {
	request := new(DBRequest)
	request.Version = ProtocolVersion
	request.Verb = verb
	request.DbName = dbName
	request.TableName = tableName
	request.StartKey = startKey
	request.Data=data
	return request;
}

//Success determines if the reply status code represents success or not
func Success(status DBReply_Status) bool {
	if status == DBReply_OK || status == DBReply_Success {
		return true
	}
	return false
}