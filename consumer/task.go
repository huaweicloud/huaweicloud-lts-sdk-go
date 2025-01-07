package consumer

type TaskResult struct {
	err                error
	startTime          string
	endTime            string
	cursorPersistent   bool
	rollBackCheckPoint string
	fetchData          []LogData
	cursor             string
	nextCursor         string
}
