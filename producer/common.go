package producer

func GetTimeMs(t int64) int64 {
	return t / 1000 / 1000
}

func GetLogSizeCalculate(log *Log) int {
	sizeInBytes := 4
	logContent := log.GetContents()
	count := len(logContent)
	for i := 0; i < count; i++ {
		sizeInBytes += len(logContent[i].Log)
	}

	return sizeInBytes

}

func GetLogListSize(logList []*Log) int {
	sizeInBytes := 0
	for _, log := range logList {
		sizeInBytes += GetLogSizeCalculate(log)
	}
	return sizeInBytes
}
