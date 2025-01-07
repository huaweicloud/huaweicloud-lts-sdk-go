package producer

type LogTag struct {
	Key   *string `json:"Key,omitempty"`
	Value *string `json:"Value,omitempty"`
}

type Log struct {
	Time      *uint32       `json:"time,omitempty"`
	Contents  []*LogContent `json:"contents,omitempty"`
	Labels    string        `json:"labels"`
	ProjectId string        `json:"tenant_project_id,omitempty"`
}

type LogGroup struct {
	Logs     []*Log    `json:"log_items,omitempty"`
	Category *string   `json:"Category,omitempty"`
	LogTags  []*LogTag `json:"LogTags,omitempty"`
}

func (m *Log) GetContents() []*LogContent {
	if m != nil {
		return m.Contents
	}
	return nil
}

type LogContent struct {
	LogTimeNs int64  `json:"log_time_ns"`
	Log       string `json:"log"`
}

type LogItem struct {
	Contents        []LogContent `json:"contents"`
	Labels          string       `json:"labels"`
	TenantProjectId string       `json:"tenant_project_id"`
}

type LogItems struct {
	LogItems []LogItem `json:"log_items"`
}

func (m *LogContent) Size() (n int) {
	if nil != m {
		return len(m.Log) + sovLog(uint64(m.LogTimeNs))
	}
	return 0
}

func (m *Log) Size() (n int) {
	var l int
	_ = l
	if m.Time != nil {
		n += 1 + sovLog(uint64(*m.Time))
	}
	if len(m.Contents) > 0 {
		for _, e := range m.Contents {
			l = e.Size()
			n += 1 + l + sovLog(uint64(l))
		}
	}
	return n
}

func (m *LogTag) Size() (n int) {
	var l int
	_ = l
	if m.Key != nil {
		l = len(*m.Key)
		n += 1 + l + sovLog(uint64(l))
	}
	if m.Value != nil {
		l = len(*m.Value)
		n += 1 + l + sovLog(uint64(l))
	}

	return n
}

func (m *LogGroup) Size() (n int) {
	var l int
	_ = l
	if len(m.Logs) > 0 {
		for _, e := range m.Logs {
			l = e.Size()
			n += 1 + l + sovLog(uint64(l))
		}
	}
	if m.Category != nil {
		l = len(*m.Category)
		n += 1 + l + sovLog(uint64(l))
	}
	if len(m.LogTags) > 0 {
		for _, e := range m.LogTags {
			l = e.Size()
			n += 1 + l + sovLog(uint64(l))
		}
	}

	return n
}

func sovLog(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (m *LogGroup) GetLogs() []*Log {
	if m != nil {
		return m.Logs
	}
	return nil
}
