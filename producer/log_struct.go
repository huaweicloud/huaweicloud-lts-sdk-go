package producer

type StructLog struct {
	Time     int64               `json:"time"`
	LineNum  int64               `json:"lineNum"`
	Contents []map[string]string `json:"contents"`
}

type StructLogs struct {
	Logs    []*StructLog      `json:"logs"`
	Labels  map[string]string `json:"labels,omitempty"`
	Source  string            `json:"source,omitempty"`
	Path    string            `json:"path,omitempty"`
	Version string            `json:"version,omitempty"`
}

func (m *StructLogs) Size() (n int) {
	size := 0
	for _, list := range m.Logs {
		for _, log := range list.Contents {
			for key, value := range log {
				size += len(key)
				size += len(value)
			}
		}
	}
	return size
}
