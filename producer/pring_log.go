package producer

import (
	"fmt"
	"path"
	"runtime"

	"github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type LogConf struct {
	Dir     string `yaml:"dir"`
	Name    string `yaml:"name"`
	Level   string `yaml:"level"`
	MaxSize int    `yaml:"max_size"`
}

func InitLoggerStd(config LogConf) {
	configBase(config)
}

func InitLoggerFile(config LogConf) {
	configBase(config)

	logger := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%v/%v", config.Dir, config.Name), // 日志输出文件路径。
		MaxSize:    config.MaxSize,                                // 日志文件最大 size(MB)，缺省 100MB。
		MaxBackups: 10,                                            // 最大过期日志保留的个数。
		MaxAge:     30,                                            // 保留过期文件的最大时间间隔，单位是天。
		LocalTime:  true,                                          // 是否使用本地时间来命名备份的日志。
	}
	logrus.SetOutput(logger)
}

func configBase(config LogConf) {
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			//处理文件名
			fileName := path.Base(frame.File)
			return frame.Function, fmt.Sprintf("%s:%d", fileName, frame.Line)
		},
	})
	switch config.Level {
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	}
	logrus.SetReportCaller(true) // 打印文件、行号和主调函数。
}
