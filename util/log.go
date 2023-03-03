package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

type LogConfig struct {
	Filename   string     `yaml:"filename"`
	MaxSize    int        `yaml:"maxSize"`
	MaxBackups int        `yaml:"maxBackups"`
	MaxAge     int        `yaml:"maxAge"`
	Zap        zap.Config `yaml:"zap"`
}

func InitLoggerWithConfig(module string, cfg LogConfig) *zap.Logger {
	cfg.Zap.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.Zap.EncoderConfig.EncodeDuration = zapcore.SecondsDurationEncoder
	cfg.Zap.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
	})
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg.Zap.EncoderConfig),
		w,
		zap.InfoLevel,
	)
	if cfg.Zap.InitialFields == nil {
		cfg.Zap.InitialFields = make(map[string]interface{})
	}
	cfg.Zap.InitialFields["module"] = module
	var opts []zap.Option
	if len(cfg.Zap.InitialFields) > 0 {
		fs := make([]zap.Field, 0, len(cfg.Zap.InitialFields))
		keys := make([]string, 0, len(cfg.Zap.InitialFields))
		for k := range cfg.Zap.InitialFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fs = append(fs, zap.Any(k, cfg.Zap.InitialFields[k]))
		}
		opts = append(opts, zap.Fields(fs...))
	}
	opts = append(opts, zap.AddCaller())
	logger := zap.New(core, opts...)
	return logger
}

func InitLogger(logConfig, module string) *zap.Logger {
	f, err := os.Open(logConfig)
	if err != nil {
		log.Panic(err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Panic(err)
	}
	cfgAll := make(map[string]LogConfig)
	err = yaml.Unmarshal(b, &cfgAll)
	if err != nil {
		log.Panic(err)
	}
	var cfg LogConfig
	if v, ok := cfgAll[module]; ok {
		cfg = v
	} else {
		cfg.Filename = module + ".log"
		cfg.MaxAge = 31
		cfg.MaxSize = 512
		cfg.MaxBackups = 7
		cfg.Zap = zap.NewProductionConfig()
	}
	return InitLoggerWithConfig(module, cfg)
}
