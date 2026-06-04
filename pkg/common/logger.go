package common

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

const (
	LakeFlowOperatorRuntimeEnv = "LAKEFLOW_OPERATOR_RUNTIME"
)

var (
	sharedLogger logr.Logger
	once         sync.Once
)

// InitLogger initializes the shared logger once. Should be called from main().
func InitLogger() logr.Logger {
	once.Do(func() {
		sharedLogger = initZapLogger()
	})
	return sharedLogger
}

// GetSharedLogger returns the shared logger instance used across all packages.
// All packages should use this same logger instance.
func GetSharedLogger() logr.Logger {
	if sharedLogger.GetSink() == nil {
		// Fallback: initialize if not already done
		InitLogger()
	}
	return sharedLogger
}

// GetLogger is deprecated in favor of GetSharedLogger.
// Kept for backward compatibility.
func GetLogger(names ...string) logr.Logger {
	logger := GetSharedLogger()
	for _, name := range names {
		logger = logger.WithName(name)
	}
	return logger
}

// NewLogger is deprecated, use GetSharedLogger instead.
// Kept for backward compatibility.
func NewLogger(names []string) logr.Logger {
	return GetLogger(names...)
}

func initZapLogger() logr.Logger {
	runEvnVal, hasEnv := os.LookupEnv(LakeFlowOperatorRuntimeEnv)
	var logConfig zap.Config
	isProd := true
	if hasEnv {
		if strings.Compare(strings.ToLower(runEvnVal), "prod") == 0 {
			logConfig = zap.NewProductionConfig()
		} else {
			logConfig = zap.NewDevelopmentConfig()
			isProd = false
		}
	} else {
		logConfig = zap.NewDevelopmentConfig()
		isProd = false
	}
	logConfig.OutputPaths = []string{"stdout"}
	logConfig.ErrorOutputPaths = []string{"stdout"}
	logConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	fmt.Println("LakeflowController runtime env isProd is", isProd)
	zapLogger, _ := logConfig.Build()

	return zapr.NewLogger(zapLogger)
}
