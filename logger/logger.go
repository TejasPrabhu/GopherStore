package logger

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

var Log *logrus.Logger

func init() {
    Log = logrus.New()
    Log.SetFormatter(&logrus.JSONFormatter{})
    Log.SetOutput(os.Stdout)  // Outputs to console; can be set to any io.Writer


    file, err := os.OpenFile("log.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
    if err == nil {
        Log.SetOutput(io.MultiWriter(file, os.Stdout))
    } else {
        Log.Info("Failed to log to file, using default stderr")
    }
}
