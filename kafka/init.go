package kafka

import "github.com/sirupsen/logrus"

var Logger *logrus.Logger

func init() {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	logger.SetLevel(logrus.DebugLevel)
	Logger = logger
}
