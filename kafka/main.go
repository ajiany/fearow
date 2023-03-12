package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var Finished = errors.New("Finished")

func KafkaDemo() {
	app := cli.NewApp()
	app.Name = "Kafka Demo"

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	webCmds := []*cli.Command{
		{
			Name:  "web",
			Usage: "start web server",
			Action: func(c *cli.Context) error {
				WebServer(c.Context)
				return nil
			},
		},
	}

	listenerCmds := []*cli.Command{
		{
			Name:  "kafka",
			Usage: "start kafka server",
			Action: func(c *cli.Context) error {
				Logger.Debugf("start kafka consumer client success")
				ConsumerServer(c.Context)
				return nil
			},
		},
	}

	app.Commands = append(app.Commands, webCmds...)
	app.Commands = append(app.Commands, listenerCmds...)

	g.Go(func() error {
		defer cancel()

		if err := app.RunContext(ctx, os.Args); err != nil {
			return err
		}
		return Finished
	})

	g.Go(func() error {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-sig:
			cancel()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	if err := g.Wait(); err != nil {
		if err != Finished {
			logrus.Fatal(err)
		}
	}

	log.Println("Server exiting")
}

func WebServer(ctx context.Context) error {

	router := gin.Default()

	api := router.Group("/api")
	api.GET("/ping", func(c *gin.Context) {
		time.Sleep(5 * time.Second)
		c.String(http.StatusOK, "Welcome Gin Server")
	})
	api.POST("/send", func(c *gin.Context) {
		msg := make(map[string]interface{}, 0)
		c.ShouldBind(&msg)
		data, _ := json.Marshal(msg)
		err := ProduceMessage(KafkaTopic, data)
		if err != nil {
			return
		}
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// 关闭 web 服务
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(ctx); err != nil {
			Logger.Fatal("Server Shutdown:", err)
		}
	}()

	// 服务连接
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s\n", err)
		return err
	}
	return nil
}
