package storeset

import (
	"context"
	"fmt"
	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
	"net/http"
)

func StartHttpServer(ctx context.Context, cancel context.CancelFunc) error {
	//start gin http server at manager port
	engine := gin.Default()

	//health
	engine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "healthy")
	})

	go func() {
		select {
		case <-ctx.Done():
			manners.Close()
		}
	}()
	go func() {
		if err := manners.ListenAndServe(managerAddr, engine); err != nil {
			fmt.Printf("failed start manager serve: %v", err)
			cancel()
		}
	}()

	return nil
}
