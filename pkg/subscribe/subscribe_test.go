package subscribe

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"log"
	"testing"
)

func TestCloudEventSend(t *testing.T) {
	ctx := cloudevents.ContextWithTarget(context.Background(), "http://www.baidu.com/")

	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	for i := 0; i < 10; i++ {
		e := cloudevents.NewEvent()
		e.SetType("com.cloudevents.sample.sent")
		e.SetSource("https://github.com/cloudevents/sdk-go/v2/samples/httpb/sender")
		_ = e.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
			"id":      i,
			"message": "Hello, World!",
		})

		res := c.Send(ctx, e)
		//TODO:判断返回是否200
		if cloudevents.IsUndelivered(res) {
			log.Printf("Failed to send: %v", res)
		} else {
			var httpResult *cehttp.Result
			cloudevents.ResultAs(res, &httpResult)
			log.Printf("Sent %d with status code %d", i, httpResult.StatusCode)
		}
	}
}

func TestCloudEventReceive(t *testing.T) {
	ctx := context.Background()
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Printf("will listen on :8080\n")
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, func(ctx context.Context, event cloudevents.Event) {
		fmt.Printf("%s", event)
	}))
}
