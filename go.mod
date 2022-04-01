module github.com/stream-stack/publisher

go 1.16

require (
	github.com/Jille/grpc-multi-resolver v1.1.0
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	github.com/stream-stack/store v0.0.0-20211223063433-55f67d6f769f
	golang.org/x/sys v0.0.0-20211210111614-af8b64212486 // indirect
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.22.5
	k8s.io/apimachinery v0.22.5
	k8s.io/client-go v0.22.5
	knative.dev/eventing v0.29.0
	knative.dev/pkg v0.0.0-20220217155112-d48172451966
)

replace github.com/stream-stack/store v0.0.0-20211223063433-55f67d6f769f => ../store
