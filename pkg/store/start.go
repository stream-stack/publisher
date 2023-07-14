package store

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/common/partition"
	"github.com/stream-stack/common/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"time"
)

func Start(ctx context.Context) error {
	partitionFile := viper.GetString("StorePartitionConfigFile")
	if partitionFile == "" {
		return fmt.Errorf("[store-client]store-partition-config-file not found")
	}
	sets, err := partition.Read(partitionFile)
	if err != nil {
		return err
	}

	StartConsensus(ctx)
	addrs := getStoreAddress(sets)
	if err := startStoreHandlers(ctx, addrs); err != nil {
		return err
	}
	return nil
}

func startStoreHandlers(ctx context.Context, addrs []string) error {
	for _, addr := range addrs {
		if err := startStoreHandler(ctx, addr); err != nil {
			return err
		}
	}
	return nil
}

func startStoreHandler(ctx context.Context, addr string) error {
	logrus.Debugf("[store-client]begin connection to store address:%v", addr)
	//TODO: 接入grpc中间件,重试,opentelemetry
	client, err := grpc.Dial(addr,
		grpc.WithTimeout(time.Second*2),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(`{"HealthCheckConfig": {"ServiceName": "store"}}`),
	)
	if err != nil {
		return err
	}
	go func() {
		defer client.Close()
		select {
		case <-ctx.Done():
			return
		}
	}()

	logrus.Debugf("[store-client]begin sync offset from store address:%v", addr)
	offset, offsetCh, err := startOffsetSyncer(ctx, v1.NewPrivateKeyValueServiceClient(client), addr)
	if err != nil {
		return err
	}
	logrus.Debugf("[store-client]begin subscribe to store address:%v,offset:%v", addr, offset)
	if err := startSubscriber(ctx, offset, v1.NewPublicEventServiceClient(client), offsetCh); err != nil {
		return err
	}
	return nil
}

func startSubscriber(ctx context.Context, offset uint64, client v1.PublicEventServiceClient, ch chan uint64) error {
	subscribe, err := client.Subscribe(ctx, &v1.SubscribeRequest{
		Offset: &offset,
	})
	if err != nil {
		logrus.Errorf("[store-client]subscribe error:%v", err)
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, err := subscribe.Recv()
				if err != nil {
					logrus.Errorf("[store-client]subscribe event error:%v", err)
					return
				}
				logrus.Debugf("[store-client]receive event{id:%s,source:%s},offset:%v",
					event.Event.Id, event.Event.Source, event.Offset)
				BeginConsensus(ctx, event.Event)
				ch <- event.Offset
			}
		}
	}()
	return nil
}

func startOffsetSyncer(ctx context.Context, kc v1.PrivateKeyValueServiceClient, addr string) (uint64, chan uint64, error) {
	offsetName := getPublisherOffsetKey()
	get, err := kc.Get(ctx, &v1.GetRequest{
		Key: offsetName,
	})
	if err != nil {
		return 0, nil, err
	}
	var offset uint64
	if get.Value != nil {
		offset = util.BytesToUint64(get.Value)
	}
	offsetCh := make(chan uint64, 1)
	duration := viper.GetDuration("OffsetSyncInterval")
	ticker := time.NewTicker(duration)
	go func() {
		prevOffset := offset
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case curr := <-offsetCh:
				if curr <= offset {
					continue
				} else {
					offset = curr
				}
			case <-ticker.C:
				if prevOffset == offset {
					logrus.Debugf("[store-client]offset %v not changed", offset)
					continue
				}
				if _, err := kc.Put(ctx, &v1.PutRequest{
					Key:   offsetName,
					Value: util.Uint64ToBytes(offset),
				}); err != nil {
					logrus.Errorf("[store-client]put offset value %v error:%v", err, offset)
				} else {
					logrus.Debugf("[store-client]put offset value %v to %s success", offset, addr)
					prevOffset = offset
				}
			}
		}
	}()
	return offset, offsetCh, nil
}

func getPublisherOffsetKey() []byte {
	hostname, _ := os.Hostname()
	return []byte(fmt.Sprintf("publisher/offset/%v", hostname))
}

func getStoreAddress(sets []*partition.Set) []string {
	result := make([]string, 0)
	for _, set := range sets {
		result = append(result, set.Addrs...)
	}
	return result
}
