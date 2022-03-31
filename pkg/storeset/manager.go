package storeset

import "context"

var subscribeAddCh = make(chan Subscribe, 1)
var subscribeRemoveCh = make(chan Subscribe, 1)

func StartStoreSetManager(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case add := <-AddStoreSetCh:
				_, ok := storesetConns[add.name]
				if ok {
					continue
				}
				conn := NewStoreSetConn(add)
				storesetConns[add.name] = conn
				go conn.Start(ctx)
			case rm := <-RemoveStoreSetCh:
				delete(storesetConns, rm)
			case add := <-subscribeAddCh:
				for _, conn := range storesetConns {
					conn.addSubscribeCh <- add
				}
			case remove := <-subscribeRemoveCh:
				for _, conn := range storesetConns {
					conn.removeSubscribeCh <- remove.name
				}
			}
		}
	}()
	return nil
}
