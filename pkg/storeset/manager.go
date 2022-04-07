package storeset

import "context"

var StoreSetOpCh = make(chan func(ctx context.Context, conns map[string]*StoreSetConn), 1)
var storesetConns = make(map[string]*StoreSetConn)

func StartStoreSetManager(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case op := <-StoreSetOpCh:
				op(ctx, storesetConns)
			}
		}
	}()
}
