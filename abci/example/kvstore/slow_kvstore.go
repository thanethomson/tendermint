package kvstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/tendermint/tendermint/abci/example/code"
	"github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
)

// SlowKVStoreApplication represents a test harness ABCI app, based on the
// in-memory key/value store ABCI app, that allows for interference with
// response times. This allows for testing of tolerances of just how synchronous
// ABCI applications need to be in order for the network to function correctly.
type SlowKVStoreApplication struct {
	app *KVStoreApplication

	checkTxMinWait   int
	checkTxMaxWait   int
	deliverTxMinWait int
	deliverTxMaxWait int
	commitMinWait    int
	commitMaxWait    int
	queryMinWait     int
	queryMaxWait     int

	checkTxWait   func()
	deliverTxWait func()
	commitWait    func()
	queryWait     func()
}

var _ types.Application = (*SlowKVStoreApplication)(nil)

var validSlowKVStoreWaitKeys = map[string]string{
	"checkTxWait":   "",
	"deliverTxWait": "",
	"commitWait":    "",
	"queryWait":     "",
	"allWait":       "",
}

// NewSlowKVStoreApplication allows us to create a KVStoreApplication which, at
// least initially, operates at the same speed as a normal KVStoreApplication.
func NewSlowKVStoreApplication() *SlowKVStoreApplication {
	return &SlowKVStoreApplication{
		app:           NewKVStoreApplication(),
		checkTxWait:   func() {},
		deliverTxWait: func() {},
		commitWait:    func() {},
		queryWait:     func() {},
	}
}

func createWaitFn(minWait, maxWait int) func() {
	// swap the values if minWait > maxWait
	if minWait > maxWait {
		t := minWait
		minWait = maxWait
		maxWait = t
	}
	// rather keep the if statements outside of the wait function, as it's most
	// likely going to be called often
	if minWait == maxWait {
		if minWait > 0 {
			return func() {
				time.Sleep(time.Duration(minWait) * time.Millisecond)
			}
		} else {
			// no-op
			return func() {}
		}
	} else {
		// randomly distributed between the two extremes
		return func() {
			time.Sleep(time.Duration(minWait+int(rand.Int31n(int32(maxWait-minWait)))) * time.Millisecond)
		}
	}
}

func (app *SlowKVStoreApplication) InitChain(req types.RequestInitChain) types.ResponseInitChain {
	return app.app.InitChain(req)
}

func (app *SlowKVStoreApplication) Info(req types.RequestInfo) types.ResponseInfo {
	return app.app.Info(req)
}

func (app *SlowKVStoreApplication) SetOption(req types.RequestSetOption) types.ResponseSetOption {
	return app.app.SetOption(req)
}

func (app *SlowKVStoreApplication) DeliverTx(tx []byte) types.ResponseDeliverTx {
	var key, value []byte
	parts := bytes.Split(tx, []byte("="))
	if len(parts) == 2 {
		key, value = parts[0], parts[1]
	} else {
		key, value = tx, tx
	}

	skey := string(key)
	if _, ok := validSlowKVStoreWaitKeys[skey]; ok {
		values := bytes.Split(value, []byte(","))
		if len(values) != 2 {
			return types.ResponseDeliverTx{Code: code.CodeTypeEncodingError, Log: "invalid min/max response time format"}
		}
		minWait, err := strconv.Atoi(string(values[0]))
		if err != nil {
			return types.ResponseDeliverTx{Code: code.CodeTypeEncodingError, Log: "invalid minimum response time"}
		}
		maxWait, err := strconv.Atoi(string(values[1]))
		if err != nil {
			return types.ResponseDeliverTx{Code: code.CodeTypeEncodingError, Log: "invalid maximum response time"}
		}
		switch skey {
		case "checkTxWait":
			app.checkTxWait, app.checkTxMinWait, app.checkTxMaxWait = createWaitFn(minWait, maxWait), minWait, maxWait
		case "deliverTxWait":
			app.deliverTxWait, app.deliverTxMinWait, app.deliverTxMaxWait = createWaitFn(minWait, maxWait), minWait, maxWait
		case "commitWait":
			app.commitWait, app.commitMinWait, app.commitMaxWait = createWaitFn(minWait, maxWait), minWait, maxWait
		case "queryWait":
			app.queryWait, app.queryMinWait, app.queryMaxWait = createWaitFn(minWait, maxWait), minWait, maxWait
		case "allWait":
			app.checkTxWait, app.checkTxMinWait, app.checkTxMaxWait = createWaitFn(minWait, maxWait), minWait, maxWait
			app.deliverTxWait, app.deliverTxMinWait, app.deliverTxMaxWait = app.checkTxWait, minWait, maxWait
			app.commitWait, app.commitMinWait, app.commitMaxWait = app.checkTxWait, minWait, maxWait
			app.queryWait, app.queryMinWait, app.queryMaxWait = app.checkTxWait, minWait, maxWait
		}
		return types.ResponseDeliverTx{
			Code: code.CodeTypeOK,
			Log:  fmt.Sprintf("set %s minWait = %d, maxWait = %d", skey, minWait, maxWait),
		}
	}

	app.app.state.db.Set(prefixKey(key), value)
	app.app.state.Size++

	tags := []cmn.KVPair{
		{Key: []byte("app.creator"), Value: []byte("Cosmoshi Netowoko")},
		{Key: []byte("app.key"), Value: key},
	}
	app.deliverTxWait()
	return types.ResponseDeliverTx{Code: code.CodeTypeOK, Tags: tags}
}

func (app *SlowKVStoreApplication) BeginBlock(req types.RequestBeginBlock) types.ResponseBeginBlock {
	return app.app.BeginBlock(req)
}

func (app *SlowKVStoreApplication) EndBlock(req types.RequestEndBlock) types.ResponseEndBlock {
	return app.app.EndBlock(req)
}

func (app *SlowKVStoreApplication) CheckTx(tx []byte) types.ResponseCheckTx {
	var key, value []byte
	parts := bytes.Split(tx, []byte("="))
	if len(parts) == 2 {
		key, value = parts[0], parts[1]
	} else {
		key, value = tx, tx
	}

	skey := string(key)
	if _, ok := validSlowKVStoreWaitKeys[skey]; ok {
		values := bytes.Split(value, []byte(","))
		if len(values) != 2 {
			return types.ResponseCheckTx{Code: code.CodeTypeEncodingError, Log: "invalid min/max response time format"}
		}
		if _, err := strconv.Atoi(string(values[0])); err != nil {
			return types.ResponseCheckTx{Code: code.CodeTypeEncodingError, Log: "invalid minimum response time"}
		}
		if _, err := strconv.Atoi(string(values[1])); err != nil {
			return types.ResponseCheckTx{Code: code.CodeTypeEncodingError, Log: "invalid maximum response time"}
		}
		return types.ResponseCheckTx{Code: code.CodeTypeOK, GasWanted: 1}
	}

	app.checkTxWait()
	return app.app.CheckTx(tx)
}

func (app *SlowKVStoreApplication) Commit() types.ResponseCommit {
	app.commitWait()
	return app.app.Commit()
}

func (app *SlowKVStoreApplication) Query(reqQuery types.RequestQuery) types.ResponseQuery {
	app.queryWait()
	return app.app.Query(reqQuery)
}
