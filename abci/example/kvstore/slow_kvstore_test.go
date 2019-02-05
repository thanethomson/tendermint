package kvstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlowKVStoreBasicFunctionality(t *testing.T) {
	app := NewSlowKVStoreApplication()

	// store a value and check that it retrieves it successfully
	key := "abc"
	value := "def"
	testKVStore(t, app, []byte(key+"="+value), key, value)
}

func requireAppWaitTimes(
	t *testing.T, app *SlowKVStoreApplication,
	checkTxMinWait, checkTxMaxWait,
	deliverTxMinWait, deliverTxMaxWait,
	commitMinWait, commitMaxWait,
	queryMinWait, queryMaxWait int) {
	require.Equal(t, checkTxMinWait, app.checkTxMinWait)
	require.Equal(t, checkTxMaxWait, app.checkTxMaxWait)
	require.Equal(t, deliverTxMinWait, app.deliverTxMinWait)
	require.Equal(t, deliverTxMaxWait, app.deliverTxMaxWait)
	require.Equal(t, commitMinWait, app.commitMinWait)
	require.Equal(t, commitMaxWait, app.commitMaxWait)
	require.Equal(t, queryMinWait, app.queryMinWait)
	require.Equal(t, queryMaxWait, app.queryMaxWait)
}

func TestSlowKVStoreAllWait(t *testing.T) {
	app := NewSlowKVStoreApplication()

	// modify all wait times
	key := "allWait"
	value := "100,200"
	r := app.DeliverTx([]byte(key + "=" + value))
	require.False(t, r.IsErr(), r)
	requireAppWaitTimes(t, app, 100, 200, 100, 200, 100, 200, 100, 200)
}

func TestSlowKVStoreCheckTxWait(t *testing.T) {
	app := NewSlowKVStoreApplication()

	key := "checkTxWait"
	value := "100,200"
	r := app.DeliverTx([]byte(key + "=" + value))
	require.False(t, r.IsErr(), r)
	requireAppWaitTimes(t, app, 100, 200, 0, 0, 0, 0, 0, 0)
}

func TestSlowKVStoreDeliverTxWait(t *testing.T) {
	app := NewSlowKVStoreApplication()

	key := "deliverTxWait"
	value := "100,200"
	r := app.DeliverTx([]byte(key + "=" + value))
	require.False(t, r.IsErr(), r)
	requireAppWaitTimes(t, app, 0, 0, 100, 200, 0, 0, 0, 0)
}

func TestSlowKVStoreCommitWait(t *testing.T) {
	app := NewSlowKVStoreApplication()

	key := "commitWait"
	value := "100,200"
	r := app.DeliverTx([]byte(key + "=" + value))
	require.False(t, r.IsErr(), r)
	requireAppWaitTimes(t, app, 0, 0, 0, 0, 100, 200, 0, 0)
}

func TestSlowKVStoreQueryWait(t *testing.T) {
	app := NewSlowKVStoreApplication()

	key := "queryWait"
	value := "100,200"
	r := app.DeliverTx([]byte(key + "=" + value))
	require.False(t, r.IsErr(), r)
	requireAppWaitTimes(t, app, 0, 0, 0, 0, 0, 0, 100, 200)
}

func TestSlowKVStoreInvalidWaitPeriod(t *testing.T) {
	app := NewSlowKVStoreApplication()
	key := "checkTxWait"
	value := "abc,def"
	cr := app.CheckTx([]byte(key + "=" + value))
	require.True(t, cr.IsErr(), cr)
	dr := app.DeliverTx([]byte(key + "=" + value))
	require.True(t, dr.IsErr(), dr)

	value = "100,abc"
	cr = app.CheckTx([]byte(key + "=" + value))
	require.True(t, cr.IsErr(), cr)
	dr = app.DeliverTx([]byte(key + "=" + value))
	require.True(t, dr.IsErr(), dr)

	value = "abc,100"
	cr = app.CheckTx([]byte(key + "=" + value))
	require.True(t, cr.IsErr(), cr)
	dr = app.DeliverTx([]byte(key + "=" + value))
	require.True(t, dr.IsErr(), dr)
}
