package internal

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/tendermint/tendermint/crypto/tmhash"

	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/state"

	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/types"
)

// Test harness error codes (which act as exit codes when the test harness fails).
const (
	NoError int = iota
	ErrMaxAcceptRetriesReached
	ErrFailedToExpandPath
	ErrFailedToLoadGenesisFile
	ErrFailedToCreateListener
	ErrFailedToStartListener
	ErrInterrupted
	ErrOther
	ErrTestPublicKeyFailed
	ErrTestSignProposalFailed
	ErrTestSignVoteFailed
)

var voteTypes = []types.SignedMsgType{types.PrevoteType, types.PrecommitType}

// TestHarnessError allows us to keep track of which exit code should be used
// when exiting the main program.
type TestHarnessError struct {
	Code int    // The exit code to return
	Err  error  // The original error
	Info string // Any additional information
}

var _ error = (*TestHarnessError)(nil)

// TestHarness allows for testing of a remote signer to ensure compatibility
// with this version of Tendermint.
type TestHarness struct {
	addr             string
	sc               *privval.SocketVal
	fpv              *privval.FilePV
	chainID          string
	acceptRetries    int
	logger           log.Logger
	exitWhenComplete bool
	exitCode         int
}

// TestHarnessConfig provides configuration to set up a remote signer test
// harness.
type TestHarnessConfig struct {
	BindAddr string

	KeyFile     string
	StateFile   string
	GenesisFile string

	AcceptDeadline time.Duration
	ConnDeadline   time.Duration
	AcceptRetries  int

	SecretConnKey ed25519.PrivKeyEd25519

	ExitWhenComplete bool // Whether or not to call os.Exit when the harness has completed.
}

// timeoutError can be used to check if an error returned from the netp package
// was due to a timeout.
type timeoutError interface {
	Timeout() bool
}

var cdc = amino.NewCodec()

// NewTestHarness will load Tendermint data from the given files (including
// validator public/private keypairs and chain details) and create a new
// harness.
func NewTestHarness(logger log.Logger, cfg TestHarnessConfig) (*TestHarness, error) {
	var err error
	var keyFile, stateFile, genesisFile string

	if keyFile, err = expandPath(cfg.KeyFile); err != nil {
		return nil, newTestHarnessError(ErrFailedToExpandPath, err, cfg.KeyFile)
	}
	if stateFile, err = expandPath(cfg.StateFile); err != nil {
		return nil, newTestHarnessError(ErrFailedToExpandPath, err, cfg.StateFile)
	}
	logger.Info("Loading private validator configuration", "keyFile", keyFile, "stateFile", stateFile)
	// NOTE: LoadFilePV ultimately calls os.Exit on failure. No error will be
	// returned if this call fails.
	fpv := privval.LoadFilePV(keyFile, stateFile)

	if genesisFile, err = expandPath(cfg.GenesisFile); err != nil {
		return nil, newTestHarnessError(ErrFailedToExpandPath, err, cfg.GenesisFile)
	}
	logger.Info("Loading chain ID from genesis file", "genesisFile", genesisFile)
	st, err := state.MakeGenesisDocFromFile(genesisFile)
	if err != nil {
		return nil, newTestHarnessError(ErrFailedToLoadGenesisFile, err, genesisFile)
	}
	logger.Info("Loaded genesis file", "chainID", st.ChainID)

	sc, err := newTestHarnessSocketVal(logger, cfg)
	if err != nil {
		return nil, newTestHarnessError(ErrFailedToCreateListener, err, "")
	}

	return &TestHarness{
		addr:             cfg.BindAddr,
		sc:               sc,
		fpv:              fpv,
		chainID:          st.ChainID,
		acceptRetries:    cfg.AcceptRetries,
		logger:           logger,
		exitWhenComplete: cfg.ExitWhenComplete,
		exitCode:         0,
	}, nil
}

// Run will execute the tests associated with this test harness. The intention
// here is to call this from one's `main` function, as the way it succeeds or
// fails at present is to call os.Exit() with an exit code related to the error
// that caused the tests to fail, or exit code 0 on success.
func (th *TestHarness) Run() {
	th.logger.Info("Starting test harness")
	donec := make(chan struct{})
	go func() {
		defer close(donec)
		accepted := false
		var startErr error
		for acceptRetries := th.acceptRetries; acceptRetries > 0; acceptRetries-- {
			th.logger.Info("Attempting to accept incoming connection", "acceptRetries", acceptRetries)
			if err := th.sc.Start(); err != nil {
				// if it wasn't a timeout error
				if _, ok := err.(timeoutError); !ok {
					th.logger.Error("Failed to start listener", "err", err)
					th.Shutdown(newTestHarnessError(ErrFailedToStartListener, err, ""))
					// we need the return statements in case this is being run
					// from a unit test - otherwise this function will just die
					// when os.Exit is called
					return
				}
				startErr = err
			} else {
				accepted = true
				break
			}
		}
		if !accepted {
			th.logger.Error("Maximum accept retries reached", "acceptRetries", th.acceptRetries)
			th.Shutdown(newTestHarnessError(ErrMaxAcceptRetriesReached, startErr, ""))
			return
		}

		// Run the tests
		if err := th.TestPublicKey(); err != nil {
			th.Shutdown(err)
			return
		}
		if err := th.TestSignProposal(); err != nil {
			th.Shutdown(err)
			return
		}
		if err := th.TestSignVote(); err != nil {
			th.Shutdown(err)
			return
		}
		th.logger.Info("SUCCESS! All tests passed.")
		th.Shutdown(nil)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			th.logger.Info("Caught interrupt, terminating...", "sig", sig)
			th.Shutdown(newTestHarnessError(ErrInterrupted, nil, ""))
		}
	}()

	// Run until complete
	<-donec
}

// TestPublicKey just validates that we can (1) fetch the public key from the
// remote signer, and (2) it matches the public key we've configured for our
// local Tendermint version.
func (th *TestHarness) TestPublicKey() error {
	th.logger.Info("TEST: Public key of remote signer")
	th.logger.Info("Local", "pubKey", th.fpv.GetPubKey())
	th.logger.Info("Remote", "pubKey", th.sc.GetPubKey())
	if th.fpv.GetPubKey() != th.sc.GetPubKey() {
		th.logger.Error("FAILED: Local and remote public keys do not match")
		return newTestHarnessError(ErrTestPublicKeyFailed, nil, "")
	}
	return nil
}

// TestSignProposal makes sure the remote signer can successfully sign
// proposals.
func (th *TestHarness) TestSignProposal() error {
	th.logger.Info("TEST: Signing of proposals")
	// sha256 hash of "hash"
	hash := tmhash.Sum([]byte("hash"))
	prop := &types.Proposal{
		Type:     types.ProposalType,
		Height:   12345,
		Round:    23456,
		POLRound: -1,
		BlockID: types.BlockID{
			Hash: hash,
			PartsHeader: types.PartSetHeader{
				Hash:  hash,
				Total: 1000000,
			},
		},
		Timestamp: time.Now(),
	}
	propBytes := prop.SignBytes(th.chainID)
	if err := th.sc.SignProposal(th.chainID, prop); err != nil {
		th.logger.Error("FAILED: Signing of proposal", "err", err)
		return newTestHarnessError(ErrTestSignProposalFailed, err, "")
	}
	th.logger.Debug("Signed proposal", "prop", prop)
	// first check that it's a basically valid proposal
	if err := prop.ValidateBasic(); err != nil {
		th.logger.Error("FAILED: Signed proposal is invalid", "err", err)
		return newTestHarnessError(ErrTestSignProposalFailed, err, "")
	}
	// now validate the signature on the proposal
	if th.sc.GetPubKey().VerifyBytes(propBytes, prop.Signature) {
		th.logger.Info("Successfully validated proposal signature")
	} else {
		th.logger.Error("FAILED: Proposal signature validation failed")
		return newTestHarnessError(ErrTestSignProposalFailed, nil, "signature validation failed")
	}
	return nil
}

// TestSignVote makes sure the remote signer can successfully sign all kinds of
// votes.
func (th *TestHarness) TestSignVote() error {
	th.logger.Info("TEST: Signing of votes")
	for _, voteType := range voteTypes {
		th.logger.Info("Testing vote type", "type", voteType)
		hash := tmhash.Sum([]byte("hash"))
		vote := &types.Vote{
			Type:   voteType,
			Height: 12345,
			Round:  23456,
			BlockID: types.BlockID{
				Hash: hash,
				PartsHeader: types.PartSetHeader{
					Hash:  hash,
					Total: 1000000,
				},
			},
			ValidatorIndex:   0,
			ValidatorAddress: tmhash.SumTruncated([]byte("addr")),
			Timestamp:        time.Now(),
		}
		voteBytes := vote.SignBytes(th.chainID)
		// sign the vote
		if err := th.sc.SignVote(th.chainID, vote); err != nil {
			th.logger.Error("FAILED: Signing of vote", "err", err)
			return newTestHarnessError(ErrTestSignVoteFailed, err, fmt.Sprintf("voteType=%d", voteType))
		}
		th.logger.Debug("Signed vote", "vote", vote)
		// validate the contents of the vote
		if err := vote.ValidateBasic(); err != nil {
			th.logger.Error("FAILED: Signed vote is invalid", "err", err)
			return newTestHarnessError(ErrTestSignVoteFailed, err, fmt.Sprintf("voteType=%d", voteType))
		}
		// now validate the signature on the proposal
		if th.sc.GetPubKey().VerifyBytes(voteBytes, vote.Signature) {
			th.logger.Info("Successfully validated vote signature", "type", voteType)
		} else {
			th.logger.Error("FAILED: Vote signature validation failed", "type", voteType)
			return newTestHarnessError(ErrTestSignVoteFailed, nil, "signature validation failed")
		}
	}
	return nil
}

// Shutdown will kill the test harness and attempt to close all open sockets
// gracefully. If the supplied error is nil, it is assumed that the exit code
// should be 0. If err is not nil, it will exit with an exit code related to the
// error.
func (th *TestHarness) Shutdown(err error) {
	var exitCode int

	if err == nil {
		exitCode = NoError
	} else if therr, ok := err.(*TestHarnessError); ok {
		exitCode = therr.Code
	} else {
		exitCode = ErrOther
	}
	th.exitCode = exitCode

	if th.sc.IsRunning() {
		// best effort request to shut the remote signer down
		th.logger.Info("Attempting to stop remote signer")
		if err := th.sc.SendPoisonPill(); err != nil {
			th.logger.Error("Failed to send poison pill message to remote signer", "err", err)
		}
	}

	// in case sc.Stop() takes too long
	if th.exitWhenComplete {
		go func() {
			time.Sleep(time.Duration(5) * time.Second)
			th.logger.Error("Forcibly exiting program after timeout")
			os.Exit(exitCode)
		}()
	}

	if th.sc.IsRunning() {
		if err := th.sc.Stop(); err != nil {
			th.logger.Error("Failed to cleanly stop listener: %s", err.Error())
		}
	}

	if th.exitWhenComplete {
		os.Exit(exitCode)
	}
}

// expandPath will check if the given path begins with a "~" symbol, and if so,
// will expand it to become the user's home directory.
func expandPath(path string) (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}

	if path == "~" {
		return usr.HomeDir, nil
	} else if strings.HasPrefix(path, "~/") {
		return filepath.Join(usr.HomeDir, path[2:]), nil
	}

	return path, nil
}

// newTestHarnessSocketVal creates our client instance which we will use for
// testing.
func newTestHarnessSocketVal(logger log.Logger, cfg TestHarnessConfig) (*privval.SocketVal, error) {
	proto, addr := cmn.ProtocolAndAddress(cfg.BindAddr)
	if proto == "unix" {
		// make sure the socket doesn't exist - if so, try to delete it
		if cmn.FileExists(addr) {
			if err := os.Remove(addr); err != nil {
				logger.Error("Failed to remove existing Unix domain socket", "addr", addr)
				return nil, err
			}
		}
	}
	ln, err := net.Listen(proto, addr)
	logger.Info("Listening at", "proto", proto, "addr", addr)
	if err != nil {
		return nil, err
	}
	var svln net.Listener
	if proto == "unix" {
		unixLn := privval.NewUnixListener(ln)
		privval.UnixListenerAcceptDeadline(cfg.AcceptDeadline)(unixLn)
		privval.UnixListenerConnDeadline(cfg.ConnDeadline)(unixLn)
		svln = unixLn
	} else {
		tcpLn := privval.NewTCPListener(ln, cfg.SecretConnKey)
		privval.TCPListenerAcceptDeadline(cfg.AcceptDeadline)(tcpLn)
		privval.TCPListenerConnDeadline(cfg.ConnDeadline)(tcpLn)
		logger.Info("Resolved TCP address for listener", "addr", tcpLn.Addr())
		svln = tcpLn
	}
	return privval.NewSocketVal(logger, svln), nil
}

func newTestHarnessError(code int, err error, info string) *TestHarnessError {
	return &TestHarnessError{
		Code: code,
		Err:  err,
		Info: info,
	}
}

func (e *TestHarnessError) Error() string {
	var msg string
	switch e.Code {
	case ErrMaxAcceptRetriesReached:
		msg = "Maximum accept retries reached"
	case ErrFailedToExpandPath:
		msg = "Failed to expand path"
	case ErrFailedToCreateListener:
		msg = "Failed to create listener"
	case ErrFailedToStartListener:
		msg = "Failed to start listener"
	case ErrInterrupted:
		msg = "Interrupted"
	case ErrTestPublicKeyFailed:
		msg = "Public key validation test failed"
	case ErrTestSignProposalFailed:
		msg = "Proposal signing validation test failed"
	default:
		msg = "Unknown error"
	}
	if len(e.Info) > 0 {
		msg = fmt.Sprintf("%s: %s", msg, e.Info)
	}
	if e.Err != nil {
		msg = fmt.Sprintf("%s (original error: %s)", msg, e.Err.Error())
	}
	return msg
}
