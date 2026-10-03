package rtpaudio

import "errors"

var (
	// ErrQueueFull indicates a source's bounded receive queue was full.
	ErrQueueFull = errors.New("RTP receive queue full")
	// ErrSourceStopped indicates a packet arrived after the source capture
	// generation was explicitly stopped.
	ErrSourceStopped = errors.New("RTP source stopped")
	// ErrUnknownSource indicates an operation targeted a source with no
	// observed packet.
	ErrUnknownSource = errors.New("unknown RTP source")
	// ErrWrongSSRC indicates a packet changed SSRC without RestartSource.
	ErrWrongSSRC = errors.New("RTP SSRC changed without capture restart")
	// ErrCounterJump indicates a sequence or timestamp jump was too large to
	// extend unambiguously.
	ErrCounterJump = errors.New("RTP counter discontinuity")
	// ErrBeforeStart indicates a packet arrived before its capture generation
	// began.
	ErrBeforeStart = errors.New("RTP packet arrived before capture start")
)
