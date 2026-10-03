package mux

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

// StartupError means startup could not establish a safe, synchronized companion connection.
type StartupError struct{ msg string }

func (e *StartupError) Error() string { return e.msg }

// Startup synchronizes a newly connected companion before clients are admitted.
//
// Native TCP retains at most four outgoing frames across client replacement.
// Five consecutive SELF_INFO replies followed by DEVICE_INFO therefore prove
// that we crossed the newly submitted handshake, not just stale output. The
// temporary flood scope is then reset to the default.
type Startup struct {
	SelfKey               []byte
	DeviceInfo            []byte
	DownstreamDeviceInfo  []byte
	UpstreamProtocolLevel byte
	ExposedProtocolLevel  byte
	Deadline              time.Duration
	selfRun               int
	awaitingScope         bool
	ready                 bool
}

// NewStartup creates a startup fence with an absolute deadline.
func NewStartup(now, timeout time.Duration) *Startup {
	return &Startup{Deadline: now + timeout}
}

// StartupProbes returns five APP_START commands (fencing the four-frame stale
// queue) followed by DEVICE_QUERY at the mux-owned protocol target.
func StartupProbes() [][]byte {
	probes := make([][]byte, 0, 6)
	for range 5 {
		probes = append(probes, AppStartPayload("meshcore-mux"))
	}
	return append(probes, DeviceQueryPayload(UpstreamAppTarget))
}

// Ready reports whether the fence and scope reset completed.
func (s *Startup) Ready() bool { return s.ready }

// CheckDeadline fails once the startup deadline has passed.
func (s *Startup) CheckDeadline(now time.Duration) error {
	if !s.ready && now >= s.Deadline {
		return &StartupError{"startup synchronization timeout"}
	}
	return nil
}

// Receive consumes one companion payload and returns the next internal command, if any.
func (s *Startup) Receive(p []byte, now time.Duration) ([]byte, error) {
	if err := s.CheckDeadline(now); err != nil {
		return nil, err
	}
	if len(p) == 0 {
		return nil, &StartupError{"empty startup response"}
	}
	if p[0] >= PushAdvert {
		return nil, nil
	}
	if s.ready {
		return nil, &StartupError{"ordinary response after startup boundary"}
	}
	switch {
	case s.awaitingScope:
		if !bytes.Equal(p, []byte{RespOk}) {
			return nil, &StartupError{"startup scope reset rejected"}
		}
		s.ready = true
	case p[0] == RespSelfInfo:
		key, err := ValidateSelfInfo(p)
		if err != nil {
			return nil, &StartupError{err.Error()}
		}
		s.SelfKey = key
		s.selfRun++
	case p[0] == RespDeviceInfo && s.selfRun >= 5:
		level, err := ValidateDeviceInfo(p)
		if err != nil {
			return nil, &StartupError{err.Error()}
		}
		s.UpstreamProtocolLevel = level
		s.DownstreamDeviceInfo, _ = DownstreamDeviceInfo(p)
		s.ExposedProtocolLevel = s.DownstreamDeviceInfo[1]
		s.DeviceInfo = dup(p)
		s.awaitingScope = true
		// SET_FLOOD_SCOPE_KEY mode zero selects the companion's configured default.
		return dup(defaultScope), nil
	default:
		s.selfRun = 0
		s.SelfKey = nil
	}
	return nil, nil
}

// Identification describes public firmware identification fields (never the PIN).
func (s *Startup) Identification() string {
	info := s.DeviceInfo
	if info == nil {
		return "device has not been identified"
	}
	field := func(off, n int) string {
		b := info[off : off+n]
		if i := bytes.IndexByte(b, 0); i >= 0 {
			b = b[:i]
		}
		return strconv.Quote(string(b))
	}
	return fmt.Sprintf("profile=companion_v14 upstream_protocol=%d exposed_protocol=%d app_target=%d compatibility_mode=%t model=%s firmware=%s build=%s",
		s.UpstreamProtocolLevel, s.ExposedProtocolLevel, UpstreamAppTarget, info[1] > MaxExposedProtocolLevel,
		field(20, 40), field(60, 20), field(8, 12))
}
