package mux

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestFrameRoundTripAcrossSplitReads(t *testing.T) {
	a, _ := EncodeFrame([]byte{1, 2, 3}, ClientToCompanionMarker)
	b, _ := EncodeFrame([]byte{9}, ClientToCompanionMarker)
	stream := append(a, b...)
	d := NewDecoder(ClientToCompanionMarker, time.Second)
	var got [][]byte
	for i := range stream {
		out, err := d.Feed(stream[i:i+1], 0)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, out...)
	}
	if len(got) != 2 || !bytes.Equal(got[0], []byte{1, 2, 3}) || !bytes.Equal(got[1], []byte{9}) {
		t.Fatalf("unexpected payloads %v", got)
	}
	if d.Partial() {
		t.Fatal("decoder should be at a frame boundary")
	}
}

func TestFrameRejectsWrongMarkerAndBadLength(t *testing.T) {
	var fe *FrameError
	if _, err := NewDecoder(ClientToCompanionMarker, time.Second).Feed([]byte{'>', 1, 0, 0}, 0); !errors.As(err, &fe) {
		t.Fatalf("expected frame error, got %v", err)
	}
	if _, err := NewDecoder(ClientToCompanionMarker, time.Second).Feed([]byte{'<', 0, 0}, 0); !errors.As(err, &fe) {
		t.Fatalf("expected empty frame error, got %v", err)
	}
	if _, err := NewDecoder(ClientToCompanionMarker, time.Second).Feed([]byte{'<', 177, 0}, 0); !errors.As(err, &fe) {
		t.Fatalf("expected oversize error, got %v", err)
	}
	if _, err := EncodeFrame(make([]byte, 177), '<'); err == nil {
		t.Fatal("oversized encode should fail")
	}
}

func TestFrameAssemblyDeadlineAndTruncation(t *testing.T) {
	d := NewDecoder(ClientToCompanionMarker, time.Second)
	if _, err := d.Feed([]byte{'<', 5}, 0); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckDeadline(999 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckDeadline(time.Second); err == nil {
		t.Fatal("expected deadline error")
	}
	if err := d.Finish(); err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestCommandValidation(t *testing.T) {
	cases := []struct {
		payload []byte
		reason  byte
	}{
		{[]byte{CmdSyncNextMessage}, 0},
		{[]byte{CmdReboot, 'r', 'e', 'b', 'o', 'o', 't'}, 0},
		{[]byte{CmdReboot, 'x'}, ErrIllegalArg},
		{[]byte{0xee}, ErrUnsupportedCmd},
		{[]byte{CmdGetStats, 3}, ErrIllegalArg},
		{[]byte{CmdSetFloodScopeKey, 1}, 0},
		{append([]byte{CmdSendTelemetryReq}, make([]byte, 3)...), 0},
	}
	for _, c := range cases {
		if _, r := ValidateCommand(c.payload); r != c.reason {
			t.Errorf("ValidateCommand(%x) = %d, want %d", c.payload, r, c.reason)
		}
	}
	if Descriptor(append([]byte{CmdSendTelemetryReq}, make([]byte, 35)...)) != remoteTelemetryDescriptor {
		t.Error("remote telemetry should use the remote-lease descriptor")
	}
}

func TestDowngradeInbox(t *testing.T) {
	v3 := []byte{RespChannelMessageV3, 0xf0, 0, 0, 2, 0xff, 0, 1, 2, 3, 4, 'h', 'i'}
	got, err := DowngradeInbox(v3, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{RespChannelMessage, 2, 0xff, 0, 1, 2, 3, 4, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
	if same, _ := DowngradeInbox(v3, 3); !bytes.Equal(same, v3) {
		t.Fatal("V3 client should receive the original frame")
	}
}

func TestDownstreamDeviceInfoCapsProtocol(t *testing.T) {
	info := deviceInfo(20)
	info = append(info, 0x55, 0x55)
	got, err := DownstreamDeviceInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != DeviceInfoSize || got[1] != MaxExposedProtocolLevel {
		t.Fatalf("unexpected downstream device info len=%d level=%d", len(got), got[1])
	}
	if info[1] != 20 {
		t.Fatal("input must not be mutated")
	}
}

func TestDeduplicatorIgnoresPathAndSNR(t *testing.T) {
	d := NewReceivedMessageDeduplicator()
	a := []byte{RespChannelMessageV3, 0x10, 0, 0, 1, 0x01, 0, 1, 0, 0, 0, 'x'}
	b := []byte{RespChannelMessageV3, 0x20, 0, 0, 1, 0x02, 0, 1, 0, 0, 0, 'x'}
	if d.Duplicate(a) || !d.Duplicate(b) {
		t.Fatal("retry copy with different path/SNR should be a duplicate")
	}
}

func TestDescribeNeverPanicsOnTruncatedFrames(t *testing.T) {
	for op := 0; op < 256; op++ {
		for n := 1; n < 40; n++ {
			p := make([]byte, n)
			p[0] = byte(op)
			p[n-1] = 0xff
			_ = DescribeCommand(p, true)
			_ = DescribeResponse(p, true)
		}
	}
}
