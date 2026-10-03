package mux

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Hex renders wire bytes in a stable form for correlating mux logs with
// companion logs and packet captures.
func Hex(b []byte) string { return hex.EncodeToString(b) }

func hexByte(p []byte, i int) string {
	if i >= len(p) {
		return "none"
	}
	return fmt.Sprintf("0x%02x", p[i])
}

// sub returns p[off:off+n], clamped to the available bytes. Descriptions
// also run for rejected frames, so they must never panic on truncation.
func sub(p []byte, off, n int) []byte {
	if off >= len(p) || n <= 0 {
		return nil
	}
	return p[off:min(off+n, len(p))]
}

func tail(p []byte, off int) []byte {
	if off >= len(p) {
		return nil
	}
	return p[off:]
}

func text(b []byte) string { return strconv.Quote(string(b)) }

func fixedText(b []byte) string {
	// Native fixed text fields are NUL-padded C strings.
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		b = b[:i]
	}
	return text(b)
}

func readI32(p []byte, off int) int32 { return int32(readU32(p, off)) }

func descriptorName(p []byte) string {
	if d := Descriptor(p); d != nil {
		return d.Name
	}
	return "unknown"
}

// DescribeCommand summarizes stable semantic fields and, when requested, the
// complete payload as hex (except sensitive CLI bodies).
func DescribeCommand(p []byte, includePayload bool) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "command=%s opcode=%s command_bytes=%d", descriptorName(p), hexByte(p, 0), len(p))
	if peer := commandPeer(p); peer != nil {
		sb.WriteString(" peer=" + Hex(peer))
	}
	sb.WriteString(commandDetails(p))
	if includePayload {
		if len(p) > 0 && p[0] == CmdRunCliCommand {
			sb.WriteString(" payload=[redacted]")
		} else {
			sb.WriteString(" payload=" + Hex(p))
		}
	}
	return sb.String()
}

// DescribeResponse summarizes ordinary responses and pushes.
func DescribeResponse(p []byte, includePayload bool) string {
	if len(p) == 0 {
		return "response=empty response_bytes=0"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "response=%s code=%s response_bytes=%d", ResponseName(p[0]), hexByte(p, 0), len(p))
	switch {
	case p[0] == RespSent && len(p) >= 10:
		fmt.Fprintf(&sb, " token=%d radio_timeout_ms=%d", readU32(p, 2), readU32(p, 6))
	case (p[0] == PushLoginSuccess || p[0] == PushLoginFailure || p[0] == PushStatusResponse ||
		p[0] == PushTelemetryResponse || p[0] == PushPathDiscoveryResponse) && len(p) >= 8:
		sb.WriteString(" peer=" + Hex(p[2:8]))
	case p[0] == PushSendConfirmed && len(p) >= 9:
		fmt.Fprintf(&sb, " token=%d round_trip_ms=%d", readU32(p, 1), readU32(p, 5))
	}
	sb.WriteString(responseDetails(p))
	if includePayload {
		if p[0] == RespCliReply {
			sb.WriteString(" payload=[redacted]")
		} else {
			sb.WriteString(" payload=" + Hex(p))
		}
	}
	return sb.String()
}

func commandDetails(p []byte) string {
	if len(p) == 0 {
		return ""
	}
	n := len(p)
	switch p[0] {
	case CmdSendTxtMsg:
		if n < 13 {
			return ""
		}
		return fmt.Sprintf(" type=%d attempt=%d timestamp=%d destination=%s text=%s",
			p[1], p[2], readU32(p, 3), Hex(p[7:13]), text(p[13:]))
	case CmdSendChannelTxtMsg:
		if n < 7 {
			return ""
		}
		return fmt.Sprintf(" type=%d channel=%d timestamp=%d text=%s", p[1], p[2], readU32(p, 3), text(p[7:]))
	case CmdSetAdvertName:
		if n > 1 {
			return " name=" + text(p[1:])
		}
	case CmdAddUpdateContact:
		if n < 132 {
			return ""
		}
		s := fmt.Sprintf(" contact_key=%s contact_type=%d flags=%d %s name=%s",
			Hex(p[1:33]), p[33], p[34], describeNormalPath(p[35], p[36:100]), fixedText(p[100:132]))
		if n >= 144 {
			s += fmt.Sprintf(" latitude_microdegrees=%d longitude_microdegrees=%d", readI32(p, 136), readI32(p, 140))
		}
		if n >= 148 {
			s += fmt.Sprintf(" last_modified=%d", readU32(p, 144))
		}
		return s
	case CmdSetAdvertLatLon:
		if n < 9 {
			return ""
		}
		s := fmt.Sprintf(" latitude_microdegrees=%d longitude_microdegrees=%d", readI32(p, 1), readI32(p, 5))
		if n >= 13 {
			s += fmt.Sprintf(" altitude=%d", readI32(p, 9))
		}
		return s
	case CmdSendRawData:
		if n < 2 {
			return ""
		}
		pathBytes := int(p[1])
		s := fmt.Sprintf(" path_bytes=%d path=%s", pathBytes, Hex(sub(p, 2, pathBytes)))
		if off := 2 + pathBytes; off <= n {
			s += " data=" + Hex(p[off:])
		}
		return s
	case CmdSendTracePath:
		if n < 10 {
			return ""
		}
		width := 1 << (p[9] & PathWidthShiftMask)
		hashes := p[10:]
		return fmt.Sprintf(" tag=%d auth=%d hash_width=%d path_bytes=%d hashes=%s",
			readU32(p, 1), readU32(p, 5), width, len(hashes), Hex(hashes))
	case CmdSetChannel:
		if n < 34 {
			return ""
		}
		return fmt.Sprintf(" channel=%d name=%s", p[1], fixedText(p[2:34]))
	case CmdSendChannelData:
		if n < 3 {
			return ""
		}
		return fmt.Sprintf(" channel=%d %s", p[1], describeNormalPath(p[2], tail(p, 3)))
	case CmdSendRawPacket:
		if n > 1 {
			return " packet=" + Hex(p[1:])
		}
	}
	return ""
}

func responseDetails(p []byte) string {
	n := len(p)
	switch p[0] {
	case RespContact, PushNewAdvert:
		if n < 132 {
			return ""
		}
		s := fmt.Sprintf(" contact_key=%s contact_type=%d flags=%d %s name=%s",
			Hex(p[1:33]), p[33], p[34], describeNormalPath(p[35], p[36:100]), fixedText(p[100:132]))
		if n >= 144 {
			s += fmt.Sprintf(" latitude_microdegrees=%d longitude_microdegrees=%d", readI32(p, 136), readI32(p, 140))
		}
		if n >= 148 {
			s += fmt.Sprintf(" last_modified=%d", readU32(p, 144))
		}
		return s
	case RespSelfInfo:
		if n < 58 {
			return ""
		}
		s := fmt.Sprintf(" node_type=%d tx_power=%d max_tx_power=%d public_key=%s"+
			" latitude_microdegrees=%d longitude_microdegrees=%d frequency_hz=%d bandwidth_hz=%d"+
			" spreading_factor=%d coding_rate=%d",
			p[1], p[2], p[3], Hex(p[4:36]), readI32(p, 36), readI32(p, 40),
			readU32(p, 48), readU32(p, 52), p[56], p[57])
		if n > 58 {
			s += " name=" + text(p[58:])
		}
		return s
	case RespDeviceInfo:
		if n < 82 {
			return ""
		}
		return fmt.Sprintf(" protocol=%d max_contacts_half=%d max_channels=%d pin=%d build=%s model=%s firmware=%s repeater=%d path_hash_mode=%d",
			p[1], p[2], p[3], readU32(p, 4), fixedText(p[8:20]), fixedText(p[20:60]), fixedText(p[60:80]), p[80], p[81])
	case RespChannelInfo:
		if n < 50 {
			return ""
		}
		return fmt.Sprintf(" channel=%d name=%s", p[1], fixedText(p[2:34]))
	case RespContactMessage:
		return describeContactMessage(p, 1, 7, 8, 9, 13)
	case RespContactMessageV3:
		s := ""
		if n >= 4 {
			s = fmt.Sprintf(" snr_quarters=%d", int8(p[1]))
		}
		return s + describeContactMessage(p, 4, 10, 11, 12, 16)
	case RespChannelMessage:
		return describeChannelMessage(p, 1, 2, 3, 4, 8)
	case RespChannelMessageV3:
		s := ""
		if n >= 4 {
			s = fmt.Sprintf(" snr_quarters=%d", int8(p[1]))
		}
		return s + describeChannelMessage(p, 4, 5, 6, 7, 11)
	case RespAdvertPath:
		if n < 6 {
			return ""
		}
		return " " + describeNormalPath(p[5], p[6:])
	case PushTraceData:
		return describeTraceResult(p)
	case PushRawData, PushControlData:
		if n < 4 {
			return ""
		}
		return fmt.Sprintf(" snr_quarters=%d rssi=%d path_encoding=%s data=%s", int8(p[1]), int8(p[2]), hexByte(p, 3), Hex(p[4:]))
	case PushLogRxData:
		if n < 3 {
			return ""
		}
		return fmt.Sprintf(" snr_quarters=%d rssi=%d packet=%s", int8(p[1]), int8(p[2]), Hex(p[3:]))
	case PushPathDiscoveryResponse:
		return describeDiscoveryPaths(p)
	}
	return ""
}

func describeContactMessage(p []byte, peer, path, typ, ts, body int) string {
	if len(p) < body {
		return ""
	}
	return fmt.Sprintf(" sender=%s path_encoding=%s type=%d timestamp=%d text=%s",
		Hex(p[peer:peer+6]), hexByte(p, path), p[typ], readU32(p, ts), text(p[body:]))
}

func describeChannelMessage(p []byte, channel, path, typ, ts, body int) string {
	if len(p) < body {
		return ""
	}
	return fmt.Sprintf(" channel=%d path_encoding=%s type=%d timestamp=%d text=%s",
		p[channel], hexByte(p, path), p[typ], readU32(p, ts), text(p[body:]))
}

func describeNormalPath(encoded byte, available []byte) string {
	if encoded == NoPathEncoding {
		return "path_encoding=0xff path=none"
	}
	count := int(encoded & PathCountMask)
	width := int(encoded>>6) + 1
	return fmt.Sprintf("path_encoding=0x%02x path_hashes=%d hash_width=%d path=%s",
		encoded, count, width, Hex(sub(available, 0, count*width)))
}

func describeTraceResult(p []byte) string {
	if len(p) < 12 {
		return ""
	}
	pathBytes := int(p[2])
	width := 1 << (p[3] & PathWidthShiftMask)
	hops := pathBytes / width
	snrOffset := 12 + pathBytes
	return fmt.Sprintf(" tag=%d auth=%d hash_width=%d path_bytes=%d hashes=%s snr_quarters=%s",
		readU32(p, 4), readU32(p, 8), width, pathBytes, Hex(sub(p, 12, pathBytes)), Hex(sub(p, snrOffset, hops+1)))
}

func describeDiscoveryPaths(p []byte) string {
	if len(p) < 10 {
		return ""
	}
	cursor := 8
	outEnc := p[cursor]
	outSize, _ := normalEncodedPathBytes(outEnc)
	s := " outbound_" + describeNormalPath(outEnc, sub(p, cursor+1, outSize))
	cursor += 1 + outSize
	if cursor >= len(p) {
		return s
	}
	inEnc := p[cursor]
	inSize, _ := normalEncodedPathBytes(inEnc)
	return s + " inbound_" + describeNormalPath(inEnc, sub(p, cursor+1, inSize))
}

func commandPeer(p []byte) []byte {
	// Public destination keys exclude the TCP envelope: ordinary peer commands
	// put their key at byte 1, remote telemetry at byte 4, path discovery at byte 2.
	if len(p) == 0 {
		return nil
	}
	off := -1
	switch p[0] {
	case CmdResetPath, CmdRemoveContact, CmdShareContact, CmdSendLogin, CmdSendStatusReq,
		CmdHasConnection, CmdLogout, CmdGetContactByKey, CmdSendBinaryReq, CmdSendAnonReq:
		off = 1
	case CmdSendTelemetryReq:
		if len(p) >= 36 {
			off = 4
		}
	case CmdSendPathDiscoveryReq:
		off = 2
	}
	if off < 0 || len(p) < off+6 {
		return nil
	}
	return sub(p, off, 32)
}

// WireLog formats one decoded wire frame with its physical direction and
// complete payload, redacting local CLI bodies.
type wireDir int

const (
	dirRx wireDir = iota
	dirTx
)

func (d wireDir) String() string {
	if d == dirTx {
		return "tx"
	}
	return "rx"
}

func wireUpstream(epoch int64, d wireDir, p []byte) string {
	command := d == dirTx
	return fmt.Sprintf("UPSTREAM(%d): %s %s payload=%s %s", epoch, d, wireName(command, p), wirePayload(command, p), wireDescribe(command, p))
}

func wireMultiClient(session int64, d wireDir, p []byte) string {
	command := d == dirRx
	return fmt.Sprintf("MULTI_CLIENT(%d): %s %s payload=%s %s", session, d, wireName(command, p), wirePayload(command, p), wireDescribe(command, p))
}

func wireDedicatedClient(port int, d wireDir, p []byte) string {
	command := d == dirRx
	return fmt.Sprintf("DEDICATED_CLIENT(%d): %s %s payload=%s %s", port, d, wireName(command, p), wirePayload(command, p), wireDescribe(command, p))
}

func wireDescribe(command bool, p []byte) string {
	if command {
		return DescribeCommand(p, false)
	}
	return DescribeResponse(p, false)
}

func wirePayload(command bool, p []byte) string {
	sensitive := RespCliReply
	if command {
		sensitive = CmdRunCliCommand
	}
	if len(p) > 0 && p[0] == sensitive {
		return "[redacted]"
	}
	return Hex(p)
}

func wireName(command bool, p []byte) string {
	if len(p) == 0 {
		return "EMPTY"
	}
	if command {
		return strings.ToUpper(descriptorName(p))
	}
	return strings.ToUpper(ResponseName(p[0]))
}
