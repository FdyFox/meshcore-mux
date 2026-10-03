package mux

import (
	"bytes"
	"fmt"
)

// Native companion protocol: the known v14 dialect caps decoded payloads at
// 176 bytes and ordinary encoded paths at 64 bytes.
const (
	MaxPayload = 176
	// App target selects response formats; firmware level independently describes features.
	UpstreamAppTarget                 byte = 14
	MaxExposedProtocolLevel           byte = 14
	MinSupportedUpstreamProtocolLevel byte = 13
	DeviceInfoSize                         = 82
	MaxPathSize                            = 64
	NoPathEncoding                    byte = 0xff
	PathCountMask                     byte = 0x3f
	PathWidthShiftMask                byte = 0x03

	ErrUnsupportedCmd byte = 1
	ErrTableFull      byte = 3
	ErrBadState       byte = 4
	ErrIllegalArg     byte = 6

	StatsTypeCore   byte = 0
	StatsTypeRadio  byte = 1
	StatsTypePacket byte = 2
)

const (
	CmdAppStart             byte = 1
	CmdSendTxtMsg           byte = 2
	CmdSendChannelTxtMsg    byte = 3
	CmdGetContacts          byte = 4
	CmdGetDeviceTime        byte = 5
	CmdSetDeviceTime        byte = 6
	CmdSendSelfAdvert       byte = 7
	CmdSetAdvertName        byte = 8
	CmdAddUpdateContact     byte = 9
	CmdSyncNextMessage      byte = 10
	CmdSetRadioParams       byte = 11
	CmdSetRadioTxPower      byte = 12
	CmdResetPath            byte = 13
	CmdSetAdvertLatLon      byte = 14
	CmdRemoveContact        byte = 15
	CmdShareContact         byte = 16
	CmdExportContact        byte = 17
	CmdImportContact        byte = 18
	CmdReboot               byte = 19
	CmdGetBattAndStorage    byte = 20
	CmdSetTuningParams      byte = 21
	CmdDeviceQuery          byte = 22
	CmdExportPrivateKey     byte = 23
	CmdImportPrivateKey     byte = 24
	CmdSendRawData          byte = 25
	CmdSendLogin            byte = 26
	CmdSendStatusReq        byte = 27
	CmdHasConnection        byte = 28
	CmdLogout               byte = 29
	CmdGetContactByKey      byte = 30
	CmdGetChannel           byte = 31
	CmdSetChannel           byte = 32
	CmdSignStart            byte = 33
	CmdSignData             byte = 34
	CmdSignFinish           byte = 35
	CmdSendTracePath        byte = 36
	CmdSetDevicePin         byte = 37
	CmdSetOtherParams       byte = 38
	CmdSendTelemetryReq     byte = 39
	CmdGetCustomVars        byte = 40
	CmdSetCustomVar         byte = 41
	CmdGetAdvertPath        byte = 42
	CmdGetTuningParams      byte = 43
	CmdSendBinaryReq        byte = 50
	CmdFactoryReset         byte = 51
	CmdSendPathDiscoveryReq byte = 52
	CmdSetFloodScopeKey     byte = 54
	CmdSendControlData      byte = 55
	CmdGetStats             byte = 56
	CmdSendAnonReq          byte = 57
	CmdSetAutoaddConfig     byte = 58
	CmdGetAutoaddConfig     byte = 59
	CmdGetAllowedRepeatFreq byte = 60
	CmdSetPathHashMode      byte = 61
	CmdSendChannelData      byte = 62
	CmdSetDefaultFloodScope byte = 63
	CmdGetDefaultFloodScope byte = 64
	CmdSendRawPacket        byte = 65
	CmdRunCliCommand        byte = 66
)

const (
	RespOk                byte = 0x00
	RespErr               byte = 0x01
	RespContactsStart     byte = 0x02
	RespContact           byte = 0x03
	RespEndOfContacts     byte = 0x04
	RespSelfInfo          byte = 0x05
	RespSent              byte = 0x06
	RespContactMessage    byte = 0x07
	RespChannelMessage    byte = 0x08
	RespCurrentTime       byte = 0x09
	RespNoMoreMessages    byte = 0x0a
	RespExportContact     byte = 0x0b
	RespBatteryAndStorage byte = 0x0c
	RespDeviceInfo        byte = 0x0d
	RespPrivateKey        byte = 0x0e
	RespDisabled          byte = 0x0f
	RespContactMessageV3  byte = 0x10
	RespChannelMessageV3  byte = 0x11
	RespChannelInfo       byte = 0x12
	RespSignStart         byte = 0x13
	RespSignature         byte = 0x14
	RespCustomVars        byte = 0x15
	RespAdvertPath        byte = 0x16
	RespTuningParams      byte = 0x17
	RespStats             byte = 0x18
	RespAutoaddConfig     byte = 0x19
	RespAllowedRepeatFreq byte = 0x1a
	RespChannelData       byte = 0x1b
	RespDefaultFloodScope byte = 0x1c
	RespCliReply          byte = 0x1d

	PushAdvert                byte = 0x80
	PushPathUpdated           byte = 0x81
	PushSendConfirmed         byte = 0x82
	PushMsgWaiting            byte = 0x83
	PushRawData               byte = 0x84
	PushLoginSuccess          byte = 0x85
	PushLoginFailure          byte = 0x86
	PushStatusResponse        byte = 0x87
	PushLogRxData             byte = 0x88
	PushTraceData             byte = 0x89
	PushNewAdvert             byte = 0x8a
	PushTelemetryResponse     byte = 0x8b
	PushBinaryResponse        byte = 0x8c
	PushPathDiscoveryResponse byte = 0x8d
	PushControlData           byte = 0x8e
	PushContactDeleted        byte = 0x8f
	PushContactsFull          byte = 0x90
)

var responseNames = map[byte]string{
	RespOk: "ok", RespErr: "err", RespContactsStart: "contacts_start",
	RespContact: "contact", RespEndOfContacts: "end_of_contacts",
	RespSelfInfo: "self_info", RespSent: "sent",
	RespContactMessage: "contact_message", RespChannelMessage: "channel_message",
	RespCurrentTime: "current_time", RespNoMoreMessages: "no_more_messages",
	RespExportContact: "export_contact", RespBatteryAndStorage: "battery_and_storage",
	RespDeviceInfo: "device_info", RespPrivateKey: "private_key",
	RespDisabled: "disabled", RespContactMessageV3: "contact_message_v3",
	RespChannelMessageV3: "channel_message_v3", RespChannelInfo: "channel_info",
	RespSignStart: "sign_start", RespSignature: "signature",
	RespCustomVars: "custom_vars", RespAdvertPath: "advert_path",
	RespTuningParams: "tuning_params", RespStats: "stats",
	RespAutoaddConfig: "autoadd_config", RespAllowedRepeatFreq: "allowed_repeat_freq",
	RespChannelData: "channel_data", RespDefaultFloodScope: "default_flood_scope",
	RespCliReply: "cli_reply",
	PushAdvert:   "advert", PushPathUpdated: "path_updated",
	PushSendConfirmed: "send_confirmed", PushMsgWaiting: "msg_waiting",
	PushRawData: "raw_data", PushLoginSuccess: "login_success",
	PushLoginFailure: "login_failure", PushStatusResponse: "status_response",
	PushLogRxData: "log_rx_data", PushTraceData: "trace_data",
	PushNewAdvert: "new_advert", PushTelemetryResponse: "telemetry_response",
	PushBinaryResponse: "binary_response", PushPathDiscoveryResponse: "path_discovery_response",
	PushControlData: "control_data", PushContactDeleted: "contact_deleted",
	PushContactsFull: "contacts_full",
}

// ResponseName returns a safe diagnostic label, never payload contents.
func ResponseName(code byte) string {
	if n, ok := responseNames[code]; ok {
		return n
	}
	return fmt.Sprintf("push_%d", code)
}

// Grammar is the response stream shape used to decide when upstream ownership can be released.
type Grammar int

const (
	GrammarSingle Grammar = iota
	GrammarContacts
	GrammarInbox
	GrammarSelfTelemetry
	GrammarDisconnecting
)

// CommandFlags are broker policy bits: scope wrapping, shared resources, and reply correlation.
type CommandFlags uint16

const (
	FlagScopeSend     CommandFlags = 1
	FlagRemoteLease   CommandFlags = 4
	FlagSigning       CommandFlags = 8
	FlagMaintenance   CommandFlags = 64
	FlagVerifyIndex   CommandFlags = 256
	FlagVerifySubtype CommandFlags = 512
)

// CommandDescriptor holds wire identity, completion grammar, allowed successes, and broker policy.
type CommandDescriptor struct {
	Opcode       byte
	Name         string
	Grammar      Grammar
	SuccessCodes []byte
	Flags        CommandFlags
}

// Has reports whether the descriptor carries a policy flag.
func (d *CommandDescriptor) Has(f CommandFlags) bool { return d.Flags&f != 0 }

func (d *CommandDescriptor) allows(code byte) bool { return bytes.IndexByte(d.SuccessCodes, code) >= 0 }

func desc(op byte, name string, g Grammar, success []byte, flags CommandFlags) *CommandDescriptor {
	return &CommandDescriptor{op, name, g, success, flags}
}

var descriptors = func() map[byte]*CommandDescriptor {
	s, c, in := GrammarSingle, GrammarContacts, GrammarInbox
	list := []*CommandDescriptor{
		desc(CmdAppStart, "app_start", s, []byte{RespSelfInfo}, 0),
		desc(CmdSendTxtMsg, "send_txt_msg", s, []byte{RespSent}, FlagScopeSend),
		desc(CmdSendChannelTxtMsg, "send_channel_txt_msg", s, []byte{RespOk}, FlagScopeSend),
		desc(CmdGetContacts, "get_contacts", c, []byte{RespContactsStart, RespContact, RespEndOfContacts}, 0),
		desc(CmdGetDeviceTime, "get_device_time", s, []byte{RespCurrentTime}, 0),
		desc(CmdSetDeviceTime, "set_device_time", s, []byte{RespOk}, 0),
		desc(CmdSendSelfAdvert, "send_self_advert", s, []byte{RespOk}, 0),
		desc(CmdSetAdvertName, "set_advert_name", s, []byte{RespOk}, 0),
		desc(CmdAddUpdateContact, "add_update_contact", s, []byte{RespOk}, 0),
		desc(CmdSyncNextMessage, "sync_next_message", in, []byte{RespContactMessage, RespChannelMessage, RespNoMoreMessages, RespContactMessageV3, RespChannelMessageV3, RespChannelData}, 0),
		desc(CmdSetRadioParams, "set_radio_params", s, []byte{RespOk}, 0),
		desc(CmdSetRadioTxPower, "set_radio_tx_power", s, []byte{RespOk}, 0),
		desc(CmdResetPath, "reset_path", s, []byte{RespOk}, 0),
		desc(CmdSetAdvertLatLon, "set_advert_latlon", s, []byte{RespOk}, 0),
		desc(CmdRemoveContact, "remove_contact", s, []byte{RespOk}, 0),
		desc(CmdShareContact, "share_contact", s, []byte{RespOk}, 0),
		desc(CmdExportContact, "export_contact", s, []byte{RespExportContact}, 0),
		desc(CmdImportContact, "import_contact", s, []byte{RespOk}, 0),
		desc(CmdReboot, "reboot", GrammarDisconnecting, nil, FlagMaintenance),
		desc(CmdGetBattAndStorage, "get_batt_and_storage", s, []byte{RespBatteryAndStorage}, 0),
		desc(CmdSetTuningParams, "set_tuning_params", s, []byte{RespOk}, 0),
		desc(CmdDeviceQuery, "device_query", s, []byte{RespDeviceInfo}, 0),
		desc(CmdExportPrivateKey, "export_private_key", s, []byte{RespPrivateKey, RespDisabled}, 0),
		desc(CmdImportPrivateKey, "import_private_key", s, []byte{RespOk, RespDisabled}, FlagMaintenance),
		desc(CmdSendRawData, "send_raw_data", s, []byte{RespOk}, 0),
		desc(CmdSendLogin, "send_login", s, []byte{RespSent}, FlagRemoteLease|FlagScopeSend),
		desc(CmdSendStatusReq, "send_status_req", s, []byte{RespSent}, FlagRemoteLease|FlagScopeSend),
		desc(CmdHasConnection, "has_connection", s, []byte{RespOk}, 0),
		desc(CmdLogout, "logout", s, []byte{RespOk}, 0),
		desc(CmdGetContactByKey, "get_contact_by_key", s, []byte{RespContact}, 0),
		desc(CmdGetChannel, "get_channel", s, []byte{RespChannelInfo}, FlagVerifyIndex),
		desc(CmdSetChannel, "set_channel", s, []byte{RespOk}, 0),
		desc(CmdSignStart, "sign_start", s, []byte{RespSignStart}, FlagSigning),
		desc(CmdSignData, "sign_data", s, []byte{RespOk}, FlagSigning),
		desc(CmdSignFinish, "sign_finish", s, []byte{RespSignature}, FlagSigning),
		desc(CmdSendTracePath, "send_trace_path", s, []byte{RespSent}, FlagRemoteLease),
		desc(CmdSetDevicePin, "set_device_pin", s, []byte{RespOk}, 0),
		desc(CmdSetOtherParams, "set_other_params", s, []byte{RespOk}, 0),
		desc(CmdSendTelemetryReq, "send_telemetry_req", GrammarSelfTelemetry, []byte{PushTelemetryResponse}, 0),
		desc(CmdGetCustomVars, "get_custom_vars", s, []byte{RespCustomVars}, 0),
		desc(CmdSetCustomVar, "set_custom_var", s, []byte{RespOk}, 0),
		desc(CmdGetAdvertPath, "get_advert_path", s, []byte{RespAdvertPath}, 0),
		desc(CmdGetTuningParams, "get_tuning_params", s, []byte{RespTuningParams}, 0),
		desc(CmdSendBinaryReq, "send_binary_req", s, []byte{RespSent}, FlagRemoteLease|FlagScopeSend),
		desc(CmdFactoryReset, "factory_reset", GrammarDisconnecting, []byte{RespOk}, FlagMaintenance),
		desc(CmdSendPathDiscoveryReq, "send_path_discovery_req", s, []byte{RespSent}, FlagRemoteLease|FlagScopeSend),
		desc(CmdSetFloodScopeKey, "set_flood_scope_key", s, []byte{RespOk}, 0),
		desc(CmdSendControlData, "send_control_data", s, []byte{RespOk}, 0),
		desc(CmdGetStats, "get_stats", s, []byte{RespStats}, FlagVerifySubtype),
		desc(CmdSendAnonReq, "send_anon_req", s, []byte{RespSent}, FlagRemoteLease|FlagScopeSend),
		desc(CmdSetAutoaddConfig, "set_autoadd_config", s, []byte{RespOk}, 0),
		desc(CmdGetAutoaddConfig, "get_autoadd_config", s, []byte{RespAutoaddConfig}, 0),
		desc(CmdGetAllowedRepeatFreq, "get_allowed_repeat_freq", s, []byte{RespAllowedRepeatFreq}, 0),
		desc(CmdSetPathHashMode, "set_path_hash_mode", s, []byte{RespOk}, 0),
		desc(CmdSendChannelData, "send_channel_data", s, []byte{RespOk}, FlagScopeSend),
		desc(CmdSetDefaultFloodScope, "set_default_flood_scope", s, []byte{RespOk}, 0),
		desc(CmdGetDefaultFloodScope, "get_default_flood_scope", s, []byte{RespDefaultFloodScope}, 0),
		desc(CmdSendRawPacket, "send_raw_packet", s, []byte{RespOk}, 0),
		desc(CmdRunCliCommand, "run_cli_command", s, []byte{RespCliReply}, 0),
	}
	m := make(map[byte]*CommandDescriptor, len(list))
	for _, d := range list {
		m[d.Opcode] = d
	}
	return m
}()

// Telemetry opcode 39 is two distinct native commands selected by length;
// only the remote form completes first with SENT.
var remoteTelemetryDescriptor = desc(CmdSendTelemetryReq, "send_telemetry_req", GrammarSingle,
	[]byte{RespSent}, FlagRemoteLease|FlagScopeSend)

// Descriptor looks up routing policy for a command payload.
func Descriptor(p []byte) *CommandDescriptor {
	if len(p) == 0 {
		return nil
	}
	d := descriptors[p[0]]
	if d == nil {
		return nil
	}
	if p[0] == CmdSendTelemetryReq && len(p) != 4 {
		return remoteTelemetryDescriptor
	}
	return d
}

// ValidateCommand separates unknown opcodes (UNSUPPORTED_CMD) from invalid
// wire layouts (ILLEGAL_ARG). A zero reason means the command is valid.
func ValidateCommand(p []byte) (*CommandDescriptor, byte) {
	d := Descriptor(p)
	if d == nil {
		return nil, ErrUnsupportedCmd
	}
	if !commandShapeValid(p) {
		return d, ErrIllegalArg
	}
	return d, 0
}

func commandShapeValid(p []byte) bool {
	// Minimum lengths intentionally allow native trailing extensions.
	n := len(p)
	if n == 0 || n > MaxPayload {
		return false
	}
	switch p[0] {
	case CmdAppStart: // Opcode + seven reserved bytes, then optional app name.
		return n >= 8
	case CmdSendTxtMsg: // Type, attempt, timestamp, six-byte peer prefix, and at least one body byte.
		return n >= 14
	case CmdSendChannelTxtMsg: // Type, channel, and four-byte timestamp before optional text.
		return n >= 7
	case CmdGetContacts: // Optional four-byte modified-since timestamp.
		return n == 1 || n >= 5
	case CmdGetDeviceTime, CmdSyncNextMessage, CmdGetBattAndStorage, CmdExportPrivateKey, CmdSignStart,
		CmdSignFinish, CmdGetCustomVars, CmdGetTuningParams, CmdGetAutoaddConfig,
		CmdGetAllowedRepeatFreq, CmdGetDefaultFloodScope, CmdSendSelfAdvert:
		return n >= 1
	case CmdSetDeviceTime:
		return n >= 5
	case CmdRunCliCommand, CmdSetAdvertName, CmdSetRadioTxPower, CmdGetChannel, CmdSignData,
		CmdSetOtherParams, CmdSetAutoaddConfig, CmdDeviceQuery:
		return n >= 2
	case CmdAddUpdateContact:
		// Native records end after the advert timestamp (136), GPS (144), or a
		// last-modified timestamp (148+). MeshCore One appends three reserved
		// bytes after GPS (147).
		if !(n == 136 || n == 144 || n == 147 || n >= 148) {
			return false
		}
		_, ok := normalEncodedPathBytes(p[35])
		return p[35] == NoPathEncoding || ok
	case CmdSetRadioParams:
		return n >= 11
	case CmdResetPath, CmdRemoveContact, CmdShareContact, CmdSendStatusReq, CmdHasConnection,
		CmdLogout, CmdGetContactByKey, CmdSendLogin: // 32-byte public key.
		return n >= 33
	case CmdSetAdvertLatLon:
		return n == 9 || n >= 13
	case CmdExportContact:
		return n == 1 || n >= 33
	case CmdImportContact:
		return n >= 99
	case CmdReboot:
		return n == 7 && string(p[1:7]) == "reboot"
	case CmdSetTuningParams:
		return n >= 9
	case CmdImportPrivateKey:
		return n >= 65
	case CmdSendRawData:
		// Only path lengths 0..63 have one unambiguous interpretation.
		if n < 6 || p[1] >= 0x40 {
			return false
		}
		return 2+int(p[1])+4 <= n
	case CmdSetChannel: // Index, 32-byte name, 16-byte key, up to 15 ignored trailing bytes.
		return n >= 50 && n < 66
	case CmdSendTracePath: // Tag, auth, flags, then whole path hashes.
		if !(n > 10 && n-10 < 171) {
			return false
		}
		shift := p[9] & PathWidthShiftMask
		pathBytes := n - 10
		return pathBytes%(1<<shift) == 0 && (pathBytes>>shift) <= MaxPathSize
	case CmdSetDevicePin:
		return n >= 5
	case CmdSendTelemetryReq:
		return n == 4 || n >= 36
	case CmdSetCustomVar:
		return n >= 4 && bytes.IndexByte(p[1:], ':') >= 0
	case CmdGetAdvertPath, CmdSendBinaryReq, CmdSendAnonReq:
		return n >= 34
	case CmdFactoryReset:
		return n == 6 && string(p[1:6]) == "reset"
	case CmdSendPathDiscoveryReq:
		return n >= 34 && p[1] == 0
	case CmdSetFloodScopeKey: // Mode 0 uses default or explicit 16-byte key; mode 1 is unscoped.
		return (n == 2 && (p[1] == 0 || p[1] == 1)) || (n == 18 && p[1] == 0)
	case CmdSendControlData:
		return n >= 2 && p[1]&0x80 != 0
	case CmdGetStats:
		return n >= 2 && p[1] <= 2
	case CmdSetPathHashMode:
		return n >= 3 && p[1] == 0 && p[2] < 3
	case CmdSendChannelData:
		return channelDataCommandValid(p)
	case CmdSetDefaultFloodScope:
		return defaultScopeCommandValid(p)
	case CmdSendRawPacket:
		return n >= 4
	}
	return false
}

func channelDataCommandValid(p []byte) bool {
	// Opcode, channel, encoded path, path bytes, then a two-byte data type.
	if len(p) < 5 {
		return false
	}
	pathBytes := 0
	if p[2] != NoPathEncoding {
		b, ok := normalEncodedPathBytes(p[2])
		if !ok {
			return false
		}
		pathBytes = b
	}
	return 3+pathBytes+2 <= len(p)
}

func defaultScopeCommandValid(p []byte) bool {
	// Clear with opcode alone, or a NUL-terminated 31-byte name and 16-byte key.
	if len(p) == 1 {
		return true
	}
	if len(p) != 48 {
		return false
	}
	nul := bytes.IndexByte(p[1:32], 0)
	return nul > 0 && nul < 31
}

// ProtocolError means an upstream payload violates the protocol; the broker must abandon the epoch.
type ProtocolError struct{ msg string }

func (e *ProtocolError) Error() string { return e.msg }

func protoErr(format string, args ...any) error { return &ProtocolError{fmt.Sprintf(format, args...)} }

// ValidateResponse validates ownership as well as shape. It returns true when
// the response completes the command (false = stream still in progress).
func ValidateResponse(d *CommandDescriptor, payload, command []byte, phase int) (bool, error) {
	if len(payload) == 0 {
		return false, protoErr("empty upstream payload")
	}
	if len(payload) > MaxPayload {
		return false, protoErr("oversized upstream payload")
	}
	code := payload[0]
	if code == RespErr {
		if len(payload) != 2 {
			return false, protoErr("malformed ERR response")
		}
		return true, nil
	}
	if !d.allows(code) {
		return false, protoErr("unexpected response 0x%x for %s", code, d.Name)
	}
	if err := ValidateResponseShape(payload); err != nil {
		return false, err
	}
	contacts := d.Grammar == GrammarContacts
	if contacts {
		expected := (phase == 0 && code == RespContactsStart) ||
			(phase == 1 && (code == RespContact || code == RespEndOfContacts))
		if !expected {
			return false, protoErr("out-of-order contacts response")
		}
	}
	if command != nil {
		if code == RespChannelInfo && d.Has(FlagVerifyIndex) && payload[1] != command[1] {
			return false, protoErr("channel response index mismatch")
		}
		if code == RespStats && d.Has(FlagVerifySubtype) && payload[1] != command[1] {
			return false, protoErr("stats response subtype mismatch")
		}
	}
	return !(contacts && code != RespEndOfContacts), nil
}

// ValidateResponseShape validates ordinary replies and asynchronous pushes
// without assuming an owner. Unknown pushes are opaque; unknown ordinary responses are fatal.
func ValidateResponseShape(p []byte) error {
	n := len(p)
	if n == 0 {
		return protoErr("empty upstream payload")
	}
	if n > MaxPayload {
		return protoErr("oversized upstream payload")
	}
	var ok bool
	switch p[0] {
	case RespOk, RespNoMoreMessages, RespDisabled, PushMsgWaiting, PushContactsFull:
		ok = n == 1
	case RespErr:
		ok = n == 2
	case RespContactsStart, RespEndOfContacts, RespCurrentTime:
		ok = n == 5
	case RespContact, PushNewAdvert:
		ok = n == 148
	case RespSelfInfo:
		ok = n >= 58
	case RespSent:
		ok = n == 10
	case RespContactMessage:
		ok = contactTextResponseValid(p, 13, 8)
	case RespChannelMessage:
		ok = n >= 8
	case RespExportContact:
		ok = n >= 2
	case RespBatteryAndStorage:
		ok = n == 11
	case RespDeviceInfo:
		ok = n >= DeviceInfoSize && p[1] >= MinSupportedUpstreamProtocolLevel
	case RespCliReply, RespCustomVars:
		ok = n >= 1
	case RespPrivateKey, RespSignature:
		ok = n == 65
	case RespContactMessageV3:
		ok = contactTextResponseValid(p, 16, 11)
	case RespChannelMessageV3:
		ok = n >= 11
	case RespChannelInfo:
		ok = n == 50
	case RespSignStart:
		ok = n == 6
	case RespAdvertPath:
		ok = advertPathResponseValid(p)
	case RespTuningParams:
		ok = n == 9
	case RespStats:
		ok = statsResponseValid(p)
	case RespAutoaddConfig:
		ok = n == 3
	case RespAllowedRepeatFreq:
		ok = n >= 1 && (n-1)%8 == 0
	case RespChannelData:
		ok = n >= 9 && n == 9+int(p[8])
	case RespDefaultFloodScope:
		ok = n == 1 || n == 48
	case PushAdvert, PushPathUpdated, PushContactDeleted:
		ok = n == 33
	case PushSendConfirmed:
		ok = n == 9
	case PushRawData, PushControlData:
		ok = n >= 4
	case PushLogRxData:
		ok = n >= 3
	case PushLoginSuccess:
		ok = n == 8 || n >= 14
	case PushLoginFailure, PushTelemetryResponse:
		ok = n >= 8
	case PushStatusResponse:
		ok = n >= 9
	case PushPathDiscoveryResponse:
		ok = pathDiscoveryResponseValid(p)
	case PushTraceData:
		ok = traceResponseValid(p)
	case PushBinaryResponse:
		ok = n >= 6
	default:
		ok = p[0] >= PushAdvert
	}
	if !ok {
		return protoErr("malformed response 0x%x (%d bytes)", p[0], n)
	}
	return nil
}

func statsResponseValid(p []byte) bool {
	if len(p) < 2 {
		return false
	}
	switch p[1] {
	case StatsTypeCore:
		return len(p) == 11
	case StatsTypeRadio:
		return len(p) == 14
	case StatsTypePacket:
		return len(p) == 30
	}
	return false
}

func traceResponseValid(p []byte) bool {
	// 12-byte prefix with tag/auth, then path hashes, one SNR per hop, and a final SNR byte.
	if len(p) < 13 {
		return false
	}
	pathBytes := int(p[2])
	width := 1 << (p[3] & PathWidthShiftMask)
	if pathBytes%width != 0 {
		return false
	}
	hops := pathBytes / width
	return hops <= MaxPathSize && len(p) == 12+pathBytes+hops+1
}

// normalEncodedPathBytes decodes an ordinary path encoding: low six bits
// count hops, upper two bits encode hash width minus one (4 is reserved).
func normalEncodedPathBytes(encoded byte) (int, bool) {
	count := int(encoded & PathCountMask)
	width := int(encoded>>6) + 1
	if width == 4 {
		return 0, false
	}
	b := count * width
	return b, b <= MaxPathSize
}

func advertPathResponseValid(p []byte) bool {
	if len(p) < 6 {
		return false
	}
	size, ok := normalEncodedPathBytes(p[5])
	return ok && len(p) == 6+size
}

func pathDiscoveryResponseValid(p []byte) bool {
	if len(p) < 10 {
		return false
	}
	cursor := 8
	out, ok := normalEncodedPathBytes(p[cursor])
	if !ok {
		return false
	}
	cursor += 1 + out
	if cursor >= len(p) {
		return false
	}
	in, ok := normalEncodedPathBytes(p[cursor])
	if !ok {
		return false
	}
	cursor += 1 + in
	return cursor == len(p)
}

func contactTextResponseValid(p []byte, baseSize, typeOffset int) bool {
	// Signed text (type 2) adds a four-byte sender prefix beyond the normal header.
	if len(p) < baseSize {
		return false
	}
	return p[typeOffset] != 2 || len(p) >= baseSize+4
}

// DowngradeInbox converts V3 inbox messages for clients targeting pre-V3 dialects
// by removing the three-byte SNR/reserved prefix.
func DowngradeInbox(payload []byte, targetVersion byte) ([]byte, error) {
	if err := ValidateResponseShape(payload); err != nil {
		return nil, err
	}
	if targetVersion < 3 {
		switch payload[0] {
		case RespContactMessageV3:
			return append([]byte{RespContactMessage}, payload[4:]...), nil
		case RespChannelMessageV3:
			return append([]byte{RespChannelMessage}, payload[4:]...), nil
		}
	}
	return dup(payload), nil
}

// PlainDM reports a SEND_TXT_MSG type 0, which uses the companion's acknowledgement ring.
func PlainDM(p []byte) bool { return len(p) >= 2 && p[0] == CmdSendTxtMsg && p[1] == 0 }

// AppStartPayload builds APP_START: opcode, seven reserved bytes, then the app name.
func AppStartPayload(appName string) []byte {
	p := make([]byte, 8, 8+len(appName))
	p[0] = CmdAppStart
	return append(p, appName...)
}

// DeviceQueryPayload builds DEVICE_QUERY with the requested protocol target.
func DeviceQueryPayload(target byte) []byte { return []byte{CmdDeviceQuery, target} }

// NormalizeDeviceQuery keeps the upstream on the mux-owned app target.
func NormalizeDeviceQuery(p []byte) []byte {
	c := dup(p)
	c[1] = UpstreamAppTarget
	return c
}

// ValidateSelfInfo returns a copy of the 32-byte public key from SELF_INFO.
func ValidateSelfInfo(p []byte) ([]byte, error) {
	if len(p) < 58 || p[0] != RespSelfInfo {
		return nil, protoErr("malformed SELF_INFO")
	}
	return dup(p[4:36]), nil
}

// ValidateDeviceInfo checks the known 82-byte DEVICE_INFO prefix and returns the firmware protocol level.
func ValidateDeviceInfo(p []byte) (byte, error) {
	if len(p) < DeviceInfoSize || len(p) > MaxPayload || p[0] != RespDeviceInfo {
		return 0, protoErr("malformed DEVICE_INFO")
	}
	if p[1] < MinSupportedUpstreamProtocolLevel {
		return 0, protoErr("unsupported firmware protocol level %d; minimum %d", p[1], MinSupportedUpstreamProtocolLevel)
	}
	return p[1], nil
}

// DownstreamDeviceInfo advertises only implemented capabilities and the known record shape.
func DownstreamDeviceInfo(p []byte) ([]byte, error) {
	level, err := ValidateDeviceInfo(p)
	if err != nil {
		return nil, err
	}
	c := dup(p[:DeviceInfoSize])
	c[1] = min(level, MaxExposedProtocolLevel)
	return c, nil
}

// ReceivedTextMessageIdentity builds a version-independent logical identity
// for inbox text. Path and SNR describe one reception and are excluded.
func ReceivedTextMessageIdentity(p []byte) (string, bool) {
	if len(p) == 0 {
		return "", false
	}
	switch p[0] {
	case RespContactMessage:
		return contactIdentity(p, 1, 8, 9, 13)
	case RespContactMessageV3:
		return contactIdentity(p, 4, 11, 12, 16)
	case RespChannelMessage:
		return channelIdentity(p, 1, 3, 4, 8)
	case RespChannelMessageV3:
		return channelIdentity(p, 4, 6, 7, 11)
	}
	return "", false
}

func contactIdentity(p []byte, sender, typ, ts, body int) (string, bool) {
	if len(p) < body {
		return "", false
	}
	return fmt.Sprintf("direct:%s:%d:%d:%s", Hex(p[sender:sender+6]), p[typ], readU32(p, ts), Hex(p[body:])), true
}

func channelIdentity(p []byte, channel, typ, ts, body int) (string, bool) {
	if len(p) < body {
		return "", false
	}
	return fmt.Sprintf("channel:%d:%d:%d:%s", p[channel], p[typ], readU32(p, ts), Hex(p[body:])), true
}

func readU32(p []byte, off int) uint32 {
	if len(p) < off+4 {
		return 0
	}
	return uint32(p[off]) | uint32(p[off+1])<<8 | uint32(p[off+2])<<16 | uint32(p[off+3])<<24
}

func dup(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
