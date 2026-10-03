package rtpaudio

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// WAVData serializes emitted output records into mono, 16-bit little-endian
// PCM WAV bytes. Missing records contribute the zero samples already present
// in OutputFrame.
func WAVData(frames []OutputFrame) []byte {
	sampleCount := uint32(len(frames) * SamplesPerPacket)
	dataBytes := sampleCount * 2
	buf := bytes.NewBuffer(make([]byte, 0, 44+int(dataBytes)))

	writeASCII := func(s string) { buf.WriteString(s) }
	writeU32 := func(v uint32) { _ = binary.Write(buf, binary.LittleEndian, v) }
	writeU16 := func(v uint16) { _ = binary.Write(buf, binary.LittleEndian, v) }

	writeASCII("RIFF")
	writeU32(36 + dataBytes)
	writeASCII("WAVE")
	writeASCII("fmt ")
	writeU32(16)
	writeU16(1) // PCM
	writeU16(1) // mono
	writeU32(ClockRate)
	writeU32(ClockRate * 2) // average bytes per second
	writeU16(2)             // block align
	writeU16(16)            // bits per sample
	writeASCII("data")
	writeU32(dataBytes)

	for _, frame := range frames {
		_ = binary.Write(buf, binary.LittleEndian, frame.Samples[:])
	}
	return buf.Bytes()
}

// ExportGeneration writes one generation's WAV and JSON missing report.
// Filenames are generated from the sanitized source and the resolved
// generation number. Generation 0 is resolved once, so both files describe the
// same immutable snapshot. Existing files are replaced, as requested by the
// caller.
func (r *Receiver) ExportGeneration(dir string, key SourceKey, generation uint64) (wavPath, reportPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	generation, frames, report, err := r.exportSnapshot(key, generation)
	if err != nil {
		return "", "", err
	}

	base := sanitizeFilename(string(key))
	id := uintToString(generation)
	wavPath = filepath.Join(dir, base+".gen"+id+".wav")
	reportPath = filepath.Join(dir, base+".gen"+id+".missing.json")

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", "", err
	}
	data = append(data, '\n')

	if err := os.WriteFile(wavPath, WAVData(frames), 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(reportPath, data, 0o644); err != nil {
		return "", "", err
	}
	return wavPath, reportPath, nil
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "source"
	}
	var b strings.Builder
	for _, r := range name {
		switch r {
		case ':', '/', '\\', '*', '?', '"', '<', '>', '|', ' ':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		return "source"
	}
	return out
}
