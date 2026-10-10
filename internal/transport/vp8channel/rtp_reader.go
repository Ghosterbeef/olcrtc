package vp8channel

import (
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// reorderWindow bounds how many out-of-order RTP packets the reorder buffer
// holds while waiting for a gap to fill. Real SFUs reorder within a handful of
// packets; once this many newer packets pile up behind a hole, the missing
// sequence is treated as genuinely lost and we advance, so a truly dropped
// packet cannot stall delivery indefinitely.
const (
	reorderWindow   = 256
	maxReorderDelay = 30 * time.Millisecond
)

// Reordered RTP packets normally carry an MTU-sized payload. Larger buffers
// are not retained after delivery so one malformed packet cannot pin a large
// allocation in the reorder buffer's local free list.
const maxRetainedRTPPayloadCap = 2 * 1024

// maxAssembledFrameSize bounds one reassembled VP8 frame. The buffer only
// resets on a start-of-partition bit, a sequence gap or a marker bit, so a
// contiguous run of packets that never sets the marker grows it without limit
// - roughly 1.2 KB per packet, for as long as the peer keeps sending. The cap
// sits well above defaultMaxPayloadSize plus the batching overhead, so no
// legitimate frame reaches it.
const maxAssembledFrameSize = 4 * defaultMaxPayloadSize

// seqLess reports whether RTP sequence a precedes b using wrap-around aware
// comparison (RFC 1982 serial arithmetic on uint16).
func seqLess(a, b uint16) bool {
	// bit15 of the wrap-around difference is the serial sign bit: set means a
	// precedes b. Avoids a signed conversion gosec flags as overflow.
	return (a-b)&0x8000 != 0
}

// reorderBuffer restores RTP sequence order before frame assembly. The SFU may
// deliver packets out of order or drop them; feeding that stream straight into
// the strict contiguity check in processRTPPacket made every reorder look like
// loss and discarded whole frames (issue #95: ~80-90% of VP8 frames dropped on
// a live SFU). Buffering by sequence number and draining in order means only
// genuine loss produces a gap.
type reorderBuffer struct {
	pkts         map[uint16]*rtp.Packet
	free         []*rtp.Packet
	nextSeq      uint16
	started      bool
	gapStartTime time.Time
}

func newReorderBuffer() *reorderBuffer {
	return &reorderBuffer{pkts: make(map[uint16]*rtp.Packet, reorderWindow)}
}

// push adds pkt and synchronously delivers packets now available in strict
// sequence order. The caller reuses its read buffer, so buffered payloads are
// copied into reorder-owned storage and recycled immediately after deliver
// returns. deliver must not retain the packet.
func (b *reorderBuffer) push(pkt *rtp.Packet, deliver func(*rtp.Packet)) {
	if !b.started {
		b.started = true
		b.nextSeq = pkt.SequenceNumber
	}
	// Drop packets older than our current position: already delivered, or
	// skipped past as lost.
	if seqLess(pkt.SequenceNumber, b.nextSeq) {
		return
	}
	if old := b.pkts[pkt.SequenceNumber]; old != nil {
		b.recycle(old)
	}
	b.pkts[pkt.SequenceNumber] = b.clone(pkt)

	// Holding a full window behind a hole, or waiting longer than maxReorderDelay
	// for the missing packet, means the head sequence is genuinely lost: skip forward.
	if len(b.pkts) > reorderWindow || (!b.gapStartTime.IsZero() && time.Since(b.gapStartTime) >= maxReorderDelay) {
		b.skipToOldest()
		b.gapStartTime = time.Time{}
	}

	b.drain(deliver)

	// Advance through remaining internal gaps if timed out
	for len(b.pkts) > 0 {
		if !b.gapStartTime.IsZero() && time.Since(b.gapStartTime) >= maxReorderDelay {
			b.skipToOldest()
			b.drain(deliver)
		} else {
			break
		}
	}

	if len(b.pkts) > 0 {
		if b.gapStartTime.IsZero() {
			b.gapStartTime = time.Now()
		}
	} else {
		b.gapStartTime = time.Time{}
	}
}

// flushTimeout forces delivery of buffered packets if waiting past maxReorderDelay.
func (b *reorderBuffer) flushTimeout(deliver func(*rtp.Packet)) {
	if len(b.pkts) == 0 {
		return
	}
	if !b.gapStartTime.IsZero() && time.Since(b.gapStartTime) >= maxReorderDelay {
		for len(b.pkts) > 0 {
			b.skipToOldest()
			b.drain(deliver)
		}
		b.gapStartTime = time.Time{}
	}
}

// drain pops contiguous packets starting at nextSeq.
func (b *reorderBuffer) drain(deliver func(*rtp.Packet)) {
	for {
		pkt, ok := b.pkts[b.nextSeq]
		if !ok {
			return
		}
		delete(b.pkts, b.nextSeq)
		b.nextSeq++
		deliver(pkt)
		b.recycle(pkt)
	}
}

func (b *reorderBuffer) clone(pkt *rtp.Packet) *rtp.Packet {
	var clone *rtp.Packet
	if last := len(b.free) - 1; last >= 0 {
		clone = b.free[last]
		b.free = b.free[:last]
	} else {
		clone = &rtp.Packet{}
	}
	clone.Header = pkt.Header
	if cap(clone.Payload) < len(pkt.Payload) {
		clone.Payload = make([]byte, len(pkt.Payload))
	} else {
		clone.Payload = clone.Payload[:len(pkt.Payload)]
	}
	copy(clone.Payload, pkt.Payload)
	return clone
}

func (b *reorderBuffer) recycle(pkt *rtp.Packet) {
	pkt.Header = rtp.Header{}
	if cap(pkt.Payload) > maxRetainedRTPPayloadCap {
		pkt.Payload = nil
	} else {
		pkt.Payload = pkt.Payload[:0]
	}
	b.free = append(b.free, pkt)
}

func (b *reorderBuffer) findOldest() uint16 {
	first := true
	var oldest uint16
	for seq := range b.pkts {
		if first || seqLess(seq, oldest) {
			oldest = seq
			first = false
		}
	}
	return oldest
}

func (b *reorderBuffer) skipToOldest() {
	if len(b.pkts) == 0 {
		return
	}
	b.nextSeq = b.findOldest()
}

// vp8FrameState tracks the packet-by-packet reassembly of a single VP8 frame.
type vp8FrameState struct {
	frameBuf    []byte
	frameValid  bool
	haveLastSeq bool
	lastSeq     uint16
	vp8Pkt      codecs.VP8Packet
}

// processRTPPacket reassembles contiguous RTP packets into full frames.
// Any sequence gap discards the partially-assembled frame so an upstream
// loss/reordering to avoid silently corrupting fragmented VP8 frames.
func (s *vp8FrameState) processRTPPacket(pkt *rtp.Packet) []byte {
	if s.haveLastSeq && pkt.SequenceNumber != s.lastSeq+1 {
		logger.Warnf("vp8channel: rtp seq gap got=%d want=%d bufLen=%d", pkt.SequenceNumber, s.lastSeq+1, len(s.frameBuf))
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
	}
	s.lastSeq = pkt.SequenceNumber
	s.haveLastSeq = true

	vp8Payload, err := s.vp8Pkt.Unmarshal(pkt.Payload)
	if err != nil {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	if s.vp8Pkt.S == 1 {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = true
	}

	if !s.frameValid {
		return nil
	}

	if len(s.frameBuf)+len(vp8Payload) > maxAssembledFrameSize {
		// A frame this large is not something the writer can produce, so the
		// marker bit is never coming. Drop what we have and wait for the next
		// start-of-partition rather than growing forever.
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	s.frameBuf = append(s.frameBuf, vp8Payload...)

	if !pkt.Marker {
		return nil
	}

	frame := s.frameBuf
	s.frameBuf = s.frameBuf[:0]
	s.frameValid = false
	if len(frame) >= epochHdrLen {
		logger.Debugf("vp8channel: frame reassembled len=%d seq=%d", len(frame), pkt.SequenceNumber)
		return frame
	}
	return nil
}

func (p *streamTransport) handleRemoteTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		go p.drainTrack(track)
		return
	}

	// We don't reset KCP here. Peer restarts are detected by the epoch
	// header on incoming frames, which works even when the SFU keeps
	// forwarding the same track across our restarts.
	go p.readVP8Track(track)
}

func (p *streamTransport) drainTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, rtpBufSize)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

func (p *streamTransport) readVP8Track(track *webrtc.TrackRemote) {
	var state vp8FrameState
	reorder := newReorderBuffer()
	var rtpCount, frameCount int

	type readResult struct {
		pkt *rtp.Packet
		err error
	}
	pktCh := make(chan readResult, 8192)

	go func() {
		buf := make([]byte, rtpBufSize)
		for {
			n, _, err := track.Read(buf)
			if err != nil {
				pktCh <- readResult{err: err}
				return
			}
			pkt := &rtp.Packet{}
			if err := pkt.Unmarshal(buf[:n]); err != nil {
				continue
			}
			payload := make([]byte, len(pkt.Payload))
			copy(payload, pkt.Payload)
			pkt.Payload = payload

			select {
			case pktCh <- readResult{pkt: pkt}:
			case <-p.closeCh:
				return
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	deliver := func(ordered *rtp.Packet) {
		frame := state.processRTPPacket(ordered)
		if frame == nil {
			return
		}
		frameCount++
		p.handleIncomingFrame(frame)
	}

	for {
		select {
		case <-p.closeCh:
			return
		case res := <-pktCh:
			if res.err != nil {
				logger.Infof("vp8channel: readVP8Track closed track=%s rtp=%d frames=%d err=%v",
					track.ID(), rtpCount, frameCount, res.err)
				return
			}
			rtpCount++
			reorder.push(res.pkt, deliver)
		case <-ticker.C:
			reorder.flushTimeout(deliver)
		}
	}
}
