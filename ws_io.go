// readFrame parses the next WebSocket frame from the TCP stream.
// Assumptions:
//   - Infura sends non-fragmented, masked data frames (opcode 0x1/0x2)
//   - Frame is ≤ maxFrameSize
//   - Control frames are skipped
//
// Compiler Directives:
//   - nosplit → ensures zero stack churn during loop
//
//go:nosplit
func readFrame(conn net.Conn) (*wsFrame, error) {
	for {
		// ───── Step 1: Minimal 2-byte header ─────
		if err := ensureRoom(conn, 2); err != nil {
			return nil, err
		}
		hdr0 := wsBuf[wsStart]
		hdr1 := wsBuf[wsStart+1]

		fin := hdr0 & 0x80    // FIN bit (must be 1)
		opcode := hdr0 & 0x0F // 0x1 = text, 0x2 = binary
		masked := hdr1 & 0x80 // must be set (client-to-server)
		plen7 := int(hdr1 & 0x7F)

		// ───── Step 2: Control Frame Handling ─────
		switch opcode {
		case 0x8: // CLOSE
			return nil, io.EOF
		case 0x9, 0xA: // PING / PONG
			wsStart += 2
			wsLen -= 2
			continue
		}

		// ───── Step 3: Decode Payload Length ─────
		offset := 2
		var plen int

		switch plen7 {
		case 126:
			if err := ensureRoom(conn, offset+2); err != nil {
				return nil, err
			}
			plen = int(binary.BigEndian.Uint16(wsBuf[wsStart+offset:]))
			offset += 2
		case 127:
			if err := ensureRoom(conn, offset+8); err != nil {
				return nil, err
			}
			plen64 := binary.BigEndian.Uint64(wsBuf[wsStart+offset:])
			if plen64 > maxFrameSize {
				return nil, fmt.Errorf("frame %d exceeds cap", plen64)
			}
			plen = int(plen64)
			offset += 8
		default:
			plen = plen7
		}

		// ───── Step 4: Extract Masking Key ─────
		var mkey uint32
		if masked != 0 {
			if err := ensureRoom(conn, offset+4); err != nil {
				return nil, err
			}
			mkey = *(*uint32)(unsafe.Pointer(&wsBuf[wsStart+offset]))
			offset += 4
		}

		// ───── Step 5: Read Payload ─────
		if err := ensureRoom(conn, offset+plen); err != nil {
			return nil, err
		}
		payloadStart := wsStart + offset
		payloadEnd := payloadStart + plen

		// ───── Step 6: Apply Mask (RFC 6455) ─────
		if masked != 0 {
			key := [4]byte{}
			*(*uint32)(unsafe.Pointer(&key[0])) = mkey
			for i := 0; i < plen; i++ {
				wsBuf[payloadStart+i] ^= key[i&3]
			}
		}

		// ───── Step 7: Reject Fragmented Frames ─────
		if fin == 0 {
			return nil, fmt.Errorf("fragmented frames not supported")
		}

		// ───── Step 8: Register Frame in Ring ─────
		idx := wsHead & (frameCap - 1)
		f := &wsFrames[idx]
		f.Payload = wsBuf[payloadStart:payloadEnd]
		f.Len = plen
		f.End = payloadEnd
		wsHead++
		return f, nil
	}
}
