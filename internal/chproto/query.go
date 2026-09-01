package chproto

import (
	"context"
	"fmt"
)

// Exception is a server-reported error.
type Exception struct {
	Code    int32
	Name    string
	Message string
}

func (e *Exception) Error() string {
	return fmt.Sprintf("clickhouse: %s (code %d): %s", e.Name, e.Code, e.Message)
}

// sendQuery writes a Query packet followed by the empty external-data block.
// settings rides each query; rio always sends the low-cardinality opt-out so
// the server delivers plain columns.
func (c *Conn) sendQuery(query string) error {
	c.writeUvarint(clientQuery)
	c.writeString("") // query id: server assigns
	// client info
	c.writeByte(1) // initial query
	c.writeString("")
	c.writeString("")
	c.writeString("0.0.0.0:0")
	var zero [8]byte
	c.w.Write(zero[:]) // initial query start time
	c.writeByte(1)     // tcp
	c.writeString("")  // os user
	c.writeString("")  // hostname
	c.writeString("go-rio/clickhouse")
	c.writeUvarint(0)
	c.writeUvarint(1)
	c.writeUvarint(Revision)
	c.writeString("") // quota key
	c.writeUvarint(0) // distributed depth
	c.writeUvarint(0) // version patch
	c.writeByte(0)    // no opentelemetry
	c.writeUvarint(0) // parallel replicas: collaborate
	c.writeUvarint(0) // count
	c.writeUvarint(0) // replica number
	// settings (name, flags, value as strings; empty name terminates)
	c.writeString("low_cardinality_allow_in_native_format")
	c.writeUvarint(0)
	c.writeString("0")
	c.writeString("")
	c.writeString("") // interserver secret
	c.writeUvarint(2) // stage: complete
	c.writeUvarint(0) // compression: off
	c.writeString(query)
	c.writeString("") // parameters terminator
	c.writeEmptyBlock()
	return c.flush()
}

func (c *Conn) writeEmptyBlock() {
	c.writeUvarint(clientData)
	c.writeString("")
	c.writeBlockHeader(0, 0)
}

func (c *Conn) writeBlockHeader(cols, rows int) {
	c.writeUvarint(1)
	c.writeByte(0) // is_overflows
	c.writeUvarint(2)
	c.w.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // bucket -1
	c.writeUvarint(0)
	c.writeUvarint(uint64(cols))
	c.writeUvarint(uint64(rows))
}

func (c *Conn) readException() error {
	first := &Exception{}
	cur := first
	for {
		code, err := c.readInt32()
		if err != nil {
			return c.fail(err)
		}
		name, err := c.readString()
		if err != nil {
			return c.fail(err)
		}
		msg, err := c.readString()
		if err != nil {
			return c.fail(err)
		}
		if err := c.skipString(); err != nil { // stack trace
			return c.fail(err)
		}
		nested, err := c.readByte()
		if err != nil {
			return c.fail(err)
		}
		cur.Code, cur.Name, cur.Message = code, name, msg
		if nested == 0 {
			return first
		}
	}
}

func (c *Conn) skipProgress() error {
	for range 6 { // read rows/bytes, total rows, written rows/bytes, elapsed
		if _, err := c.readUvarint(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) skipProfileInfo() error {
	for _, kind := range [6]byte{'v', 'v', 'v', 'b', 'v', 'b'} {
		var err error
		if kind == 'v' {
			_, err = c.readUvarint()
		} else {
			_, err = c.readByte()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// nextPacket reads server packets, consuming every mid-stream informational
// one, and returns the first packet the caller must act on. Errors poison
// the connection.
func (c *Conn) nextPacket() (uint64, error) {
	for {
		pt, err := c.readUvarint()
		if err != nil {
			return 0, c.fail(err)
		}
		switch pt {
		case serverProgress:
			err = c.skipProgress()
		case serverProfileInfo:
			err = c.skipProfileInfo()
		case serverTotals, serverExtremes, serverLog, serverProfileEvents:
			err = c.skipMetaBlock()
		case serverTableColumns:
			if err = c.skipString(); err == nil {
				err = c.skipString()
			}
		default:
			return pt, nil
		}
		if err != nil {
			return 0, c.fail(err)
		}
	}
}

// skipBlockInfo consumes the field-tagged BlockInfo prefix.
func (c *Conn) skipBlockInfo() error {
	for {
		f, err := c.readUvarint()
		if err != nil {
			return err
		}
		switch f {
		case 0:
			return nil
		case 1:
			if _, err := c.readByte(); err != nil {
				return err
			}
		case 2:
			if _, err := c.readInt32(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("chproto: unknown block info field %d", f)
		}
	}
}

// skipMetaBlock consumes a Log/ProfileEvents block allocation-free: fixed
// widths discard through the buffered reader.
func (c *Conn) skipMetaBlock() error {
	if err := c.skipString(); err != nil { // table name
		return err
	}
	if err := c.skipBlockInfo(); err != nil {
		return err
	}
	cols, err := c.readUvarint()
	if err != nil {
		return err
	}
	rows, err := c.readUvarint()
	if err != nil {
		return err
	}
	for range cols {
		if err := c.skipString(); err != nil { // name
			return err
		}
		typ, err := c.readTypeScratch()
		if err != nil {
			return err
		}
		if _, err := c.readByte(); err != nil { // custom serialization flag
			return err
		}
		if err := skipColumnData(c, typ, int(rows)); err != nil {
			return err
		}
	}
	return nil
}

// readColumnFlag consumes a column's custom-serialization flag byte.
func (c *Conn) readColumnFlag(i int) error {
	flag, err := c.readByte()
	if err != nil {
		return c.fail(err)
	}
	if flag != 0 {
		return c.fail(fmt.Errorf("chproto: column %d uses custom serialization", i))
	}
	return nil
}

// readTypeScratch reads a type string into the connection's scratch buffer.
func (c *Conn) readTypeScratch() (string, error) {
	var err error
	c.typeBuf, err = c.readStringInto(c.typeBuf[:0])
	if err != nil {
		return "", err
	}
	return string(c.typeBuf), nil
}

// readMetaColumns parses an INSERT schema sample's column descriptors; the
// sample carries no rows.
func (c *Conn) readMetaColumns() ([]Column, error) {
	if err := c.skipBlockInfo(); err != nil {
		return nil, err
	}
	cols, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	if _, err := c.readUvarint(); err != nil { // rows: always zero
		return nil, err
	}
	out := make([]Column, cols)
	for i := range out {
		if out[i].Name, err = c.readString(); err != nil {
			return nil, err
		}
		if out[i].Type, err = c.readString(); err != nil {
			return nil, err
		}
		if flag, err := c.readByte(); err != nil {
			return nil, err
		} else if flag != 0 {
			return nil, fmt.Errorf("chproto: column %q uses custom serialization", out[i].Name)
		}
	}
	return out, nil
}

// Query executes a row-returning statement. The returned Rows must be fully
// consumed (Next until false, or Close) before the connection is reused; it
// is owned by the connection and recycled — with its decoders and buffers —
// by the next Query.
func (c *Conn) Query(ctx context.Context, query string) (*Rows, error) {
	c.applyDeadline(ctx)
	if err := c.sendQuery(query); err != nil {
		return nil, c.fail(err)
	}
	if c.rows == nil {
		c.rows = &Rows{conn: c}
	}
	rows := c.rows
	rows.reset()
	// prefetch until the first data block or terminal packet, so execution
	// errors surface here rather than on the first Next.
	if err := rows.pump(); err != nil {
		return nil, err
	}
	return rows, nil
}

// Exec executes a statement and drains its response.
func (c *Conn) Exec(ctx context.Context, query string) error {
	rows, err := c.Query(ctx, query)
	if err != nil {
		return err
	}
	return rows.Close()
}
