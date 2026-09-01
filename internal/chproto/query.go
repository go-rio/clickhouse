package chproto

import (
	"context"
	"fmt"
	"time"
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

// skipMetaBlock consumes a Log/ProfileEvents/sample block without keeping it.
func (c *Conn) skipMetaBlock() error {
	if err := c.skipString(); err != nil { // table name
		return err
	}
	_, err := c.readMetaColumns()
	return err
}

// readMetaColumns parses a block's header and discards its column payloads,
// returning the column descriptors (used for INSERT schema samples).
func (c *Conn) readMetaColumns() ([]Column, error) {
	if err := c.skipBlockInfo(); err != nil {
		return nil, err
	}
	cols, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	rows, err := c.readUvarint()
	if err != nil {
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
		if rows > 0 {
			if err := discardColumn(c, out[i].Type, int(rows)); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// Query executes a row-returning statement. The returned Rows must be fully
// consumed (Next until false, or Close) before the connection is reused.
func (c *Conn) Query(ctx context.Context, query string) (*Rows, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.netc.SetDeadline(deadline)
	} else {
		c.netc.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(query); err != nil {
		return nil, c.fail(err)
	}
	rows := &Rows{conn: c}
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
