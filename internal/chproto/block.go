package chproto

import (
	"fmt"
	"io"
)

// Column describes one block column.
type Column struct {
	Name string
	Type string
}

// Rows streams a query's result blocks. Column decoders and their buffers
// are built on the first block and reused for every following one; value
// accessors return borrowed data valid only until the next Next call.
type Rows struct {
	conn *Conn
	cols []Column
	decs []Decoder

	rows int // rows in the current block
	idx  int // current row, -1 before the first Next of a block
	done bool
	err  error
}

// Columns returns the result's column descriptors.
func (r *Rows) Columns() []Column { return r.cols }

// Decoder exposes column i's typed accessor for the current row.
func (r *Rows) Decoder(i int) Decoder { return r.decs[i] }

// Row returns the current row index within the block.
func (r *Rows) Row() int { return r.idx }

// Next advances to the next row, pulling the next block when the current one
// is exhausted. It returns false at end of stream or on error.
func (r *Rows) Next() bool {
	if r.err != nil {
		return false
	}
	if r.idx+1 < r.rows {
		r.idx++
		return true
	}
	if r.done {
		return false
	}
	if err := r.pump(); err != nil || r.rows == 0 {
		return false
	}
	r.idx = 0
	return true
}

// pump reads packets until a non-empty data block, end of stream, or error.
// On return either rows > 0 with idx before the first row, or the stream is
// finished.
func (r *Rows) pump() error {
	c := r.conn
	r.rows, r.idx = 0, -1
	if r.done {
		return nil
	}
	for {
		pt, err := c.readUvarint()
		if err != nil {
			r.err = c.fail(err)
			return r.err
		}
		switch pt {
		case serverData:
			nrows, err := r.readBlock()
			if err != nil {
				r.err = err
				return err
			}
			if nrows > 0 {
				r.rows = nrows
				return nil
			}
		case serverEndOfStream:
			r.done = true
			return nil
		case serverException:
			r.err = c.readException()
			r.done = true
			return r.err
		case serverProgress:
			if err := c.skipProgress(); err != nil {
				r.err = c.fail(err)
				return r.err
			}
		case serverProfileInfo:
			if err := c.skipProfileInfo(); err != nil {
				r.err = c.fail(err)
				return r.err
			}
		case serverTotals, serverExtremes, serverLog, serverProfileEvents:
			if err := c.skipMetaBlock(); err != nil {
				r.err = c.fail(err)
				return r.err
			}
		case serverTableColumns:
			if err := c.skipString(); err != nil {
				r.err = c.fail(err)
				return r.err
			}
			if err := c.skipString(); err != nil {
				r.err = c.fail(err)
				return r.err
			}
		default:
			r.err = c.fail(fmt.Errorf("chproto: unexpected packet %d", pt))
			return r.err
		}
	}
}

// readBlock decodes one data block into the reused column decoders.
func (r *Rows) readBlock() (int, error) {
	c := r.conn
	if err := c.skipString(); err != nil { // table name
		return 0, c.fail(err)
	}
	if err := c.skipBlockInfo(); err != nil {
		return 0, c.fail(err)
	}
	ncols64, err := c.readUvarint()
	if err != nil {
		return 0, c.fail(err)
	}
	nrows64, err := c.readUvarint()
	if err != nil {
		return 0, c.fail(err)
	}
	ncols, nrows := int(ncols64), int(nrows64)
	if ncols == 0 {
		return 0, nil // stream-boundary marker block
	}

	first := r.decs == nil
	if first {
		r.cols = make([]Column, ncols)
		r.decs = make([]Decoder, ncols)
	} else if ncols != len(r.cols) {
		return 0, c.fail(fmt.Errorf("chproto: block column count changed: %d != %d", ncols, len(r.cols)))
	}
	for i := range ncols {
		name, err := c.readString()
		if err != nil {
			return 0, c.fail(err)
		}
		typ, err := c.readString()
		if err != nil {
			return 0, c.fail(err)
		}
		flag, err := c.readByte()
		if err != nil {
			return 0, c.fail(err)
		}
		if flag != 0 {
			return 0, c.fail(fmt.Errorf("chproto: column %q uses custom serialization", name))
		}
		if first {
			r.cols[i] = Column{Name: name, Type: typ}
			dec, err := newDecoder(typ, c.timezone)
			if err != nil {
				return 0, c.fail(err)
			}
			r.decs[i] = dec
		} else if typ != r.cols[i].Type {
			return 0, c.fail(fmt.Errorf("chproto: column %q type changed: %s != %s", name, typ, r.cols[i].Type))
		}
		if nrows > 0 {
			if err := r.decs[i].read(c, nrows); err != nil {
				return 0, c.fail(err)
			}
		}
	}
	return nrows, nil
}

// Close drains the remainder of the stream so the connection can be reused.
func (r *Rows) Close() error {
	for r.err == nil && !r.done {
		if err := r.pump(); err != nil {
			break
		}
		// discard fetched rows
		r.idx = r.rows
	}
	if r.err != nil && r.err != io.EOF {
		return r.err
	}
	return nil
}

// Err reports the first error hit while streaming.
func (r *Rows) Err() error { return r.err }
