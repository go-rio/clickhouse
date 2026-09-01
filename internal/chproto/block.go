package chproto

import "fmt"

// Column describes one block column.
type Column struct {
	Name string
	Type string
}

// Rows streams a query's result blocks, reusing decoders across blocks and
// queries. Accessors return borrowed data valid only until the next Next.
type Rows struct {
	conn  *Conn
	cols  []Column
	decs  []Decoder
	names []string // built by Names, reset with the column shape

	rows      int // rows in the current block
	idx       int // current row, -1 before the first Next of a block
	blockSeen bool
	done      bool
	err       error
}

// reset readies the Rows for a fresh query, keeping decoders for reuse.
func (r *Rows) reset() {
	r.rows, r.idx, r.blockSeen, r.done, r.err = 0, -1, false, false, nil
}

// Columns returns the result's column descriptors.
func (r *Rows) Columns() []Column { return r.cols }

// Names returns the column names, built once per column shape.
func (r *Rows) Names() []string {
	if r.names == nil {
		r.names = make([]string, len(r.cols))
		for i, c := range r.cols {
			r.names[i] = c.Name
		}
	}
	return r.names
}

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

// pump reads packets until a non-empty data block, end of stream, or error;
// on return either rows > 0 with idx reset, or the stream is finished.
func (r *Rows) pump() error {
	c := r.conn
	r.rows, r.idx = 0, -1
	for {
		pt, err := c.nextPacket()
		if err != nil {
			r.err = err
			return err
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

	if !r.blockSeen {
		// The first block establishes the shape; decoders from an earlier
		// query are reused per column when the type matches.
		r.blockSeen = true
		if ncols != len(r.cols) {
			r.names = nil
		}
		cols := make([]Column, 0, ncols)
		decs := make([]Decoder, 0, ncols)
		for i := range ncols {
			name, err := c.readString()
			if err != nil {
				return 0, c.fail(err)
			}
			typ, err := c.readString()
			if err != nil {
				return 0, c.fail(err)
			}
			if err := c.readColumnFlag(i); err != nil {
				return 0, err
			}
			if i < len(r.cols) && typ == r.cols[i].Type {
				decs = append(decs, r.decs[i])
			} else {
				dec, err := newDecoder(typ, c.timezone)
				if err != nil {
					return 0, c.fail(err)
				}
				decs = append(decs, dec)
				r.names = nil
			}
			if i < len(r.cols) && name != r.cols[i].Name {
				r.names = nil
			}
			cols = append(cols, Column{Name: name, Type: typ})
			if nrows > 0 {
				if err := decs[i].read(c, nrows); err != nil {
					return 0, c.fail(err)
				}
			}
		}
		r.cols, r.decs = cols, decs
		return nrows, nil
	}
	// Later blocks must keep the first block's shape.
	if ncols != len(r.cols) {
		return 0, c.fail(fmt.Errorf("chproto: block column count changed: %d != %d", ncols, len(r.cols)))
	}
	for i := range ncols {
		if err := c.skipString(); err != nil { // name is fixed per shape
			return 0, c.fail(err)
		}
		typ, err := c.readTypeScratch()
		if err != nil {
			return 0, c.fail(err)
		}
		if typ != r.cols[i].Type {
			return 0, c.fail(fmt.Errorf("chproto: column %d type changed: %s != %s", i, typ, r.cols[i].Type))
		}
		if err := c.readColumnFlag(i); err != nil {
			return 0, err
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
	}
	return r.err
}

// Err reports the first error hit while streaming.
func (r *Rows) Err() error { return r.err }
