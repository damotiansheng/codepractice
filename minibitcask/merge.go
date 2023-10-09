package minibitcask

import (
	"io"
	"minibitcask/utils"
	"minibitcask/wal"
	"os"
	"time"
)

type Merge struct {
	interval time.Duration
	beginCh  chan struct{}
	closeCh  chan struct{}
	endCh    chan error
	db       *DB
}

func NewMerge(db *DB) *Merge {
	return &Merge{
		interval: db.GetOpt().GetMergeInteval(),
		db:       db,
		closeCh: make(chan struct{}),
		beginCh: make(chan struct{}),
		endCh: make(chan error),
	}
}

func (m *Merge) Start() {
	if 0 == m.interval {
		return
	}

	tick := time.NewTicker(m.interval)
	go func() {
		for {
			select {
			case <-tick.C:
				m.merge()
			case <-m.beginCh:
				m.endCh <- m.merge()
				tick.Reset(m.interval)
			case <-m.closeCh:
				tick.Stop()
				return
			}
		}
	}()
}

func (m *Merge) Stop() {
	m.closeCh <- struct{}{}
}

func (m *Merge) beginMerge() error {
	m.beginCh <- struct{}{}
	return <-m.endCh
}

func (m *Merge) Close() {
	m.Stop()
	close(m.closeCh)
	close(m.beginCh)
	close(m.endCh)
}

func (m *Merge) merge() error {
	// get need merge files
	fids, err := utils.GetDataFiles(m.db.GetOpt().GetDir(), wal.SEGMENT_FILE_EXT)
	if err != nil {
		return err
	}

	// no need merge
	if len(fids) <= 1 {
		return err
	}

	// rotate file
	if err := m.db.Rotate(); err != nil {
		return err
	}

	reader, err := m.db.wal.NewWalReader(fids[len(fids) - 1])
	if err != nil {
		return err
	}

	// interate
	for {
		data, walPos, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// judge data is valid or not
		record := DecodeRecord(data)
		if record.GetFlag() == TYPE_RECORD_DELETE {
			continue
		}

		if err = m.db.MergeRecord(data, record, walPos); err != nil {
			return err
		}
	}

	// delete merged files
	for _, fid := range fids {
		file := utils.GetSegmentFilePath(m.db.GetOpt().GetDir(), fid, wal.SEGMENT_FILE_EXT)
		// delete file in filesystem
		if err = os.Remove(file); err != nil {
			return err
		}
	}

	return err
}


