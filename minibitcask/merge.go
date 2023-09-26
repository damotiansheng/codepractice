package minibitcask

import (
	"io"
	"minibitcask/utils"
	"os"
	"time"
)

type Merge struct {
	interval time.Duration
	beginCh  chan struct{}
	closeCh  chan struct{}
	endCh    chan struct{}
	db       *DB
}

func NewMerge(db *DB) *Merge {
	return &Merge{
		interval: db.GetOpt().GetMergeInteval(),
		db:       db,
		closeCh: make(chan struct{}),
		beginCh: make(chan struct{}),
		endCh: make(chan struct{}),
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
				m.merge()
				m.endCh <- struct{}{}
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

func (m *Merge) beginMerge() {
	m.beginCh <- struct{}{}
	<-m.endCh
}

func (m *Merge) Close() {
	m.Stop()
	close(m.closeCh)
	close(m.beginCh)
	close(m.endCh)
}

func (m *Merge) merge() error {
	// get need merge files
	fids, err := utils.GetDataFiles(m.db.GetOpt().GetDir(), utils.DATA_FILE_EXT)
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

	// merge every file
	for _, fid := range fids {
		err = m.mergeFile(fid)
		if err != nil {
			return err
		}
	}

	// delete merged files
	for _, fid := range fids {
		file := utils.GetActiveFilePath(m.db.GetOpt().GetDir(), fid)
		// delete file in filesystem
		if err = os.Remove(file); err != nil {
			return err
		}
	}

	return err
}

func (m *Merge) mergeFile(fid uint32) error {
	// foreach every record in file and judge is whether valid, ignore invalid record then write valid record to file and update index
	path := utils.GetActiveFilePath(m.db.GetOpt().GetDir(), fid)

	var offset int64
	// Open the file for reading
	readFile, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	for {
		record, err := ReadRecord(readFile, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		offset += int64(record.Size())

		// ignore deleted records
		if record.GetFlag() == TYPE_RECORD_DELETE {
			continue
		}

		if err = m.db.MergeRecord(record); err != nil {
			return err
		}
	}

	return nil
}

