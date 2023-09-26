package minibitcask

import "time"

type Options struct {
	dir	string
	syncEnable bool
	maxActiveFileSize uint32
	mergeInteval time.Duration
}

var (
	DefaultOptions = &Options{
		dir:				"/tmp/",
		syncEnable:			false,
		maxActiveFileSize:	1024*1024,
		mergeInteval:		time.Hour,}
)

type Option func(*Options)

func WithDir(dir string) Option {
	return func(options *Options) {
		options.dir = dir
	}
}

func WithSyncEnable(syncEnable bool) Option {
	return func(options *Options) {
		options.syncEnable = syncEnable
	}
}

func WithMergeInteval(mergeInteval time.Duration) Option {
	return func(options *Options) {
		options.mergeInteval = mergeInteval
	}
}

func WithMaxActiveFileSize(maxActiveFileSize uint32) Option {
	return func(options *Options) {
		options.maxActiveFileSize = maxActiveFileSize
	}
}

func (opt *Options) GetMergeInteval() time.Duration {
	return opt.mergeInteval
}

func (opt *Options) GetDir() string {
    return opt.dir
}

func (opt *Options) GetSyncEnable() bool {
    return opt.syncEnable
}

func (opt *Options) GetMaxActiveFileSize() uint32 {
    return opt.maxActiveFileSize
}


