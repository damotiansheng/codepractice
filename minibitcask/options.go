package minibitcask

type Options struct {
	dir	string
	syncEnable bool
	maxActiveFileSize uint32
}

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

func WithMaxActiveFileSize(maxActiveFileSize uint32) Option {
	return func(options *Options) {
		options.maxActiveFileSize = maxActiveFileSize
	}
}

// NewOptions returns a new Options
func NewOptions(opts ...Option) Options {
	options := Options{
		dir:	"",
		syncEnable: false,
	}

	for _, o := range opts {
		o(&options)
	}

	return options
}


