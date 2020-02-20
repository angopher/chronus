package controller

type Config struct {
	Enabled           bool `toml:"enabled"`
	MaxShardCopyTasks int  `toml:"max_shard_copy_tasks"`
}

func NewConfig() Config {
	return Config{
		Enabled:           true,
		MaxShardCopyTasks: 10,
	}
}
