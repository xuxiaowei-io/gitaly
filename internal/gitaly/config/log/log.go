package log

// Config contains logging configuration values
type Config struct {
	Dir    string `toml:"dir,omitempty" json:"dir"`
	Format string `toml:"format,omitempty" json:"format"`
	Level  string `toml:"level,omitempty" json:"level"`
}
