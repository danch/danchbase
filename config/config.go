package config

import "github.com/tkanos/gonfig"

// Config holds global configuration settings for danchbase
type Config struct {
	DataDir string 
	Port string
	BindHost string
}

var configuration  *Config

//Configure the Config object from all supported sources. Vague enough? Thought so.
//If this returns an error, you should probably just panic and get it over with, but that's your call
func Configure(configPath string) (*Config, error) {
	configuration = new(Config)
	err := gonfig.GetConf(configPath, configuration)
	if (err != nil) {
		return nil, err
	}
	return configuration, nil
}

//GetConfig returns the Config object that's been initialized, or nil if the Config hasn't been initialized
func GetConfig() *Config {
	return configuration
}

