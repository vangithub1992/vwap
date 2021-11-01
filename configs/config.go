package configs

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	Conn         *ConnectionConfig `json:"conn_config"`
	TradingPairs []string          `json:"trading_pairs"`
	Channels     []string          `json:"channels"`
}

type ConnectionConfig struct {
	SocketUrl string `json:"socket_url"`
}

func ReadConfig(fp string) (*Config, error) {
	c := &Config{}
	if _, err := os.Stat(fp); err != nil {
		log.Printf("[Config] error during read config file: %s", err)
		return nil, err
	}
	file, err := os.Open(fp)
	if err != nil {
		log.Printf("[Config] error during opening config file: %s", err)
		return nil, err
	}
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(c); err != nil {
		log.Printf("[Config] error during decoding config file to json: %s", err)
		return nil, err
	}

	return c, nil
}
