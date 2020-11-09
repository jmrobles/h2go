package h2go

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var doLogging = false

type h2connInfo struct {
	host     string
	port     int
	database string
	username string
	password string
	isMem    bool
	logging  bool

	dialer net.Dialer
}
type h2Driver struct {
	driver.DriverContext
	driver.Driver
}

type h2Connector struct {
	driver.Connector

	ci     h2connInfo
	driver h2Driver
}

func (h2d h2Driver) Open(dsn string) (driver.Conn, error) {
	ci, err := parseURL(dsn)
	L(log.InfoLevel, "Open")
	L(log.DebugLevel, "Open with dsn: %s", dsn)
	if err != nil {
		return nil, err
	}
	return connect(ci)
}

func (h2d *h2Driver) OpenConnector(dsn string) (driver.Connector, error) {
	L(log.DebugLevel, "OpenConnector")
	ci, err := parseURL(dsn)
	if err != nil {
		return nil, err
	}
	return &h2Connector{ci: ci, driver: *h2d}, nil
}

func (h2c *h2Connector) Connect(ctx context.Context) (driver.Conn, error) {
	L(log.DebugLevel, "Connect")
	return connect(h2c.ci)
}

func (h2c *h2Connector) Driver() driver.Driver {
	return h2c.driver
}
func init() {
	sql.Register("h2", &h2Driver{})
}

// Helpers

func parseURL(dsnurl string) (h2connInfo, error) {
	var ci h2connInfo
	u, err := url.Parse(dsnurl)
	if err != nil {
		return ci, errors.Wrapf(err, "failed to parse connection url")
	}
	// Set host
	if ci.host = u.Hostname(); len(ci.host) == 0 {
		ci.host = "127.0.0.1"
	}
	// Set port
	ci.port, _ = strconv.Atoi(u.Port())
	if ci.port == 0 {
		ci.port = defaultH2port
	}
	// Set database
	if ci.database = u.Path; len(ci.database) == 0 {
		ci.database = "~/test"
	}
	// Username & password
	userinfo := u.User
	if userinfo != nil {
		ci.username = userinfo.Username()
		if pass, ok := userinfo.Password(); ok {
			ci.password = pass
		}
	}
	for k, v := range u.Query() {
		var val string
		if len(v) > 0 {
			val = strings.TrimSpace(v[0])
		}
		switch strings.ToLower(k) {
		case "mem":
			ci.isMem = val == "" || val == "1" || val == "yes" || val == "true"
			if ci.isMem {
				ci.database = strings.Replace(ci.database, "/", "", 1)
				ci.database = "mem:" + ci.database
			}
		case "logging":
			logType := strings.ToLower(v[0])
			switch logType {
			case "none":
				doLogging = false
			case "info":
				doLogging = true
				log.SetLevel(log.InfoLevel)
			case "debug":
				doLogging = true
				log.SetLevel(log.DebugLevel)
			case "error":
				doLogging = true
				log.SetLevel(log.ErrorLevel)
			case "warn":
			case "warning":
				doLogging = true
				log.SetLevel(log.WarnLevel)
			case "panic":
				doLogging = true
				log.SetLevel(log.PanicLevel)
			case "trace":
				doLogging = true
				log.SetLevel(log.TraceLevel)
			}
		default:
			return ci, errors.Errorf("unknown H2 server connection parameters => \"%s\" : \"%s\"", k, val)
		}

	}
	return ci, nil
}
