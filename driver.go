package h2go

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type h2Driver struct{}

func init() {
	sql.Register("h2", &h2Driver{})
}

func (h2d *h2Driver) Open(dsn string) (driver.Conn, error) {
	ci, err := h2d.parseURL(dsn)
	if err != nil {
		return nil, err
	}
	return connect(ci)
}

func (h2d *h2Driver) parseURL(dsnurl string) (h2connInfo, error) {
	var ci h2connInfo
	u, err := url.Parse(dsnurl)
	if err != nil {
		return ci, errors.Wrapf(err, "failed to parse connection url")
	}
	// Set host
	if ci.host = u.Hostname(); len(ci.host) == 0 {
		ci.host = "127.0.0.1"
	}
	log.Printf("HOST: %s", ci.host)
	// Set port
	ci.port, _ = strconv.Atoi(u.Port())
	if ci.port == 0 {
		ci.port = defaultH2port
	}
	// Set database
	if ci.database = u.Path; len(ci.database) == 0 {
		ci.database = "~/test"
	}
	log.Printf("database: %s", ci.database)
	// Username & password
	userinfo := u.User
	if userinfo != nil {
		log.Printf("has user info: %v", userinfo)
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
				ci.database = "mem:" + ci.database
			}
		default:
			return ci, errors.Errorf("unknown H2 server connection parameters => \"%s\" : \"%s\"", k, val)
		}
	}
	return ci, nil
}
