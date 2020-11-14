/*
Copyright 2020 JM Robles (@jmrobles)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package h2go

import (
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

type h2client struct {
	conn  net.Conn
	trans transfer
	sess  session
}

func (c *h2client) doHandshake(ci h2connInfo) error {
	var err error
	// 1. send min client version
	err = c.trans.writeInt32(9)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send min client version")
	}
	// 2. send max client version
	err = c.trans.writeInt32(19)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send max client version")
	}
	// 3. Send db name
	err = c.trans.writeString(ci.database)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send database name")
	}
	// 4. Send original url
	err = c.trans.writeString("jdbc:h2:" + ci.database)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send original url")
	}
	// 5. Send username
	err = c.trans.writeString(ci.username)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send username")
	}
	// 6. Send password
	hashedPassword, err := getHashedPassword(ci.username, ci.password)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't hash password")
	}
	err = c.trans.writeBytes(hashedPassword[:])
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't hashed password")
	}
	// 7. Send file password hash
	err = c.trans.writeBytes(nil)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send hashed file password")
	}
	// 8. Send aditional properties
	// TODO: bynow, 0 properties tos send
	err = c.trans.writeInt32(0)
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't send properties")
	}
	err = c.trans.flush()
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't flush data to socket")
	}
	// 9. Wait for Status OK ack
	code, err := c.trans.readInt32()
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't get H2 Server status code")
	}
	// 10. Read client version
	clientVer, err := c.trans.readInt32()
	if err != nil {
		return errors.Wrapf(err, "H2 handshake: can't get H2 Server client version ack")
	}
	L(log.InfoLevel, "H2 server code: %d - client ver: %d", code, clientVer)
	return nil
}

func (c *h2client) close() error {
	err := c.sess.close(&c.trans)
	if err != nil {
		return err
	}
	// Close client
	return c.conn.Close()
}
