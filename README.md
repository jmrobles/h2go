# Apache H2 Database Go Driver

__This driver is VERY experimental state__ 

__NOT use for production yet__

## Introduction

[Apache H2 Database](https://h2database.com) is a very-low footprint database with in-memory capabilities.

It's written in Java and it's fully ACID compliant.

You can use H2 as embedded database or via TCP/IP.

It has interfaces for Postgres protocol and native TCP server.

## Motivation

Until now, using H2 in your Go projects could only be done through the Postgres driver.

This approach has several cons. The poor error messagens or not being able to use native data types are some of them.

This pure Go driver uses the native TCP interface.

## Pre-requesites

In "contrib" folder you can find the scripts to download and launch the H2 database server.
You need to have any Java Runtime installed.

```bash
cd contrib
./downloadH2.sh
./runStandalone.sh
```

## Usage

First make sure the H2 server is running in TCP server mode. You can launch using the `runStandalone.sh` or with a command similar to the following:

```bash
ava -classpath h2.jar org.h2.tools.Server -tcp -tcpAllowOthers -ifNotExists
```

This starts the server at the defaulr port (9092)

The following example connect to H2 and creates an in-memory database. 

```go
package main

import (
	"database/sql"
	"log"
	_ "github.com/jmrobles/h2go"
)

func main() {
	conn, err := sql.Open("h2", "h2://sa@localhost/testdb?mem=true")
	if err != nil {
		log.Fatalf("Can't connet to H2 Database: %s", err)
	}
    err = conn.Ping()
    if err != nil {
        log.Fatalf("Can't ping to H2 Database: %s", err)
    }
    log.Printf("H2 Database connected")
    conn.Close()
}
```

In the folder `examples` you can find more examples.
## Connection string

In the connection string you must specify:

- Database driver: `h2` literal
- Username (optional)
- Password (optinal)
- Host: format <host>(:<port>)?
- Database name
- Other connection options

### Options

You can use the following options:

- mem=(true|false): to use in-memory or in-disk database
- logging=(none|info|debug|error|warn|panic|trace): the common logging level

## Parameters

For the use of parameters in SQL statement you need to use the `?` placeholder symbol.

For example:
```go 
    conn.Exec("INSERT INTO employees VALUES (?,?,?)", name, age, salary)
```

## Data types

The following H2 datatypes are implemented:

| H2 Data type | Go mapping |
|--------------|------------|
| String       | string     |
| StringIgnoreCase | string |
| StringFixed | string |
| Bool | bool |
| Short | int16 |
| Int | int32 |
| Long | int64 |
| Float | float32 |
| Double | float64 |
| Byte | byte |
| Bytes | []byte |
| Time | time.Time |
| Time with timezone | time.Time |
| Date | time.Time
| Timestamp | time.Time |
| Timestamp with timezone | time.Time

## H2 Supported version

This driver supports H2 database version 1.4.200 or above.

## ToDo

- Rest of native data types (UUID, JSON, Decimal, ...)
- `NamedValue` interface
- Multiple result sets
- Improve `context` usage (timeouts, ...)
- Submit your issue

## Contributors

[jmrobles](https://jmrobles.medium.com)

Pull Requests are welcome

## License

MIT License
