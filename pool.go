package mysql

import (
	//"log"
	"yn.com/ext/common/gomsg"
	"time"
	LOGGER "yn.com/ext/common/logger"
)

var (
	ins *Conn
	host string
	database string
	user string
	password string
)

func Starup(h, d, u, p string) *Conn {
	host = h
	database = d
	user = u
	password = p

	ins = connectMySQL(host, database, user, password, "utf8", 2000, 1000)
	if ins == nil {
		LOGGER.Error("connect mysql failed ...")
		return nil
	}

	go ins.polling()

	return ins
}

func Stack() {
	gomsg.Recover()

	ins.reconnect()
	go ins.polling()
}

func (s *Conn) reconnect() {
	err := ins.close()
	if err != nil {
		LOGGER.Error("close mysql failed ...")
		return
	}
	ins = connectMySQL(host, database, user, password, "utf8", 2000, 1000)
}

func (s *Conn) polling() {
	defer Stack()

	timer := time.NewTicker(300 * time.Second)
	for {
		select {
		case <-timer.C:
			err := ins.ping()
			if err != nil {
				ins.reconnect()
			} 
		}
	}
}