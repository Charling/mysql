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
	maxOpenNums int
	maxIdelNums int
)

func Stratup(h, d, u, p string,openNums,idleNums int) *Conn {
	host = h
	database = d
	user = u
	password = p
	maxOpenNums = openNums
	maxIdelNums = idleNums

	ins = connectMySQL(host, database, user, password, "utf8", maxOpenNums, maxIdelNums)
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
	ins = connectMySQL(host, database, user, password, "utf8", maxOpenNums, maxIdelNums)
}

//"database/sql"本身就是实现一个连接池，此处更多就是预防意外情况下做重连操作
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