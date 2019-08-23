package mysql

import (
	//"log"
	"database/sql" 
	LOGGER "yn.com/ext/common/logger"
	"time"
)

type dbIndex struct {
	index int
	host string
	database string
	user string
	password string
	maxOpenNums int
	maxIdelNums int
}
var (
	mapConns map[dbIndex]*Conn
)

func Startup() {
	mapConns = make(map[dbIndex]*Conn)
}

func Connect(index int,h, d, u, p string,openNums,idleNums int) *Conn {
	var idx dbIndex
	idx.index = index
	idx.host = h
	idx.database = d
	idx.user = u
	idx.password = p
	idx.maxOpenNums = openNums
	idx.maxIdelNums = idleNums

	conn := connectMySQL(idx.host, idx.database, idx.user, idx.password, "utf8", idx.maxOpenNums, idx.maxIdelNums)
	if conn == nil {
		LOGGER.Error("connect mysql failed ...")
		return nil
	}

	mapConns[idx] = conn
	return conn
}

func StartWork() {
	go func() {
		timer := time.NewTicker(300 * time.Second)
		for {
			select {
			case <-timer.C:
				for idx,conn := range mapConns {
					//conn.SQLDB本身就是连接池，此处只是定期做检测处理
					err := conn.ping()
					if err != nil {
						conn.close()
						Connect(idx.index,idx.host,idx.database,idx.user,idx.password,idx.maxOpenNums,idx.maxIdelNums)
						break
					}
				}
			}
		}
	}()
}

func GetMysqlDBConn(index int) *sql.DB {
	for idx,_ := range mapConns {
		if idx.index == index {
			return mapConns[idx].SQLDB
		}
	}

	return nil
}