package main

import (
	"crypto/x509"
	"fmt"
	"time"
)

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

const (
	TLOG   = "LOG"
	TDEBUG = "DEBUG"
)

func logMsg(msg string) {
	if logFlag {
		fmt.Println(msg)
	}
}
func getFileName(fname, prefix string) string {
	if prefix == "" {
		return fmt.Sprintf("%s%s", fname, time.Now().Format(".01-02-2006-15-04-05"))
	}
	return fmt.Sprintf("%s_%s%s", fname, prefix, time.Now().Format(".01-02-2006-15-04-05"))
}

// log debug statements
func logDMsg(msg string, err error) {
	if debug {
		if err == nil {
			fmt.Println(msg)
			return
		}
		fmt.Println(msg, " :", err)
	}
}
