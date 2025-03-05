package rlog

import (
	"log"
)

var (
	DebugEnabled = true //默认开启 Debug
)

func Debug(format string, v ...any) {
	if DebugEnabled {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func Error(format string, v ...any) {
	log.Printf("[ERROR] "+format, v...)
}
