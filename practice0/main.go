package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	b := make([]byte, 100)

	// Первая горутина
	go func() {
		for {
			filler(b[:50], '0', '1')
			time.Sleep(time.Second)
		}
	}()

	// Вторая горутина
	go func() {
		for {
			filler(b[50:], 'X', 'Y')
			time.Sleep(time.Second)
		}
	}()

	// Третья горутина
	go func() {
		for {
			fmt.Println(string(b))
			time.Sleep(time.Second)
		}
	}()

	// Основной цикл
	for {
	}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	rand.Seed(time.Now().UnixNano())
	for i := range b {
		r := rand.Intn(2)
		if r == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}
