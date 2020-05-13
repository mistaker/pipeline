package pipeline

import (
	"log"
	"sync"
)

const (
	defaultThreadNum = 16
)

type (
	generateFunc   func(source writer)
	reduceFunc     func(item interface{}, data chan<- interface{})
	reduceVoidFunc func(item interface{})
	receiveFunc    func(data <-chan interface{})
	holdPlace      struct{}

	writer interface {
		write(interface{})
	}
)

func SafeGo(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println(err)
			}
		}()
		fn()
	}()
}

func PipeLine(generate generateFunc, reduce reduceFunc, receive receiveFunc) {
	var (
		source     = make(chan interface{})
		data       = make(chan interface{})
		done       = make(chan holdPlace)
		threadNum  = make(chan holdPlace, defaultThreadNum)
		waitReduce sync.WaitGroup
	)

	defer func() {
		waitReduce.Wait()
		close(data)
		<-done
	}()

	nGenerate := func() {
		defer func() {
			close(source)
		}()
		generate(newGuardedWriter(source))
	}

	nReceive := func() {
		defer func() {
			done <- holdPlace{}
		}()
		receive(data)
	}

	SafeGo(nGenerate)
	SafeGo(nReceive)

	for {
		select {
		case item, flag := <-source:
			if !flag {
				return
			}
			select {
			case threadNum <- holdPlace{}:
				waitReduce.Add(1)
				SafeGo(func() {
					defer func() {
						waitReduce.Done()
						<-threadNum
					}()
					reduce(item, data)
				})
			}
		}
	}
}

func Pipe(generate generateFunc, reduceVoid reduceVoidFunc) {
	var (
		source    = make(chan interface{})
		threadNum = make(chan holdPlace, defaultThreadNum)
		wg        sync.WaitGroup
	)

	defer func() {
		wg.Wait()
	}()

	nGenerate := func() {
		defer func() {
			close(source)
		}()
		generate(newGuardedWriter(source))
	}

	SafeGo(nGenerate)

	for {
		select {
		case item, flag := <-source:
			if flag == false {
				return
			}
			select {
			case threadNum <- holdPlace{}:
				wg.Add(1)
				SafeGo(func() {
					defer func() {
						wg.Done()
						<-threadNum
					}()
					reduceVoid(item)
				})
			}
		}
	}
}

func PipeFinish(fnList ...func()) {
	Pipe(func(write writer) {
		for _, fn := range fnList {
			write.write(fn)
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	})
}

type guardedWriter struct {
	channel chan<- interface{}
}

func newGuardedWriter(channel chan<- interface{}) *guardedWriter {
	return &guardedWriter{
		channel: channel,
	}
}

func (gw *guardedWriter) write(data interface{}) {
	gw.channel <- data
}
