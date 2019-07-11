.PHONY: all

all: gobi

gobi: main.go
	go build -o gobi main.go

clean: gobi
	rm -f gobi
