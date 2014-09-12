run:
	GOPATH=/root/go HOST_NUM=11 GITW_PORT=80 INFO_LOG=off DYNAMO_TABLE=gitw_prod_contest /usr/local/go/bin/go run main.go

.PHONY: all run