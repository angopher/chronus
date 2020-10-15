module github.com/angopher/chronus

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/dgraph-io/dgraph v1.2.7
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.9.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.2
	github.com/google/go-cmp v0.5.2
	github.com/influxdata/influxdb v1.8.3
	github.com/influxdata/influxql v1.1.1-0.20200828144457-65d3ef77d385
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/klauspost/compress v1.11.1 // indirect
	github.com/klauspost/pgzip v1.2.5 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1 // test
	github.com/urfave/cli/v2 v2.2.0
	github.com/willf/bitset v1.1.11 // indirect
	github.com/xlab/treeprint v1.0.0 // indirect
	go.etcd.io/etcd v0.0.0-20200824191128-ae9734ed278b // actual it's v3.4.13. we need it because of nasty dependency issue.
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201002170205-7f63de1d35b0
	golang.org/x/net v0.0.0-20201010224723-4f7140c49acb
	golang.org/x/text v0.3.3
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.14
