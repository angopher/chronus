module github.com/angopher/chronus

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/dgraph-io/dgraph v1.2.7
	github.com/fatih/color v1.7.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/go-cmp v0.4.0
	github.com/influxdata/influxdb v1.8.3
	github.com/influxdata/influxql v1.1.1-0.20200828144457-65d3ef77d385
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/klauspost/compress v1.4.1 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/pgzip v1.2.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1 // test
	github.com/urfave/cli/v2 v2.2.0
	github.com/willf/bitset v1.1.9 // indirect
	github.com/xlab/treeprint v0.0.0-20181112141820-a009c3971eca // indirect
	go.etcd.io/etcd v3.3.25+incompatible
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace github.com/coreos/etcd v3.3.25+incompatible => go.etcd.io/etcd v3.3.25+incompatible

go 1.13
