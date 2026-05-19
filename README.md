# milvus-backup

基于 `github.com/milvus-io/milvus/client/v2/milvusclient` 的通用 Milvus 集合备份/恢复工具。

## 能力

- 支持备份全部集合，或通过 `-collections` 指定集合。
- 备份 collection schema、partition、consistency、shard、properties 和数据。
- 数据使用 JSONL 存储，`manifest.json` 保存元数据，便于检查、迁移和二次处理。
- 导出使用 `QueryIterator` 分批读取，恢复使用 column-based insert 分批写入，适合大集合。
- 支持常见标量、JSON、Geometry WKT、Float/Binary/Float16/BFloat16/Int8 向量，以及标量数组字段。

## 构建

```bash
go build -o bin/milvus-backup ./cmd/milvus-backup
```

## 备份

```bash
./bin/milvus-backup backup \
  -address localhost:19530 \
  -progress-every 10000 \
  -out ./backup
```

只备份指定集合：

```bash
./bin/milvus-backup backup \
  -address localhost:19530 \
  -collections users,items \
  -batch-size 2000 \
  -progress-every 5000 \
  -out ./backup
```

## 恢复

```bash
./bin/milvus-backup restore \
  -address localhost:19530 \
  -in ./backup \
  -progress-every 10000 \
  -drop-existing
```

恢复到新集合名，避免覆盖：

```bash
./bin/milvus-backup restore \
  -address localhost:19530 \
  -in ./backup \
  -name-suffix _restored
```

## 连接参数

命令行参数：

- `-address`: Milvus 地址，默认 `localhost:19530`
- `-username`, `-password`: 用户名和密码
- `-api-key`: API key
- `-db`: database 名称
- `-tls`: 启用 TLS
- `-progress-every`: 每处理 N 行打印一次进度；设为 `0` 可关闭行数进度日志

也可以使用环境变量：

- `MILVUS_ADDRESS`
- `MILVUS_USERNAME`
- `MILVUS_PASSWORD`
- `MILVUS_API_KEY`
- `MILVUS_DB`

运行日志会打印实际连接的数据库，例如 `connected to Milvus: address=localhost:19530 database=mydb tls=false`。如果日志里仍是 `database=default`，通常说明命令没有带到 `-db`，或运行的是旧版本二进制，需要重新构建。

指定 `-db` 时，工具会先校验数据库是否存在；指定 `-collections` 时，如果集合不存在，会打印当前数据库下可用集合，便于排查库名或集合名写错。

## 目录格式

```text
backup/
  manifest.json
  collection_a.jsonl
  collection_b.jsonl
```

`manifest.json` 保存集合结构和数据文件映射；每个 `.jsonl` 文件一行一条 Milvus row。

## 当前边界

这是一个面向通用 collection 数据迁移的工具，不直接复制底层 segment/binlog，也不恢复 index 构建任务、load 状态、alias、RBAC。恢复后可按业务需要重新创建索引和加载集合。
