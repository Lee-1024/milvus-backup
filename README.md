# milvus-backup

基于 `github.com/milvus-io/milvus/client/v2/milvusclient` 的 Milvus collection 备份与恢复工具。

## 能力

- 支持备份全部 collection，或通过 `-collections` 指定 collection。
- 备份 collection schema、partition、consistency、shard、properties 和数据。
- 按 partition 导出数据，恢复时会把数据插回原 partition。
- 数据使用 JSONL 存储，`manifest.json` 保存元数据，便于检查、迁移和二次处理。
- 导出使用 `QueryIterator` 分批读取，恢复使用 column-based insert 分批写入。
- 支持常见标量、JSON、Geometry WKT、Float/Binary/Float16/BFloat16/Int8 向量、SparseVector，以及标量数组字段。
- 恢复时使用 JSON number 解码，避免大整数主键或 `int64` 字段经过 `float64` 丢精度。

## 构建

```bash
go build -o bin/milvus-backup.exe ./cmd/milvus-backup
```

## 备份

```bash
./bin/milvus-backup backup \
  -address localhost:19530 \
  -progress-every 10000 \
  -out ./backup
```

只备份指定 collection：

```bash
./bin/milvus-backup backup \
  -address 10.54.56.88:19530 \
  -db test0000 \
  -collections test001 \
  -batch-size 2000 \
  -progress-every 5000 \
  -out D:/milvus-bak \
  -skip-failed
```

## 恢复

```bash
./bin/milvus-backup restore \
  -address localhost:19530 \
  -in ./backup \
  -progress-every 10000 \
  -drop-existing
```

恢复到新 collection 名，避免覆盖原 collection：

```bash
./bin/milvus-backup restore \
  -address localhost:19530 \
  -in ./backup \
  -name-suffix _restored
```

只恢复指定 collection：

```bash
./bin/milvus-backup restore \
  -address 192.168.64.1:19530 \
  -db test000 \
  -collections test001 \
  -in ./backup \
  -drop-existing
```

注意：`-collections` 指的是备份 manifest 里的原 collection 名，不是加后缀后的目标 collection 名。`-db` 指恢复目标数据库，目标数据库需要已经存在。

## 连接参数

命令行参数：

- `-address`: Milvus 地址，默认 `localhost:19530`
- `-username`, `-password`: 用户名和密码
- `-api-key`: API key
- `-db`: database 名称
- `-tls`: 启用 TLS
- `-progress-every`: 每处理 N 行打印一次进度；设为 `0` 可关闭行数进度日志
- `-timeout`: 操作超时时间，例如 `30m`；设为 `0` 不启用超时

环境变量：

- `MILVUS_ADDRESS`
- `MILVUS_USERNAME`
- `MILVUS_PASSWORD`
- `MILVUS_API_KEY`
- `MILVUS_DB`

运行日志会打印实际连接的数据库，例如：

```text
connected to Milvus: address=localhost:19530 database=mydb tls=false
```

如果日志里仍是 `database=default`，通常说明命令没有带到 `-db`，或运行的是旧版本二进制，需要重新构建。

## 目录格式

单分区 collection 的备份目录通常是：

```text
backup/
  manifest.json
  collection_a.jsonl
```

多分区 collection 会为每个 partition 生成独立数据文件：

```text
backup/
  manifest.json
  collection_a.jsonl
  collection_a__partition__partition_1.jsonl
  collection_a__partition__partition_2.jsonl
```

`manifest.json` 保存 collection 结构、partition 列表和数据文件映射；每个 `.jsonl` 文件一行是一条 Milvus row。

## 恢复语义和边界

这个工具面向通用 collection 数据迁移，不直接复制底层 segment/binlog。

恢复时会重建 collection 和 partition，并把每个 partition 的数据写回对应 partition。旧版本备份如果没有 `partition_files` 字段，仍按单数据文件恢复。

恢复时不会恢复以下对象或状态：

- index 构建任务
- load 状态
- alias
- RBAC、用户和角色
- 数据库本身

恢复字段时的特殊行为：

- AutoID 字段不会作为 insert 列写入，由 Milvus 重新生成。
- 函数输出字段不会作为 insert 列写入，由 Milvus 根据 function 重新计算。
- schema 中的动态字段标记会保留，但动态字段数据是否能完整恢复取决于 Milvus `QueryIterator` 返回的字段内容。

恢复后请按业务需要重新创建 index、加载 collection，并重新配置 alias 和权限。
