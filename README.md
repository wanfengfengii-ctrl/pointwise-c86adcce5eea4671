# Telemetry Lake

Python 3.11 标准库实现的本地单表遥测湖表。接入完整批次，保留更正和删除的逻辑修订，固定下游加工快照，并支持同桶压实与对象核验。

## 运行

在项目根目录运行：

```sh
python3.11 -B -m unittest discover -s tests -t . -v
python3.11 -B demo.py
```

无需安装依赖。测试和 demo 仅在自己创建的临时目录内建立数据库及对象；demo 在结束时清理自己的目录。

## 公开接口

```python
from lake.admission.api import Store
from lake.admission.batch import put, delete

with Store("lake-data") as lake:
    receipt = lake.ingest("batch-001", [
        put("a", {"v": 1}), put("b", {"v": 2}),
    ])
    fixed = lake.freeze("daily")
    lake.ingest("batch-002", [put("a", {"v": 3}), delete("b")])
    assert lake.get("a") == {"v": 3}
    assert lake.get("b") is None
    assert list(lake.scan(fixed)) == [("a", {"v": 1}), ("b", {"v": 2})]
    lake.compact(0)
    lake.compact(1)
    assert lake.verify(fixed).readable
```

Store(path, *, hook=None, segment_rows=128, index_stride=8) 打开一个目录。close 幂等，支持上下文管理；关闭后的新调用抛 RuntimeError。已经绑定的 scan 迭代器只依赖其固定对象，可在关闭 Store 后消费。

- ingest(request_id, operations) 返回不可变 CommitReceipt，字段为 request_id、commit、manifest_id、operation_count；to_dict 返回独立字典。请求和行 ID 是原样保留的非空 UTF-8 字符串。批次非空，同 ID 不得重复。
- put 完整替换 JSON 对象。对象键必须为字符串，递归值仅接受对象、列表、字符串、有限数字、布尔和 null。delete 产生墓碑，删除缺失 ID 也形成一次提交。
- 整批校验、深复制、按 ID 排序后计算请求指纹。操作顺序及对象键顺序不影响指纹；不声明数字 1 和 1.0 编码相等。同请求同内容返回原回执，同请求异内容抛 ValueError。
- freeze(name) 返回持久快照 ID；读取传入该 ID。名称非空，同名同清单幂等，同名异清单拒绝，即使两清单的可见行相同。
- get(id, snapshot=None) 返回独立载荷字典；缺失或删除返回 None。
- scan(snapshot=None) 在调用时固定清单，返回按 ID 排序的 (id, payload) 迭代器。同 ID 以最大逻辑提交号决胜，不输出墓碑。点读与全表读口径一致。
- compact(bucket) 只接受整数 0 或 1，返回清单 ID；无需合并则返回原清单。保留全部原修订和墓碑，不增加业务提交。准备期间当前头改变时抛 lake.maintenance.publication.StalePlan，调用方可重新规划。
- verify(snapshot=None) 返回 VerificationReport，含 manifest_id、commit、objects、readable 和 to_dict()。逐对象状态含 address、kind、digest、ok、code、reason。数据和索引缺失、摘要、大小及结构错误均使总体不可读。未知快照抛 ValueError。
- 必要对象不可读时，get 或完整消费 scan 抛 lake.objects.integrity.CorruptObject，保留 address、code、reason 及原异常原因。读及核验不把损坏解释成缺行。

## 持久状态与发布

lake/admission 完成验证、复制及幂等协调。正常写入按 CRC32(id 的 UTF-8 字节) 模 2 路由到固定两桶。lake/segments 生成按 (id, commit) 升序排列的 JSONL；墓碑和 put 都携带原业务提交号。稀疏索引独立归档，记录 ID 组首的记录序号和 UTF-8 字节偏移，点读实际消费前驱锚点区间。读取前会完整核验段与索引，不承诺最优 I/O。

lake/objects 将完整暂存文件原子安装到 data/index 内容地址；地址不含业务标识。已有地址仍须核对摘要及大小，坏对象不能覆盖或复用。安装失败可留下未引用完整对象。

lake/catalog 的 SQLite 只保存目录版本、清单和段描述符、当前头、成功提交、请求回执及命名快照，不保存另一份权威行表。候选提交在写锁外生成和安装所有对象；短事务重新核对请求及输入头，并原子发布清单、提交、回执和当前头。头改变时接纳重新准备；失败没有半批可见性。原始准备或安装 I/O 异常传播。

lake/reads 在短读事务中固定清单，之后不再访问可变头。旧快照对象持续保留。lake/maintenance 的同桶压实在写锁外读取、核验及重写，发布时比较完整输入头，另一桶保持原样。对象核验同时检查数据和索引，结构交叉错误定位相关对象且不改目录。

## 回归与阶段回调

五个测试文件覆盖接纳、读取、失败、维护与完整性。tests/support.py 的历史参照按成功业务提交独立重放字典，不调用生产读取算法来生成预期。回归包含两连接交错、重开、完整批次拒绝、规范化重放、输入和输出隔离、多字节索引及修订组边界、物理顺序逆置、安装与元数据失败、既有坏对象拒用、过时压实及摘要匹配的非法结构。

hook(stage, details) 默认不执行动作；用于观测阶段、安排第二连接或传播注入的异常。阶段包括 batch_normalized、before_object_install、object_installed、segment_installed、admission_prepared、before_batch_publish、metadata_after_manifest、scan_bucket、compaction_inputs_read、before_compaction_publish、compaction_metadata_after_manifest。两个 metadata 阶段位于短写事务内，回调不得在这里等待另一个写连接；其余载荷准备、安装及读取阶段均在写事务外。segment_rows 和 index_stride 是正整数，用于控制段大小及索引密度。

项目保留历史对象，不包含历史到期回收、远端对象存储或分布式协调。
