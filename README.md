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
- begin_rebucket(request_id) -> str：启动一次从 2 桶到 3 桶的在线重分桶，返回持久任务 ID。同请求重复调用返回同一任务且不重置进度；任务已存在而请求不同抛 ValueError。请求命名空间独立于 ingest，空湖也可开始。
- step_rebucket(limit) -> dict：limit 为正整数（排除布尔），返回 {"processed": n, "ready": bool}，0 <= n <= limit。n 是本次成功复制的来源物理记录数，含旧修订与墓碑。每步接续固定的来源序列与游标，不重扫已完成前缀；准备完成后返回 {"processed": 0, "ready": True}。
- finish_rebucket() -> bool：来源未搬完或仍会遗漏已接纳修改时返回 False 并保留任务；安全切换返回 True，重复调用仍为 True。
- rebucket_status() -> dict：独立可 JSON 往返的字典，字段 task_id、phase（idle/copying/ready/finished）、source_buckets（恒 2）、target_buckets（恒 3）、processed、total。total 在开始时固定，ready/finished 时 processed=total。
- 搬迁期间另一连接可正常 ingest；步骤间可关闭重开。搬迁不分配业务提交、不写接纳回执，同 (id, 逻辑提交号) 的相同内容只算一个事实。copying/ready 阶段 compact 只合并目标布局的段，不改动被固定的来源段；finish 后当前读取与写入只依赖新布局，旧快照与已绑定 scan 保持原结果。

## 持久状态与发布

lake/admission 完成验证、复制及幂等协调。正常写入按 CRC32(id 的 UTF-8 字节) 模 2 路由到固定两桶。lake/segments 生成按 (id, commit) 升序排列的 JSONL；墓碑和 put 都携带原业务提交号。稀疏索引独立归档，记录 ID 组首的记录序号和 UTF-8 字节偏移，点读实际消费前驱锚点区间。读取前会完整核验段与索引，不承诺最优 I/O。

lake/objects 将完整暂存文件原子安装到 data/index 内容地址；地址不含业务标识。已有地址仍须核对摘要及大小，坏对象不能覆盖或复用。安装失败可留下未引用完整对象。

lake/catalog 的 SQLite 只保存目录版本、清单和段描述符、当前头、成功提交、请求回执及命名快照，不保存另一份权威行表。候选提交在写锁外生成和安装所有对象；短事务重新核对请求及输入头，并原子发布清单、提交、回执和当前头。头改变时接纳重新准备；失败没有半批可见性。原始准备或安装 I/O 异常传播。

lake/reads 在短读事务中固定清单，之后不再访问可变头。旧快照对象持续保留。lake/maintenance 的同桶压实在写锁外读取、核验及重写，发布时比较完整输入头，另一桶保持原样。对象核验同时检查数据和索引，结构交叉错误定位相关对象且不改目录。

## 在线重分桶

`lake/rebucket` 实现一次 2 桶到 3 桶的在线搬迁，契约见 REBUCKET.md。每个段描述符记录其布局桶数，清单记录所属布局；当前头的读绑定会把主布局段与搬迁期间仍在头中的来源段一起合并，同 `(id, 逻辑提交号)` 的相同内容只计一次。`layout_tasks` 固定来源段序列、来源清单和总记录数，`layout_state` 记录活动桶数、阶段、已处理数与来源游标；两者都是目录元数据，不保存另一份权威行表。

- begin 在短写事务中固定来源段并写入任务，随后把路由切到 3 桶并保留来源段直到 finish。切换后新入仓写目标段；来源段仍属旧布局，只作为复制输入与旧快照依据。begin 之前已准备、begin 之后才发布的入仓会因输入头变化重新准备，按目标布局落段。
- step 在读事务外按游标读出本轮来源记录、生成并安装目标段，再在短写事务中核对游标与阶段，原子追加目标段、推进 processed 与游标。复制异常不推进进度，重试重复同一区间且不增加业务提交。发布前发出 `rebucket_prepared`（含 task_id、start、stop，位于写事务外）。
- finish 仅在 processed=total、当前头中的旧布局段恰为固定来源段、且全部目标段对象可读时，把当前头收缩为纯目标布局段并标记 finished；任何一项不满足都返回 False 或抛错并保留可续任务。切换后当前读写只依赖新布局；旧对象不删除，旧固定快照与已绑定 scan 仍按其清单读旧对象。

逻辑身份始终是 (id, 原业务提交号)；布局归属写在段描述符与清单上；续点由任务固定的来源序列加游标给出；退出条件是来源有限记录全部复制且点击发后无遗漏。四者共同保证搬迁期间更正、删除与并发入仓都不会被覆盖或复活。

## 回归与阶段回调

原五个测试文件覆盖接纳、读取、失败、维护与完整性，tests/test_rebucket.py 覆盖在线重分桶。tests/support.py 的历史参照按成功业务提交独立重放字典，不调用生产读取算法来生成预期；重分桶测试与 reconcile_rebucket.py 也用同一原则核对 get、scan、固定快照与业务提交号。回归包含两连接交错、重开、完整批次拒绝、规范化重放、输入和输出隔离、多字节索引及修订组边界、物理顺序逆置、安装与元数据失败、既有坏对象拒用、过时压实及摘要匹配的非法结构，以及来源读出后更正或删除、最后一步前与 ready 后新写、入仓准备跨 begin/finish、步骤异常重试、逐步重开、空湖、无效参数和旧 V1 目录直接打开。

hook(stage, details) 默认不执行动作；用于观测阶段、安排第二连接或传播注入的异常。阶段包括 batch_normalized、before_object_install、object_installed、segment_installed、admission_prepared、before_batch_publish、metadata_after_manifest、scan_bucket、compaction_inputs_read、before_compaction_publish、compaction_metadata_after_manifest、rebucket_prepared。两个 metadata 阶段位于短写事务内，回调不得在这里等待另一个写连接；其余载荷准备、安装及读取阶段均在写事务外。segment_rows 和 index_stride 是正整数，用于控制段大小及索引密度。

验证入口：

```sh
python3.11 -B verify.py
```

它依次运行原完整回归、reconcile_rebucket.py 的独立历史核账、REBUCKET 公开样例与两个 demo，任一失败非零退出。

项目保留历史对象，不包含历史到期回收、远端对象存储或分布式协调，也不做第二次调桶、任务取消、旧对象回收或杀进程恢复。
