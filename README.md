# DICOM 研究数据匿名化 API

基于 **Python + FastAPI + SQLite + pydicom + Pillow** 的 DICOM 研究数据匿名化服务。
客户端提交含多序列的 DICOM ZIP、版本化匿名化策略和可选像素遮挡框，服务输出
**确定性、可复验**的匿名化结果。

## 特性

- **确定性 UID 映射**：按 Study/Series/Instance 关系，用 HMAC-SHA256 将原始 UID
  映射为 `2.25.<decimal>` 形式的新 UID；同一 UID 在所有实例及引用处
  （`ReferencedSOPInstanceUID` 等）一致替换，保持引用完整性。
- **PHI 清除（覆盖所有 SQ 层级）**：内置类 DICOM PS3.15 基本档案（姓名、出生日期、
  机构、医师、入组号等置空，`PatientID` 哈希化），默认删除全部私有标签；
  默认动作与策略中的 `empty`/`remove`/`hash`/`replace` 动作按同一策略递归作用于
  **顶层数据集及保留下来的每一层序列项（含嵌套 SQ）**。策略整条移除某个序列时，
  其子项不再被处理或计数；`empty` 一个序列则保留空序列壳。策略可增删改。
- **日期重写且保持时间差**：按患者派生确定性偏移（天数 + 秒级抖动），
  所有 DA/TM/DT 标签同步平移，跨实例时间差逐秒保持。
- **像素遮挡**：按遮挡框（instance/series/study 三级作用域）真正覆写像素
  （Pillow），支持 8/16 位灰度与 8 位交错 RGB，覆写后统一以
  Explicit VR Little Endian 重新编码。
- **字节级确定性**：相同输入 + 相同策略 ⇒ 输出 ZIP 字节完全一致
  （固定 ZIP 时间戳、确定性文件命名、确定性 DICOM 编码、HMAC 派生映射）。
- **全有或全无**：缺失引用、非法 VR、压缩像素、策略冲突等逐项报告，
  任一错误即整体失败，绝不产出半成品。
- **审计与可复验哈希**：结果附审计摘要（统计、UID 映射、警告）及
  输出 ZIP 与逐文件 SHA-256；ZIP 内含 `manifest.json` 供离线复验。
- **幂等提交**：相同输入 + 策略 + 遮挡规则在**同一 `ANON_SECRET` 作用域下**
  的重复提交返回同一作业（HTTP 200）；轮换密钥后重提会创建独立作业，
  绝不会把旧密钥的结果当作新密钥的结果返回。
- **中断恢复（崩溃安全）**：输入 ZIP 落盘后才登记作业；执行者持有数据库**租约**
  并按期续约。进程崩溃后，无论重启容器还是原样重提，遗留作业都会以**原
  `job_id`** 被接管重跑（跨进程原子裁决，活跃执行者不会被抢占）；结果 ZIP 先写
  临时文件再原子更名、落盘后才标记 `done`，绝不暴露半成品或误标完成。
- **密钥作用域（轮换安全）**：判重 ID 与持久化判重条件都带密钥**指纹**
  （SHA-256 截断，非可逆，不保存明文密钥）。旧密钥作业仍可用原 ID 查询下载，
  新密钥产出独立作业 ID、审计与哈希；升级前的无指纹旧记录自动迁移且永不被复用。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务监听 <http://localhost:8000>：

- 交互式 API 文档：<http://localhost:8000/docs>
- 健康检查：`GET /healthz`
- 策略说明：`GET /policies`

作业数据库、持久化输入与结果 ZIP 保存在命名卷 `dicom-anon-data`（容器内
`/data`），重启容器不会丢失历史作业；**崩溃时未完成的作业会在重启后自动以原
`job_id` 恢复执行**（见下文[「中断恢复」](#中断恢复崩溃安全)）。

> 生产部署请设置稳定的 `ANON_SECRET`（环境变量或 `.env` 文件），
> 它是 UID 映射 / 日期偏移 / 哈希的 HMAC 根密钥；变更后映射结果会改变。
> 密钥如何参与判重、轮换后会发生什么，见下文
> [「密钥轮换与判重作用域」](#密钥轮换与判重作用域)。

## 本地开发

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
pytest tests/ -q          # 52 项测试
```

## 示例请求

### 1. 生成示例数据（1 个检查 × 2 个序列 × 2 个实例，含模拟 PHI）

```bash
python scripts/make_sample.py          # 生成字节确定的 sample_input.zip
```

每个实例除顶层 PHI 外还携带：

- 一棵**保留的多层序列树**
  `VerifyingObserverSequence → OriginalAttributesSequence → OriginalAttributesSequence`，
  各层都含 `PatientName`/`InstitutionName` 等 PHI、一个私有标签，叶节点含
  `ReferencedSOPSequence`（指向包内其他实例）；
- 一棵 `RequestAttributesSequence`，其子项同样含多层 PHI——示例策略会把它整条移除。

ZIP 使用固定条目顺序与 1980-01-01 时间戳写入，重新生成逐字节一致。

### 2. 提交匿名化作业

```bash
curl -s -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
```

响应（HTTP 202；同一密钥下重复提交返回 200 与同一 `job_id`）：

```json
{ "job_id": "218b2ae74856eadbd34f8504f8e4521f", "status": "queued",
  "status_url": "/jobs/218b2ae74856eadbd34f8504f8e4521f", "detail": null }
```

> 下列示例数值用 `docker compose` 的默认 `ANON_SECRET=change-me-in-production`
> 实跑采集；设置了其他密钥时 `job_id`、映射 UID 与哈希会不同。重新生成的
> `sample_input.zip` 与示例策略字节固定，所以同一密钥下结果可逐字节复验。

### 3. 查询状态与审计摘要

```bash
curl -s http://localhost:8000/jobs/<job_id>
```

成功时（节选，来自对 `sample_input.zip` + `examples/policy.json` 的真实调用）：

```json
{
  "job_id": "218b2ae74856eadbd34f8504f8e4521f",
  "status": "done",
  "policy_version": "1.0",
  "policy_sha256": "1c0d2dd6424dc99a890d877fd54488fca69367c8eea49437ed191c79b37d8175",
  "input_sha256": "2bcc31fa707c4b9c50a71781cde5a2b24abe9dd20ded1eb0b651bf222760739e",
  "secret_fingerprint": "037e11597df47b747293d41a7942a87a",
  "audit": {
    "secret_fingerprint": "037e11597df47b747293d41a7942a87a",
    "summary": { "studies": 1, "series": 2, "instances": 4,
                 "uids_mapped": 7, "tags_removed": 4, "tags_emptied": 52,
                 "tags_replaced": 8, "tags_hashed": 4,
                 "private_tags_removed": 12, "date_tags_shifted": 16,
                 "redaction_boxes_applied": 2 },
    "uid_mapping": {
      "1.2.826.0.1.3680043.9.7433.1.1":     "2.25.303989340376998048752381211511128573336",
      "1.2.826.0.1.3680043.9.7433.1.1.1":   "2.25.93670731803630432347505141810977829969"
    },
    "output": { "zip_sha256": "be0c34e6278075de3a1afd598e16b0696d0e0076d885ae20bd79b3db55781707",
                "files": [
      { "path": "STUDY_0001/SERIES_0001/INSTANCE_00001.dcm",
        "sha256": "02d822b43a7cb63f0e04eb80e0564eeb8a855220fa8f698e56de4cd7811efa97",
        "study_instance_uid":  "2.25.303989340376998048752381211511128573336",
        "series_instance_uid": "2.25.93670731803630432347505141810977829969",
        "sop_instance_uid":    "2.25.145337988595756024462092823072960691239" } ] }
  },
  "result": { "sha256": "be0c34e6278075de3a1afd598e16b0696d0e0076d885ae20bd79b3db55781707",
              "size_bytes": 16029,
              "download_url": "/jobs/218b2ae74856eadbd34f8504f8e4521f/download" }
}
```

`secret_fingerprint` 是当前 `ANON_SECRET` 的非可逆指纹（SHA-256 前 16 字节
的十六进制），同时出现在顶层状态、`audit` 以及结果 ZIP 的 `manifest.json` 中；
接口与数据库都不会保存或回显明文密钥。升级前写入的旧记录该字段为 `null`。

上述计数体现递归处理：`tags_removed=4` 即 4 个实例各移除 1 个
`RequestAttributesSequence`，其子树（`PatientName`、私有标签、额外 UID 等）
不再处理也不计入 `tags_emptied`/`private_tags_removed`/`uids_mapped`；
保留的 `VerifyingObserverSequence` 三层项内 PHI 均被清空、叶节点引用 UID
被一致重映射，序列层级结构原样保留。

失败时（逐项错误，无半成品；非法 VR / 非法值即使位于 SQ 项内也按标签逐项报告）：

```json
{ "job_id": "89468715c44f29e4e5cf1733d29f3782", "status": "failed",
  "secret_fingerprint": "037e11597df47b747293d41a7942a87a",
  "errors": [
    { "file": "compressed.dcm", "code": "COMPRESSED_PIXELS",
      "message": "compressed transfer syntax 1.2.840.10008.1.2.4.50 is not supported; …",
      "tag": null } ],
  "audit": null, "result": null }
```

### 4. 下载并复验

```bash
curl -s -OJ http://localhost:8000/jobs/218b2ae74856eadbd34f8504f8e4521f/download
sha256sum anonymized-218b2ae74856eadbd34f8504f8e4521f.zip
# be0c34e6…81707  anonymized-…zip  —— 与 result.sha256 / audit.output.zip_sha256 一致
unzip -p anonymized-218b2ae74856eadbd34f8504f8e4521f.zip manifest.json   # 含密钥指纹与逐文件 SHA-256 / 映射后 UID
```

可用 pydicom 直接确认多层 SQ 的清洗结果：

```python
from pydicom import dcmread
import io, zipfile
zf = zipfile.ZipFile("anonymized-218b2ae74856eadbd34f8504f8e4521f.zip")
ds = dcmread(io.BytesIO(zf.read("STUDY_0001/SERIES_0001/INSTANCE_00001.dcm")))
assert "RequestAttributesSequence" not in ds                 # 整条移除
leaf = (ds.VerifyingObserverSequence[0]
          .OriginalAttributesSequence[0]
          .OriginalAttributesSequence[0])
assert leaf.PatientName == "" and leaf.InstitutionName == "" # 深层 PHI 已清空
assert str(leaf.ReferencedSOPSequence[0].ReferencedSOPInstanceUID).startswith("2.25.")
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/jobs` | 提交作业（multipart：`file` ZIP、`policy` JSON 字符串、`redactions` 可选 JSON 数组）。幂等：相同输入在同一 `ANON_SECRET` 作用域内返回既有作业；若既有作业因进程中断停在 `queued`/`processing` 且无活跃执行者，本次提交会以原 ID 接管恢复 |
| `GET` | `/jobs/{id}` | 查询状态 / 逐项错误 / 审计摘要 / 结果哈希（跨密钥轮换，旧 ID 仍可查） |
| `GET` | `/jobs/{id}/download` | 下载匿名化结果 ZIP（未完成返回 409；结果文件已被删除返回 410） |
| `GET` | `/policies` | 支持的策略版本、默认档案、遮挡框格式 |
| `GET` | `/healthz` | 健康检查 |

## 密钥轮换与判重作用域

匿名化结果由四者唯一决定：输入 ZIP、策略、遮挡规则、`ANON_SECRET`。
因此判重必须与引擎使用的密钥一致，否则轮换密钥后原样重提会误命中旧记录、
取回由旧密钥生成的结果。本服务把**密钥指纹**同时放进：

1. **作业 ID**：`job_id = sha256("job-scope-v1" | fingerprint | input_sha256
   | canonical(policy) | canonical(redactions))[:32]`；
2. **SQLite 持久化判重条件**：`jobs.secret_fp` 列，命中要求指纹严格相等。

指纹为 `sha256("dicom-anon-secret-fingerprint-v1\n" + secret)` 的前 16 字节
（十六进制），非可逆；任何地方都不保存、不记录、不回显明文密钥。指纹也写入
状态响应的顶层 `secret_fingerprint`、`audit` 与结果 ZIP 内的 `manifest.json`，
便于离线核对某个结果是在哪个密钥作用域下产出的。

由此得到的语义：

- **同密钥重复提交** → 同一 `job_id`，返回 **HTTP 200** 复用既有作业；
- **轮换密钥后原样重提** → 新 `job_id`，返回 **HTTP 202**，产出独立的
  审计、UID 映射和 ZIP 哈希，绝不会把旧密钥结果当成新密钥结果；
- **旧 ID 仍可查询下载**：作业以 ID 为主键保存，轮换不删除历史；旧作业的
  `secret_fingerprint` 保持为它产出时的指纹；
- **升级前的旧记录**：数据库启动时自动 `ALTER TABLE` 增加 `secret_fp` 列，
  旧记录该列为 `NULL`、状态中 `secret_fingerprint` 为 `null`，仍可用旧 ID
  查询；但任何带指纹的判重都不会匹配 `NULL` 行，因此旧结果永远不会被
  新提交复用。

### 实操：Docker Compose 下验证一次轮换

先用默认密钥（A）提交：

```bash
docker compose up --build -d
python scripts/make_sample.py
curl -s -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
# {"job_id":"218b2ae74856eadbd34f8504f8e4521f","status":"queued", ...}
```

同一密钥再提一次，复用（200，同一 ID）：

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
# 200
```

把密钥改为 B 并重启（数据卷保持不变）：

```bash
ANON_SECRET=rotated-secret-B docker compose up -d --force-recreate
```

原样重提同一份输入 → 新作业（202，不同 ID）：

```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
# {"job_id":"ac0189360c414bf389d2849c49a28460","status":"queued", ...}
# HTTP 202
```

两个作业并存，各自独立的指纹与哈希：

| | 密钥 A（默认） | 密钥 B（`rotated-secret-B`） |
| --- | --- | --- |
| `job_id` | `218b2ae74856eadbd34f8504f8e4521f` | `ac0189360c414bf389d2849c49a28460` |
| `secret_fingerprint` | `037e11597df47b747293d41a7942a87a` | `de5b87d1c94eb992c156ef6f94a1cb1f` |
| `result.sha256` | `be0c34e6…b55781707` | `8b6ae2cd…165e297` |

旧作业（A）在轮换后依旧可查可取，且下载到的仍是 A 的字节：

```bash
curl -s http://localhost:8000/jobs/218b2ae74856eadbd34f8504f8e4521f/download \
  | sha256sum
# be0c34e6278075de3a1afd598e16b0696d0e0076d885ae20bd79b3db55781707  -
```

## 中断恢复（崩溃安全）

旧行为：作业登记为 `queued` 后，输入 ZIP 只存在于内存中的后台任务里；
进程在任务完成前退出，作业便永远停在 `queued`/`processing`——原样重提只会
判重返回 200，却没有任何执行者再跑它。

现在的机制（全部基于 `DATA_DIR` 内的持久化状态，跨进程安全）：

1. **输入落盘**：登记作业前，上传的 ZIP 先原子写入
   `DATA_DIR/inputs/{job_id}.zip`（临时文件 + `os.replace`），崩溃后输入仍可恢复。
2. **执行租约**：执行者持有作业行的租约（`lease_owner` = 主机/PID/随机串，
   `lease_expires_at` 至多 `LEASE_TTL_SECONDS` 秒，默认 30），运行期间按
   TTL/3 间隔续约。**租约是否过期/停滞即"旧任务是否仍在执行"的判据。**
3. **原子裁决接管**：接管是单条 `UPDATE ... WHERE 租约已过期`（或启动时
   `WHERE 租约等于启动快照` 的 CAS），SQLite 写串行化保证并发下只有一个
   接管者成功；活跃执行者的租约被续约推进，永远不会被抢占。被取代的执行者
   续约/状态推进会因租约不匹配而失效，不会改写记录。
4. **与记录一致的输入**：接管者执行时，策略与遮挡规则取自数据库记录，
   ZIP 字节（重提请求体或持久化输入）必须哈希等于记录的 `input_sha256`，
   否则作业以 `INPUT_UNAVAILABLE` 失败（不会拿错输入产出结果）。
5. **原子结果发布**：结果先写临时文件、`fsync`、`os.replace` 到
   `results/{job_id}.zip`，**落盘完成后**才置 `done`；崩溃只会留下下次启动时
   被清理的临时文件，下载接口永远拿不到半成品，作业也不会被误标 `done`。

触发恢复的两条路径：

- **重提恢复**：`POST /jobs` 判重命中且作业未完成时，当前进程尝试接管；
  成功则以原 `job_id` 重新执行（响应仍为 HTTP 200 复用语义）。
- **启动恢复**：服务启动时接管租约已过期的作业；对"快速重启"场景
  （旧租约尚未过期但永不再续约），启动 1.5×TTL 后按启动快照 CAS 接管。
  因此 `docker compose up -d`（或 `restart`）后无需任何操作即可恢复。
  启动恢复只接管**当前密钥指纹作用域**内的作业，轮换出去的旧密钥作业
  不会被新密钥误跑。

### 实操：Docker Compose 下复现一次中断恢复

```bash
docker compose up --build -d
python scripts/make_sample.py
curl -s -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
# {"job_id":"218b2ae74856eadbd34f8504f8e4521f","status":"queued", ...}

# 在后台任务完成前模拟崩溃（真实部署中的容器宕机/被kill等价于此）
docker compose kill -s SIGKILL api
```

此时作业停在 `queued`（或 `processing`），租约属于已死进程。重启：

```bash
docker compose up -d        # 同一数据卷、同一 ANON_SECRET
```

启动恢复会在租约过期后（默认最多约 1.5×TTL）自动以原 ID 重跑；也可以不等，
在租约过期后**原样重提**同一份输入，立即触发接管（HTTP 200，同一 `job_id`）：

```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST http://localhost:8000/jobs \
  -F "file=@sample_input.zip" \
  -F "policy=<examples/policy.json" \
  -F "redactions=<examples/redactions.json"
# {"job_id":"218b2ae74856eadbd34f8504f8e4521f","status":"processing",
#  "status_url":"/jobs/218b2ae74856eadbd34f8504f8e4521f",
#  "detail":"identical submission already exists; returning the existing job"}
# HTTP 200
```

恢复完成后查询/下载，结果与从未中断时逐字节一致（以下为实跑采集）：

```bash
curl -s http://localhost:8000/jobs/218b2ae74856eadbd34f8504f8e4521f
```

```json
{
  "job_id": "218b2ae74856eadbd34f8504f8e4521f",
  "status": "done",
  "result": {
    "sha256": "be0c34e6278075de3a1afd598e16b0696d0e0076d885ae20bd79b3db55781707",
    "size_bytes": 16029,
    "download_url": "/jobs/218b2ae74856eadbd34f8504f8e4521f/download"
  }
}
```

```bash
curl -s http://localhost:8000/jobs/218b2ae74856eadbd34f8504f8e4521f/download | sha256sum
# be0c34e6278075de3a1afd598e16b0696d0e0076d885ae20bd79b3db55781707  -
```

> 调优：`LEASE_TTL_SECONDS`（默认 30）控制租约时长。崩溃后恢复的等待
> 上界约为 1.5×TTL（启动接管）或 TTL（重提接管）；多副本共享同一
> `DATA_DIR` 时该机制同样保证同一作业只有一个活跃执行者。


## 匿名化策略（版本化）

```json
{
  "version": "1.0",
  "salt": "demo-research-project",
  "remove_private_tags": true,
  "strict_references": true,
  "date_shift": { "enabled": true, "max_days": 365 },
  "tags": {
    "keep":    ["PatientSex", "PatientAge", "StudyDescription"],
    "empty":   ["InstitutionName"],
    "remove":  ["PatientAddress", "RequestAttributesSequence"],
    "hash":    ["PatientID"],
    "replace": { "ReferringPhysicianName": "ANON^MD" }
  }
}
```

- `version`：必填，当前支持 `"1.0"`；不支持的版本报 `POLICY_INVALID`。
- `salt`：可选项目级盐，并入策略哈希，隔离不同项目的映射结果。
- `remove_private_tags`（默认 `true`）：删除全部奇数组号私有标签（含序列内）。
- `strict_references`（默认 `true`）：实例引用的 SOPInstanceUID 必须都在
  提交包内，否则报 `MISSING_REFERENCE`（检查发生在动作执行前，因此即使引用
  位于随后会被移除的序列内也仍会被检查）。
- `date_shift.max_days`：日期偏移上限（0–3650 天），偏移量由
  HMAC(患者键) 确定性派生，同一患者所有实例一致。
- `tags`：在默认档案之上叠加。`keep` 将标签从默认清除动作中豁免；
  `empty`/`remove`/`hash`/`replace` 追加或覆盖默认动作。**所有动作都递归
  作用于顶层及保留下来的每一层 SQ 项数据集**，按标签出现次数逐项执行并计入
  审计；`remove` 一个 SQ 标签会整条删除该序列且不再遍历/计数其子项，
  `empty` 一个 SQ 标签则保留一个空序列（项全部丢弃）；对 SQ 标签使用
  `hash`/`replace` 报 `POLICY_CONFLICT`。
- 同一标签被赋予两个动作、`replace` 值违反标签 VR、或试图修改
  受管标签（`StudyInstanceUID`、`PixelData`、行列像素描述符等）
  均报 `POLICY_CONFLICT`。

默认档案：37 个 PHI 标签置空（姓名、生日、机构、医师、入组号、
检查/序列描述、核实人姓名/机构等），`PatientID` 哈希（HMAC-SHA256 前 16 位
十六进制），`PatientSex/Age/Size/Weight` 默认保留。完整清单见 `GET /policies`。

## 像素遮挡框

```json
[
  { "scope": "series", "uid": "1.2.826.0.1.3680043.9.7433.1.1.1",
    "boxes": [[0, 0, 128, 30]], "fill": 0 }
]
```

- `scope`：`instance`（SOPInstanceUID）/ `series` / `study`。
- `boxes`：`[x, y, width, height]` 像素坐标（原点左上），多帧图像逐帧应用。
- `fill`：填充值 0–65535，默认 0（黑）。
- 越界（`REDACTION_OUT_OF_BOUNDS`）、目标不存在
  （`REDACTION_TARGET_NOT_FOUND`）、布局不支持
  （`UNSUPPORTED_PIXEL_LAYOUT`）都会使作业整体失败。

## 错误代码

| 代码 | 含义 |
| --- | --- |
| `ZIP_INVALID` | 非法/损坏的 ZIP 或条目 |
| `READ_ERROR` | 文件形似 DICOM 但无法解析 |
| `NOT_DICOM`（警告） | 非 DICOM 文件被跳过并记入 `warnings` |
| `MISSING_UID` | 缺少 SOP/Study/Series Instance UID 或 SOP Class UID |
| `DUPLICATE_INSTANCE` | 同一 SOPInstanceUID 出现多次 |
| `MISSING_REFERENCE` | 引用的 SOPInstanceUID 不在提交包内 |
| `INVALID_VR` / `INVALID_VALUE` | 未知 VR 或 DA/TM/DT/UI 值非法（附标签号；递归检查 SQ 项内元素） |
| `COMPRESSED_PIXELS` | JPEG/RLE/Deflated 等压缩传输语法（需先解压） |
| `UNSUPPORTED_TRANSFER_SYNTAX` | 非隐式/显式 VR Little Endian（如 Big Endian） |
| `UNSUPPORTED_PIXEL_LAYOUT` | 遮挡目标像素布局不支持 |
| `REDACTION_*` | 遮挡目标缺失 / 遮挡框越界 |
| `POLICY_INVALID` / `POLICY_CONFLICT` | 策略结构非法 / 策略冲突 |
| `NO_INSTANCES` | 包内无可用 DICOM 实例 |
| `INPUT_UNAVAILABLE` | 中断恢复时持久化输入缺失或与记录的 `input_sha256` 不符，作业无法恢复 |

## 确定性说明

输出字节由以下因素唯一决定：输入 ZIP 字节、策略（规范化 JSON）、
遮挡规则、服务密钥 `ANON_SECRET`。具体措施：UID 映射 / 日期偏移 /
值哈希均用 HMAC-SHA256 派生；输出统一编码为 Explicit VR Little Endian、
固定文件元与 128 字节零前导；ZIP 条目按 `STUDY_xxxx/SERIES_xxxx/
INSTANCE_xxxxx.dcm` 确定性命名、固定 1980-01-01 时间戳与压缩级别；
`manifest.json` 键序固定。重复处理同一输入（在同一密钥作用域内）将得到
逐字节相同的结果。密钥指纹随结果与 `manifest.json` 返回，可据此确认结果
对应的密钥作用域；指纹非可逆，不能用于还原密钥。

## 安全与合规提示

- 本服务面向研究数据去标识化场景；输出是否满足具体法规（如 HIPAA
  Safe Harbor、GDPR）需由数据负责人评估， burned-in 文本建议配合遮挡框。
- `ANON_SECRET` 决定映射可复现性，请妥善保管并仅在受信环境部署。轮换密钥
  不会覆盖或删除既有结果：旧作业仍可按原 ID 下载，同内容在新密钥下会作为
  独立作业重新处理（见[「密钥轮换与判重作用域」](#密钥轮换与判重作用域)）。
  数据库与审计只保存密钥指纹，不保存明文密钥。
- UID 映射表（审计中返回）本身属于敏感关联信息，请按研究数据治理要求保存。
