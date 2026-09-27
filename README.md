# ty-transfer

云盘文件搬运流水线：**下载 → 无损分卷 → 上传 → 归档标记**，跑在 GitHub Actions 上。

技术栈：`rclone` + `ffmpeg`。

## 它干什么

| 步骤 | 动作 |
|---|---|
| 1 | 从源云盘按顺序取文件（超过单文件大小上限的自动跳过） |
| 2 | ffmpeg **无损分卷**成小段（`xxx.part001.mp4` 形式，`-c copy` 不重编码），控制单段不超硬上限 |
| 3 | 逐段上传到目标目录，上传后回查大小做校验 |
| 4 | 给每段加 `_D` 标记；源文件加 `已下载_` 前缀（表示已处理，下轮跳过） |
| 5 | 继续下一个，直到时间预算或数量上限，本轮收工 |

每轮开跑前会先拉一份「已存在编号清单」做比对，命中的直接跳过，**不重复下载**。

## 运行

- 自动：每 6 小时一次
- 手动：Actions → `ty-transfer` → Run workflow，可填处理个数、单文件上限、时间预算

## 文件

| 文件 | 说明 |
|---|---|
| `tools/ty_pipeline.py` | 主脚本（取件 → 分卷 → 上传 → 标记） |
| `tools/ty_cloud.py` | 云盘 API 客户端（纯标准库实现） |
| `tools/logmask.py` | 日志脱敏：文件名 / 路径 / 手机号 → 不可逆短标签 |
| `tools/pathcfg.py` | 路径配置读取（值来自 Secret，不写死在源码里） |

## Secrets

| 名字 | 内容 |
|---|---|
| `TY_AUTH` | 云盘接口授权串 |
| `TY_CLOUD_ID` | 云盘空间 ID |
| `RCLONE_CONF` | rclone 配置（目标远端凭据） |
| `PATHS_JSON` | 路径配置（不含凭据） |

全部由 GitHub 加密保存，**不落盘、不进仓库**。

## 安全约定

- 所有 workflow **仅由 `schedule` / `workflow_dispatch` 触发**，不接受外部 PR 触发。
- 每个 workflow 声明 `permissions: contents: read` —— `GITHUB_TOKEN` 只读。
- 运行时产生的 token 一律先 `::add-mask::` 再输出，日志里不会出现明文凭据。
- **日志中的文件名、目录路径、账号（手机号）一律替换成不可逆短哈希标签**，
  同一对象每次得到同一标签（便于排障），但无法反推原名。开关见 `tools/logmask.py`。
- 仓库中**不含任何凭据**；脚本只从环境变量 / Secret 读取。

## 本地运行

脚本不绑定 CI，本机装了 rclone + ffmpeg 也能跑（需要 `paths.local.json` 提供路径配置）：

```bash
export TY_AUTH=<授权串>
export TY_CLOUD_ID=<空间 ID>
python3 tools/ty_pipeline.py --max-files 5 --max-minutes 30
```
