# 调度编排（DolphinScheduler）

用 DolphinScheduler 3.2.2 接管原来的 cron，实现「增量 ETL → 数据质量校验」的依赖编排，
校验失败时阻断下游。

## 1. 部署形态

```
┌───────────────────────────────────────────┐
│ Docker 容器 dolphinscheduler-standalone    │
│   master / worker / api / alert 同进程     │
│   Web UI  :12345/dolphinscheduler/ui       │
│   任务执行：shell（容器内）                  │
└───────────────┬───────────────────────────┘
                │ ssh（密钥免密）
                ▼
┌───────────────────────────────────────────┐
│ 宿主机 172.17.0.1（Ubuntu 24.04）           │
│   /home/data/bigdata/jobs/*.sh             │
│   Spark 3.5.3（pyspark，local 模式）        │
│   MySQL / MinIO（容器）                     │
└───────────────────────────────────────────┘
```

**为什么任务要回连宿主机**：DS 容器里只有 JDK 8 + bash + curl，没有 python3 和 pyspark；
ETL 与校验脚本都在宿主机的 venv 里。生产上也是同样的模式——调度器只负责触发，计算在计算节点上跑。

## 2. 关键配置

### 2.1 SSH 数据源

| 字段 | 值 |
|---|---|
| 名称 | vm-host |
| 类型 | SSH |
| 主机 | 172.17.0.1 |
| 端口 | 22 |
| 用户 | data |

### 2.2 容器内免密（一次性）

```bash
# 容器内生成密钥
docker exec -u root dolphinscheduler-standalone \
  ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519 -q

# 公钥写入宿主机
cat /root/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys   # 在宿主机执行

# 容器内 ssh 别名，避免长命令被折行
cat > /root/.ssh/config <<'EOF'
Host host
    HostName 172.17.0.1
    User data
    IdentityFile /root/.ssh/id_ed25519
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
```

> 容器镜像默认没有 ssh 客户端，需要先 `apt-get install -y openssh-client`。

### 2.3 工作流 `retail_dw_daily`

两个 SHELL 节点，串行依赖：

| 节点 | 脚本（一行） | 作用 | 耗时 |
|---|---|---|---|
| `run_incr` | `ssh host 'bash /home/data/bigdata/jobs/wf_run_incr.sh'` | 激活 venv 跑 Spark 增量 | 14 秒 |
| `check_quality` | `ssh host 'bash /home/data/bigdata/jobs/run_quality.sh'` | 5 条断言，失败退出码 1 | 9 秒 |

依赖关系：`run_incr ──▶ check_quality`（上游成功才执行下游）。

### 2.4 定时

```
cron:    0 0 2 * * ? *      每天 02:00
时区:    Asia/Shanghai
状态:    ONLINE
```

## 3. 执行结果

![调度实例执行成功](ds-instance-success.png)


| 实例 | 结果 | 说明 |
|---|---|---|
| #1 | FAILURE（1 秒，exit 127） | 脚本被折成两行，`data@172.17.0.1` 被当成命令 |
| #2 | **SUCCESS（24 秒）** | `run_incr` 14s + `check_quality` 9s，两个任务退出码均为 0 |

实例 #2 的质量校验日志：

```
[PASS] 完整性   ODS 100,000 行 vs 源文件 100,000 行
[PASS] 唯一性   总行数 120,322 vs 主键去重 120,322（重复 0）
[PASS] 有效性   金额<=0 共 0 行；金额与 数量×单价 不一致 0 行
[PASS] 时效性   分区 90 个，单日行数 1229~1468，空分区 0 个
[PASS] 一致性   总行数 120,322 vs 基线 120,322（偏差 0.00%）
校验结果：5/5 通过，耗时 5.4 秒
process has exited ... exitStatusCode:0
```

## 4. 踩坑记录

| 现象 | 原因 | 解决 |
|---|---|---|
| 节点面板里找不到 SSH 节点 | 该发行版的节点列表只到 `REMOTESHELL` | SHELL 节点 + 容器内 `ssh` 命令替代 |
| 任务 1 秒失败，exit 127 | 长命令粘进文本框被折成两行，第二行被当命令执行 | 配 `~/.ssh/config` 别名，命令缩短到一行 |
| 节点设置面板全部置灰 | 工作流处于「上线」状态，定义被锁 | 先下线 → 编辑 → 保存 → 重新上线 |
| 定时配好但不触发 | 定时本身是 OFFLINE，需要单独上线 | 调度列表里点「定时上线」 |

## 5. 面试怎么讲

> “我用 DolphinScheduler 接管了原来的 cron：工作流里两个任务串行，第一个跑 Spark 增量，
> 第二个跑 5 条数据质量断言。断言失败会以非 0 退出，上游标红、下游自动阻断——这比 cron 强的地方
> 是有依赖、有重试、有告警、有可视化。
>
> 中间踩过一个坑：任务脚本被文本框折行导致 exit 127，后来把命令收敛成 SSH 别名才稳定；
> 另外修改已上线的工作流必须先下线，否则改的是草稿、跑的还是旧版本。”

## 6. 与 cron 的对比

| 维度 | cron | DolphinScheduler |
|---|---|---|
| 依赖编排 | 无（要自己写判断） | 支持 DAG，上游成功才跑下游 |
| 失败处理 | 无 | 重试次数、失败策略、实例追溯 |
| 告警 | 无 | 邮件/钉钉/Webhook |
| 可观测 | 只能看日志文件 | Web 界面看实例、任务、耗时、甘特图 |
| 补数 | 手工改脚本 | 指定日期区间批量补 |
