# quantitative-trading

一个面向 A 股场景的本地量化回测项目。

当前仓库已经内置了环境初始化脚本、数据同步脚本、终端批量回测入口和单次 GUI 回测入口。按下面的步骤执行即可完成环境配置、安装和使用。

## 1. 项目定位

- 数据源：东方财富直连、Baostock、Akshare、Tushare
- 回测引擎：Backtrader
- 运行方式：本地 Python + `zsh` 脚本
- 结果输出：`data/daily` 保存行情数据，`logs/backtest` 保存 HTML 回测报告

当前菜单默认开放的策略大类：

- 普通双均线
- 统计套利配对交易

当前项目还支持一层可选的 **大模型分析**：

- 单次回测完成后，基于结构化指标生成 AI 分析报告
- 批量回测完成后，基于候选结果生成 AI 横向比较报告
- 大模型只做分析和归因，不直接参与下单或改写策略逻辑
- 回测 HTML 标题后会自动出现 `AI` 按钮，可直接跳转到 AI 分析页
- 回测 HTML 里会同时展示 `买卖点` 与 `优化买卖点`
- `买卖建议` 面板支持在 `原策略` / `优化策略` 间切换
- 年 / 月 / 日时间筛选会同时作用于图表、买卖建议和日志面板

说明：

- 仓库里还有其他策略文件，但默认白名单只放开了上面两个大类
- `start.sh`、`start_backtest.sh`、`start_backtest_gui.sh` 都依赖项目根目录下的 `.venv`

## 2. 环境要求

请直接按下面的前提准备，不做兼容性兜底：

- macOS
- `zsh`
- `python3`
- 可访问外部行情数据源的网络环境

Python 版本建议：

- 至少 Python 3.10

原因：

- 代码里大量使用了 `str | None` 这类 3.10+ 语法
- 脚本默认使用 `.venv/bin/python`

## 3. 目录说明

核心目录和文件：

- `bootstrap.sh`：初始化虚拟环境并安装依赖
- `start.sh`：项目总入口
- `sync_data.sh`：同步股票数据
- `start_backtest.sh`：终端批量回测入口
- `start_backtest_gui.sh`：单次 GUI 回测入口
- `run_batch_backtest.py`：批量回测主程序
- `run_single_backtest_gui.py`：单次回测 + HTML 报告主程序
- `backtest/`：策略实现
- `sync/`：数据同步逻辑
- `utils/`：公共工具
- `data/daily/`：本地日线 CSV 数据
- `logs/backtest/`：回测 HTML 报告

## 4. 安装与初始化

进入项目目录：

```bash
cd /Users/y/Downloads/project/quantitative-trading
```

执行初始化脚本：

```bash
./bootstrap.sh
```

这个脚本会完成以下动作：

1. 创建 `.venv`
2. 创建 `data` 和 `logs` 目录
3. 升级 `pip`
4. 使用清华镜像安装 `requirements.txt`

当前依赖包括：

- `akshare`
- `baostock`
- `tushare`
- `backtrader`

如果脚本执行成功，你会看到：

```text
环境初始化完成
```

## 5. 快速开始

最短路径就是下面 3 步：

1. 初始化环境
2. 同步数据
3. 启动回测

对应命令：

```bash
./bootstrap.sh
./sync_data.sh
./start.sh
```

`start.sh` 会进入总菜单：

```text
1. GUI 回测 + AI 分析
2. GUI 回测（不启用 AI）
3. 终端批量回测
4. 拉取数据
q. 退出
```

说明：

- 直接回车默认进入 `1. GUI 回测 + AI 分析`
- 也支持快捷键：`ga`、`g`、`b`、`s`
- 现在不再需要先选 GUI，再单独选一次 AI 开关

## 5.1 启用大模型分析

当前项目已经支持 **可切换模型**，并且默认按 **DeepSeek** 预设运行。

默认加载顺序：

1. 项目根目录下的 `.env.local`
2. 项目根目录下的 `.env.llm.local`
3. 当前 shell 已经导出的环境变量

仓库里提供了一个示例文件：

```text
.env.llm.example
```

默认 DeepSeek 配置最少只需要这些字段：

```bash
QT_ENABLE_LLM_ANALYSIS=1
QT_LLM_PROVIDER=deepseek
QT_LLM_DEEPSEEK_API_KEY="你的 DeepSeek 密钥"
```

如果你想切换模型提供方，可以改：

```bash
QT_LLM_PROVIDER=openai
QT_LLM_OPENAI_API_KEY="你的 OpenAI 密钥"
QT_LLM_MODEL="gpt-5"
```

也支持自定义 OpenAI 兼容网关：

```bash
QT_LLM_PROVIDER=custom
QT_LLM_API_KEY="你的密钥"
QT_LLM_BASE_URL="你的兼容接口地址"
QT_LLM_MODEL="你的模型名"
```

可选参数：

```bash
QT_LLM_TIMEOUT_SECONDS=60
QT_LLM_TEMPERATURE=0.2
```

说明：

- 默认 provider 是 `deepseek`
- 默认 DeepSeek `base_url` 是 `https://api.deepseek.com`
- 默认 DeepSeek `model` 是 `deepseek-chat`
- 你可以随时用 `QT_LLM_PROVIDER` 和 `QT_LLM_MODEL` 覆盖默认值
- 如果已开启分析但缺少必要环境变量，程序会直接报错，不做静默跳过
- GUI 单次回测会产出单票分析报告
- 终端批量回测会额外产出一份横向排序分析报告

分析报告输出目录：

```text
logs/llm_analysis/
```

典型文件示例：

```text
logs/llm_analysis/simple_ma_backtest-sz.000725.html
logs/llm_analysis/simple_ma_backtest_v2-batch.html
```

单次回测报告现在会直接给出两套口径：

- `买卖点`：原始策略实际信号
- `优化买卖点`：在原策略基础上增加趋势确认、回撤保护和不追高过滤后的建议信号
- `买卖建议` 面板：可在 `原策略` / `优化策略` 间切换，并保持与时间筛选联动

## 6. 数据同步

### 6.1 交互式同步

执行：

```bash
./sync_data.sh
```

脚本会先让你选择同步方式：

```text
1. 输入固定股票代码同步
2. 拉取默认设置的数据
3. 拉取全部上证主板普通账户可买股票
```

然后再选择数据源：

```text
1. 东方财富直连
2. Baostock
3. Akshare
4. Tushare
5. 自动（东方财富直连 -> Baostock -> Akshare -> Tushare）
```

默认推荐直接选 `5`，也就是自动模式。

### 6.2 命令行同步

同步默认股票池：

```bash
./sync_data.sh --source=auto
```

同步指定股票：

```bash
./sync_data.sh --source=auto sh.600580 sz.000725
```

也支持直接写 6 位代码：

```bash
./sync_data.sh --source=auto 600580 000725
```

同步全部上证主板普通账户可买股票：

```bash
./sync_data.sh --source=auto --all-sh-main
```

可选数据源：

- `auto`
- `eastmoney`
- `baostock`
- `akshare`
- `tushare`

### 6.3 默认同步范围

当你不传股票代码，也不传 `--all-sh-main` 时，脚本会同步当前默认配置需要的数据，主要来自：

- `backtest/simple_ma_backtest.py` 里的默认标的
- 对应测试用例股票池

当前默认股票主要包括：

- `sh.600580`
- `sz.000100`
- `sz.000725`
- `sz.001308`
- `sz.002594`
- `sh.600255`

### 6.4 数据文件落盘位置

同步后的 CSV 会写到：

```text
data/daily/<股票代码>_<复权类型>.csv
```

例如：

```text
data/daily/sz.000725_hfq.csv
```

当前项目默认使用 `hfq`（后复权）。

### 6.5 基准指数同步

如果你希望在同步默认股票池时顺带同步基准指数数据，可以显式打开环境变量：

```bash
SYNC_INCLUDE_BENCHMARK=1 ./sync_data.sh
```

## 7. 回测使用方法

### 7.1 总入口

直接执行：

```bash
./start.sh
```

适合第一次使用，所有入口都在这里。

### 7.2 单次 GUI 回测

执行：

```bash
./start_backtest_gui.sh
```

特点：

- 交互式选择策略和股票
- 自动生成 HTML 报告
- macOS 下默认会尝试自动打开报告
- 如果某只单票缺少本地数据，会先尝试自动同步再回测

生成的报告默认在：

```text
logs/backtest/
```

常见报告文件示例：

```text
logs/backtest/simple_ma_backtest-sz.000725.html
logs/backtest/simple_ma_backtest_v1-sz.000725.html
logs/backtest/simple_ma_backtest_v2-sz.000725.html
logs/backtest/pair_trade_backtest-pair_000100_001308.html
```

普通双均线家族在 GUI 下会联跑多个版本，并额外生成一个对比页：

```text
logs/backtest/simple_ma_backtest-family-sz.000725.html
```

如果你不想自动打开浏览器，可以这样执行：

```bash
OPEN_GUI=0 ./start_backtest_gui.sh
```

#### 直接指定策略和股票

也可以跳过菜单，直接传参：

```bash
./start_backtest_gui.sh simple_ma_backtest sz.000725
```

或者只传股票代码，让策略继续走交互选择：

```bash
./start_backtest_gui.sh sz.000725
```

当前默认开放的策略 ID：

- `simple_ma_backtest`
- `simple_ma_backtest_v1`
- `simple_ma_backtest_v2`
- `pair_trade_backtest`

### 7.3 终端批量回测

执行：

```bash
./start_backtest.sh
```

这个入口会进入批量回测菜单，然后要求你：

1. 选择策略
2. 输入初始资金
3. 依次回测该策略下配置好的测试标的

也可以直接执行 Python 入口并指定策略：

```bash
./.venv/bin/python run_batch_backtest.py simple_ma_backtest
```

如果不传策略 ID，会使用默认策略。

### 7.4 单独运行 Python 入口

你也可以直接运行主程序：

```bash
./.venv/bin/python run_single_backtest_gui.py
./.venv/bin/python run_batch_backtest.py
./.venv/bin/python sync/sync_akshare.py --source=auto sz.000725
```

适合二次开发或调试脚本时使用。

## 8. 当前策略说明

### 8.1 普通双均线

当前有 3 个版本：

- `simple_ma_backtest`：基础版
- `simple_ma_backtest_v1`：强化收益版
- `simple_ma_backtest_v2`：稳健轮动版

默认股票池来自 `utils/default_stocks.py`。

### 8.2 统计套利配对交易

当前默认内置 3 组交易对：

- `pair_000100_001308`
- `pair_000100_000725`
- `pair_000725_001308`

策略信号使用价格比值的滚动 zscore，并增加阈值穿越开仓与 zscore 失效止损。

GUI 会优先推荐“高相关且价差具备均值回归特征”的本地交易对候选。

现在也支持在“统计套利配对交易”里手动输入两只股票，自选组合回测。

## 9. 输出产物

### 9.1 数据文件

- `data/daily/*.csv`
- `data/cookie.txt`：东方财富 cookie 缓存
- `data/a_share_code_name_cache.json`：股票池缓存

### 9.2 日志文件

- `logs/sync_akshare.log`
- `logs/backtest/*.html`

## 10. 常见问题

### 10.1 提示未找到 `.venv`

先执行：

```bash
./bootstrap.sh
```

### 10.2 提示缺少回测数据文件

先同步数据：

```bash
./sync_data.sh
```

如果你知道要测哪只股票，可以直接同步指定标的：

```bash
./sync_data.sh --source=auto sz.000725
```

### 10.3 GUI 没有自动弹出报告

这是正常现象，尤其在非默认图形环境下更常见。直接手动打开：

```text
logs/backtest/
```

对应的 HTML 文件即可。

### 10.4 为什么菜单里看不到某些策略文件

因为策略展示受 `backtest/strategy_registry.py` 里的白名单控制。当前默认只开放：

- `simple_ma_backtest`
- `pair_trade_backtest`

对应家族下的可见版本会自动出现在菜单中。

## 11. 一套推荐使用流程

如果你是第一次跑这个仓库，推荐直接照这个顺序：

```bash
cd /Users/y/Downloads/project/quantitative-trading
./bootstrap.sh
./sync_data.sh
./start.sh
```

如果你只想快速看一只股票的回测报告：

```bash
./bootstrap.sh
./sync_data.sh --source=auto sz.000725
./start_backtest_gui.sh simple_ma_backtest sz.000725
```
