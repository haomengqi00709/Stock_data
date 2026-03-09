# 📈 Trusted AI Advisory: 全球蓝筹股量化数据构建方案 (2026)

本方案旨在为 **Nasdaq 100** 与 **Fortune 500** 建立一套低成本、高可靠、机构级的私人数据仓库。

---

## 1. 目标覆盖范围 (Target Universe)
- **Nasdaq 100 (NDX):** 聚焦全球科技领头羊（高波动、高成长）。
- **Fortune 500 (US):** 聚焦实体经济压舱石（高股息、稳健价值）。
- **去重策略：** 以 Ticker Symbol 为唯一键，建立统一的代码映射表。

---

## 2. 数据全景图 (Data Scoping)

| 维度 | 数据项 (Data Points) | 推荐工具 | 频率 | 存储建议 |
| :--- | :--- | :--- | :--- | :--- |
| **量价行情** | OHLCV (开高低收量)、前复权价 (Adj Close) | `yfinance` / `AkShare` | 每日收盘 | Parquet |
| **财务指标** | PE (动/静), PB, ROE, 净利率, 负债率 | `AkShare` | 每日 | DuckDB |
| **三大报表** | 资产负债表、利润表、现金流量表 (10年历史) | `AkShare` / `FMP API` | 每季更新 | Parquet |
| **分红送配** | 拆股记录 (Splits)、派息 (Dividends) | `yfinance` | 发生时 | DuckDB |
| **AI 情绪** | 实时新闻标题、研报摘要、Gemini 情绪评分 | `Requests` + `Gemini` | 实时 | JSON/DuckDB |

---

## 3. 技术架构 (Technical Stack)



- **采集引擎:** Python 3.10+ (使用异步 `aiohttp` 提升大规模抓取效率)。
- **分析引擎:** `DuckDB` (支持 SQL 的高性能嵌入式分析库)。
- **存储格式:** `Apache Parquet` (列式压缩，比 CSV 节省 90% 空间，查询速度快百倍)。
- **AI 修复层:** `Gemini 1.5 Flash` (用于自动读取 PDF 财报并补齐 API 缺失数据)。

---

## 4. 落地执行计划 (Implementation Roadmap)

### 第一阶段：环境与基础表初始化 (Week 1)
1. **安装依赖:** `pip install akshare yfinance duckdb pandas`
2. **构建代码池:** 抓取最新 Nasdaq 100 成员名单，并合并 Fortune 500 列表。
3. **建立本地目录结构:**
   - `/data/raw/` (存储原始下载文件)
   - `/data/processed/` (存储清洗后的标准数据)
   - `/data/db/` (存放 DuckDB 索引文件)

### 第二阶段：冷启动——历史数据补完 (Week 2)
1. **量价补全:** 循环代码池，抓取过去 10 年的日线数据。
   - **防封号逻辑:** 每 10 只股票随机 `sleep(2, 5)` 秒。
2. **财务补全:** 下载过去 40 个季度的标准化财报数据。
3. **数据校验:** 编写 Python 脚本检查 `Adjusted Close` 是否存在异常跳变（防止拆股数据错误）。

### 第三阶段：增量更新与 AI 赋能 (Ongoing)
1. **每日作业:** 下午 16:30 (收盘后) 自动增量抓取当天行情。
2. **缺失修复 (AI Helper):**
   - 识别 `NaN` 或 0 值的财务字段。
   - 自动搜索 SEC EDGAR 或公司官网的 10-K/10-Q 链接。
   - 喂给 Gemini 提取数值并更新本地数据库。

---

## 5. 机构级避坑准则 (Pro-Tips)

1. **复权唯一性:** 始终以 `Adjusted Close` 进行技术指标计算，否则 RSI、MACD 等指标在拆股日会完全失效。
2. **时间戳一致性:** 统一使用 `UTC` 时间存储，在展示层再转换为本地时间（如 EST/EDT）。
3. **数据库连接:** 在回测时，利用 DuckDB 的 `read_parquet('data/*.parquet')` 功能，实现无需加载内存的全速扫描。

---

## 6. 核心 SQL 查询示例 (DuckDB)

```sql
-- 查询 ROE 大于 15% 且市盈率 PE 低于 20 的纳斯达克科技股
SELECT symbol, close, pe, roe
FROM stocks_standard
WHERE index = 'NASDAQ100' 
  AND roe > 0.15 
  AND pe < 20
ORDER BY roe DESC;