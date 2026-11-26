package gateway

// SQLite 持久层：请求日志 + 每日统计。
// 选择 SQLite(WAL, pure-Go driver) 的原因：单实例自用、零运维、单文件备份；
// 相比 JSON 文件：追加写不再需要压缩重写，且支持按时间/供应商索引查询。
// 配置（供应商/模型/成员/密钥）仍走内存缓存 + data.json 落盘：
// 它是小 blob、读路径全在内存，SQLite 化没有收益。

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动，无 CGO
)

// DB SQLite 句柄。
type DB struct {
	db *sql.DB
}

// OpenDB 打开（必要时创建）数据目录下的 SQLite 库并建表。
func OpenDB(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	sqlDB, err := sql.Open("sqlite", filepath.Join(dataDir, "gateway.db"))
	if err != nil {
		return nil, err
	}
	// WAL：读写不互斥；NORMAL：掉电最多丢最后一个事务（监控数据可接受）
	for _, p := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := sqlDB.Exec(p); err != nil {
			sqlDB.Close()
			return nil, err
		}
	}
	d := &DB{db: sqlDB}
	if err := d.migrate(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) migrate() error {
	_, err := d.db.Exec(`
CREATE TABLE IF NOT EXISTS request_logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	ts INTEGER NOT NULL,
	key_note TEXT DEFAULT '',
	model TEXT DEFAULT '',
	provider TEXT DEFAULT '',
	provider_id TEXT DEFAULT '',
	up_model TEXT DEFAULT '',
	up_key TEXT DEFAULT '',
	status INTEGER DEFAULT 0,
	latency_ms INTEGER DEFAULT 0,
	ttft_ms INTEGER DEFAULT 0,
	stream INTEGER DEFAULT 0,
	prompt_tokens INTEGER DEFAULT 0,
	completion_tokens INTEGER DEFAULT 0,
	cached_tokens INTEGER DEFAULT 0,
	req_id TEXT DEFAULT '',
	final INTEGER DEFAULT 0,
	error TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON request_logs(ts);
CREATE TABLE IF NOT EXISTS daily_stats (
	day TEXT NOT NULL,
	provider_id TEXT NOT NULL,
	model TEXT NOT NULL,
	requests INTEGER DEFAULT 0,
	errors INTEGER DEFAULT 0,
	prompt_tok INTEGER DEFAULT 0,
	compl_tok INTEGER DEFAULT 0,
	cached_tok INTEGER DEFAULT 0,
	latency_sum INTEGER DEFAULT 0,
	ttft_sum INTEGER DEFAULT 0,
	ttft_n INTEGER DEFAULT 0,
	creq INTEGER DEFAULT 0,
	cfail INTEGER DEFAULT 0,
	PRIMARY KEY (day, provider_id, model)
);`)
	if err != nil {
		return err
	}
	// 旧库升级：已有表补列（列已存在时报错忽略）
	for _, alter := range []string{
		"ALTER TABLE request_logs ADD COLUMN cached_tokens INTEGER DEFAULT 0",
		"ALTER TABLE daily_stats ADD COLUMN cached_tok INTEGER DEFAULT 0",
		"ALTER TABLE request_logs ADD COLUMN debug TEXT DEFAULT ''",
		"ALTER TABLE request_logs ADD COLUMN provider_id TEXT DEFAULT ''",
		"ALTER TABLE request_logs ADD COLUMN ttft_ms INTEGER DEFAULT 0",
		"ALTER TABLE daily_stats ADD COLUMN ttft_sum INTEGER DEFAULT 0",
		"ALTER TABLE daily_stats ADD COLUMN ttft_n INTEGER DEFAULT 0",
		"ALTER TABLE request_logs ADD COLUMN req_id TEXT DEFAULT ''",
		"ALTER TABLE request_logs ADD COLUMN final INTEGER DEFAULT 0",
		"ALTER TABLE daily_stats ADD COLUMN creq INTEGER DEFAULT 0",
		"ALTER TABLE daily_stats ADD COLUMN cfail INTEGER DEFAULT 0",
	} {
		if _, err := d.db.Exec(alter); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return err
		}
	}
	// 用请求日志重算有日志日期的每日统计（修复历史统计错数，幂等）
	d.RebuildStatsFromLogs()
	return nil
}

// RebuildStatsFromLogs 用请求日志重算「有日志日期」的每日统计，覆盖这些日的旧聚合；
// 没有日志覆盖的更早日期保持不动（日志保留期短于统计保留期）。
func (d *DB) RebuildStatsFromLogs() {
	if _, err := d.db.Exec(`DELETE FROM daily_stats WHERE day IN
(SELECT DISTINCT date(ts,'unixepoch','localtime') FROM request_logs)`); err != nil {
		log.Printf("[db] 重算统计-清理旧聚合失败: %v", err)
		return
	}
	if _, err := d.db.Exec(`INSERT INTO daily_stats (day,provider_id,model,requests,errors,prompt_tok,compl_tok,cached_tok,latency_sum,ttft_sum,ttft_n,creq,cfail)
SELECT date(ts,'unixepoch','localtime') AS day, provider_id, model,
       COUNT(*), SUM(CASE WHEN status=1 THEN 1 ELSE 0 END),
       SUM(prompt_tokens), SUM(completion_tokens), SUM(cached_tokens), SUM(latency_ms), SUM(ttft_ms),
       SUM(CASE WHEN ttft_ms>0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN final=1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN final=1 AND status=1 THEN 1 ELSE 0 END)
FROM request_logs GROUP BY day, provider_id, model`); err != nil {
		log.Printf("[db] 重算统计失败: %v", err)
		return
	}
	log.Printf("[db] 已从请求日志重算每日统计")
}

// Close 关闭连接。
func (d *DB) Close() { _ = d.db.Close() }

// InsertLog 追加一条请求日志。
func (d *DB) InsertLog(r RequestLog) {
	_, err := d.db.Exec(`INSERT INTO request_logs
(ts,key_note,model,provider,provider_id,up_model,up_key,status,latency_ms,ttft_ms,stream,prompt_tokens,completion_tokens,cached_tokens,req_id,final,error,debug)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.Time, r.KeyNote, r.Model, r.Provider, r.ProviderID, r.UpModel, r.UpKey, r.Status, r.Latency, r.TTFT,
		boolToInt(r.Stream), r.PromptTok, r.ComplTok, r.CachedTok, r.ReqID, boolToInt(r.Final), r.Error, r.Debug)
	if err != nil {
		log.Printf("[db] 写请求日志失败: %v", err)
	}
}

// UpsertStat 累加一条尝试到每日统计（含失败尝试）。
// 注意列位：requests 插入即记 1（冲突时 +1），errors 按是否失败记 0/1。
func (d *DB) UpsertStat(day, providerID, model string, isErr bool, prompt, compl, cached, latency, ttft int64, final bool) {
	creq, cfail := int64(0), int64(0)
	if final {
		creq = 1
		if isErr {
			cfail = 1
		}
	}
	_, err := d.db.Exec(`INSERT INTO daily_stats
(day,provider_id,model,requests,errors,prompt_tok,compl_tok,cached_tok,latency_sum,ttft_sum,ttft_n,creq,cfail)
VALUES(?,?,?,1,?,?,?,?,?,?,?,?,?)
ON CONFLICT(day,provider_id,model) DO UPDATE SET
	requests=requests+1,
	errors=errors+excluded.errors,
	prompt_tok=prompt_tok+excluded.prompt_tok,
	compl_tok=compl_tok+excluded.compl_tok,
	cached_tok=cached_tok+excluded.cached_tok,
	latency_sum=latency_sum+excluded.latency_sum,
	ttft_sum=ttft_sum+excluded.ttft_sum,
	ttft_n=ttft_n+excluded.ttft_n,
	creq=creq+excluded.creq,
	cfail=cfail+excluded.cfail`,
		day, providerID, model, boolToInt(isErr), prompt, compl, cached, latency, ttft, boolToInt(ttft > 0), creq, cfail)
	if err != nil {
		log.Printf("[db] 更新每日统计失败: %v", err)
	}
}

// FinalizeAttempt 把某次业务请求的最后一条尝试标记为 final（成功已由写入方直接标记）。
// 用于「重试/换密钥/换供应商全部失败」后收尾，保证一次客户端请求只计 1 次业务请求。
// 幂等：最后一条已是 final=1 时不做任何事。
func (d *DB) FinalizeAttempt(reqID string) {
	if reqID == "" {
		return
	}
	var id, ts, status int64
	var providerID, model string
	err := d.db.QueryRow(`SELECT id, ts, provider_id, model, status FROM request_logs
WHERE req_id=? ORDER BY id DESC LIMIT 1`, reqID).Scan(&id, &ts, &providerID, &model, &status)
	if err != nil {
		return
	}
	var fin int
	if err := d.db.QueryRow(`SELECT final FROM request_logs WHERE id=?`, id).Scan(&fin); err != nil || fin == 1 {
		return
	}
	if _, err := d.db.Exec(`UPDATE request_logs SET final=1 WHERE id=?`, id); err != nil {
		log.Printf("[db] 标记业务请求收尾失败: %v", err)
		return
	}
	day := time.Unix(ts, 0).Format("2006-01-02")
	cfail := 0
	if status == 1 {
		cfail = 1
	}
	if _, err := d.db.Exec(`UPDATE daily_stats SET creq=creq+1, cfail=cfail+? WHERE day=? AND provider_id=? AND model=?`,
		cfail, day, providerID, model); err != nil {
		log.Printf("[db] 更新业务请求计数失败: %v", err)
	}
}

// QueryStats 按日期范围（闭区间）取明细与汇总。
func (d *DB) QueryStats(from, to string) (rows []DailyStat, reqs, errs, ptok, ctok, cktok int64) {
	rows = []DailyStat{}
	rs, err := d.db.Query(`SELECT day,provider_id,model,requests,errors,prompt_tok,compl_tok,cached_tok,latency_sum,ttft_sum,ttft_n,creq,cfail
FROM daily_stats WHERE day BETWEEN ? AND ? ORDER BY day DESC`, from, to)
	if err != nil {
		log.Printf("[db] 查统计失败: %v", err)
		return rows, 0, 0, 0, 0, 0
	}
	defer rs.Close()
	for rs.Next() {
		var s DailyStat
		if err := rs.Scan(&s.Day, &s.ProviderID, &s.Model, &s.Requests, &s.Errors, &s.PromptTok, &s.ComplTok, &s.CachedTok, &s.LatencySum, &s.TTFTSum, &s.TTFTN, &s.CReq, &s.CFail); err == nil {
			rows = append(rows, s)
			reqs += s.Requests
			errs += s.Errors
			ptok += s.PromptTok
			ctok += s.ComplTok
			cktok += s.CachedTok
		}
	}
	return rows, reqs, errs, ptok, ctok, cktok
}

// QueryLogs 按时间范围取日志，时间升序返回，最多 limit 条（取最新）。
func (d *DB) QueryLogs(fromTs, toTs int64, limit int) []RequestLog {
	out := []RequestLog{}
	rs, err := d.db.Query(`SELECT ts,key_note,model,provider,up_model,up_key,status,latency_ms,stream,prompt_tokens,completion_tokens,cached_tokens,error,debug,ttft_ms,req_id,final
FROM request_logs WHERE ts BETWEEN ? AND ? ORDER BY ts DESC LIMIT ?`, fromTs, toTs, limit)
	if err != nil {
		log.Printf("[db] 查日志失败: %v", err)
		return out
	}
	defer rs.Close()
	for rs.Next() {
		var r RequestLog
		var stream int
		var fin int
		if err := rs.Scan(&r.Time, &r.KeyNote, &r.Model, &r.Provider, &r.UpModel, &r.UpKey, &r.Status, &r.Latency, &stream, &r.PromptTok, &r.ComplTok, &r.CachedTok, &r.Error, &r.Debug, &r.TTFT, &r.ReqID, &fin); err == nil {
			r.Stream = stream == 1
			r.Final = fin == 1
			out = append(out, r)
		}
	}
	// DESC 取最新 limit 条 → 反转成升序，保持与旧接口一致
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// QueryLogsRecent 取最新 limit 条日志（新→旧），供「日志」页使用。
func (d *DB) QueryLogsRecent(limit int) []RequestLog {
	return d.queryLogsOrdered(`SELECT ts,key_note,model,provider,up_model,up_key,status,latency_ms,stream,prompt_tokens,completion_tokens,cached_tokens,error,debug,ttft_ms,req_id,final
FROM request_logs ORDER BY ts DESC, id DESC LIMIT ?`, limit)
}

// QueryLogsPaged 分页取日志（新→旧）：limit 每页条数，offset 偏移。
func (d *DB) QueryLogsPaged(limit, offset int) []RequestLog {
	return d.queryLogsOrdered(`SELECT ts,key_note,model,provider,up_model,up_key,status,latency_ms,stream,prompt_tokens,completion_tokens,cached_tokens,error,debug,ttft_ms,req_id,final
FROM request_logs ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?`, limit, offset)
}

// CountLogs 日志总条数（分页用）。
func (d *DB) CountLogs() int64 {
	var n int64
	if err := d.db.QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&n); err != nil {
		return 0
	}
	return n
}

func (d *DB) queryLogsOrdered(query string, args ...any) []RequestLog {
	out := []RequestLog{}
	rs, err := d.db.Query(query, args...)
	if err != nil {
		log.Printf("[db] 查日志失败: %v", err)
		return out
	}
	defer rs.Close()
	for rs.Next() {
		var r RequestLog
		var stream int
		var fin int
		if err := rs.Scan(&r.Time, &r.KeyNote, &r.Model, &r.Provider, &r.UpModel, &r.UpKey, &r.Status, &r.Latency, &stream, &r.PromptTok, &r.ComplTok, &r.CachedTok, &r.Error, &r.Debug, &r.TTFT, &r.ReqID, &fin); err == nil {
			r.Stream = stream == 1
			r.Final = fin == 1
			out = append(out, r)
		}
	}
	return out
}

// Cleanup 清理过期数据：日志保留 logDays 天，统计保留 statDays 天。
func (d *DB) Cleanup(logDays, statDays int) {
	cutoff := time.Now().AddDate(0, 0, -logDays).Unix()
	if _, err := d.db.Exec(`DELETE FROM request_logs WHERE ts < ?`, cutoff); err != nil {
		log.Printf("[db] 清理日志失败: %v", err)
	}
	dayCutoff := time.Now().AddDate(0, 0, -statDays).Format("2006-01-02")
	if _, err := d.db.Exec(`DELETE FROM daily_stats WHERE day < ?`, dayCutoff); err != nil {
		log.Printf("[db] 清理统计失败: %v", err)
	}
}

// MigrateJSONL 一次性把旧 requests.jsonl 导入 SQLite（成功后改名 .imported）。
func (d *DB) MigrateJSONL(dataDir string) {
	path := filepath.Join(dataDir, "requests.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	n := 0
	for _, ln := range strings.Split(string(b), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		var r RequestLog
		if json.Unmarshal([]byte(ln), &r) == nil && r.Time > 0 {
			d.InsertLog(r)
			n++
		}
	}
	if err := os.Rename(path, path+".imported"); err == nil {
		log.Printf("[db] 已迁移 requests.jsonl %d 条入 SQLite", n)
	}
}

// ImportStats 用绝对值导入历史每日统计（配置迁移用，不做累加）。
func (d *DB) ImportStats(stats []DailyStat) {
	for _, s := range stats {
		_, err := d.db.Exec(`INSERT OR REPLACE INTO daily_stats
(day,provider_id,model,requests,errors,prompt_tok,compl_tok,cached_tok,latency_sum) VALUES(?,?,?,?,?,?,?,?,?)`,
			s.Day, s.ProviderID, s.Model, s.Requests, s.Errors, s.PromptTok, s.ComplTok, s.CachedTok, s.LatencySum)
		if err != nil {
			log.Printf("[db] 导入历史统计失败: %v", err)
		}
	}
	if len(stats) > 0 {
		log.Printf("[db] 已迁移配置内每日统计 %d 条入 SQLite", len(stats))
	}
}

// MigrateConfigStats 把配置 JSON 里的历史每日统计迁入 SQLite，随后从配置中清空
// （此后统计只写 SQLite，配置不再携带 DailyStats）。
func (d *DB) MigrateConfigStats(store *Store) {
	var stats []DailyStat
	store.View(func(c *Config) { stats = append(stats, c.DailyStats...) })
	if len(stats) == 0 {
		return
	}
	d.ImportStats(stats)
	store.Update(func(c *Config) { c.DailyStats = nil })
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
