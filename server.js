'use strict';

/**
 * 水质实验室 · 样本分装与混样溯源工作台
 *
 * 核心不变量：
 *  - 每个操作有唯一操作号；同一操作号重复提交相同内容时幂等返回，不重复扣量。
 *  - 一次操作的来源瓶不得重复，取用体积均为正整数，目标瓶体积 = 取用量之和。
 *  - 任一时刻任何瓶的余量不得为负（按 操作时间 + 入库顺序 重放全量台账校验）。
 *  - 更正历史操作的取用量时，在事务内重算；若导致后续操作超用，整次更正回滚，
 *    原数据保持不变，并返回冲突记录。
 *  - 更正支持两阶段：预演（不写库）给出余量变化与目标瓶及下游瓶的原瓶贡献前后对照，
 *    并签发绑定 更正计划 + 台账版本 的确认凭据；确认在单事务中核对凭据并原子应用，
 *    结果与预演一致；同一凭据重试幂等返回首次结果，不重复更正。
 *  - 质检隔离：已登记原瓶可设置隔离（原因非空）或解除；只要来源链含隔离原瓶，
 *    衍生瓶即受影响；受影响瓶不得用于新分装 / 混样（单条提交、批次预演与提交均拦截，
 *    批内前项受影响时后项引用一并拦截，整批不落库）。历史记录与更正不受影响，
 *    更正成功后影响范围按当前来源关系重算。隔离状态变动使台账版本递增，
 *    未使用的批次提交 / 更正确认凭据失效；已成功提交的相同请求重试沿用原幂等结果。
 *  - 证据封存：凭唯一封存号把某个已登记瓶在当前已提交台账状态下的形成证据
 *    （完整来源谱系、各形成操作时间与取用量、当时余量、精确原瓶贡献、隔离命中）
 *    原样固化到 evidence_seals 表并计算 SHA-256 内容摘要。封存只读台账、只写证据表，
 *    不递增台账版本，已发放凭据不受影响；同号同目标重试幂等返回原封存，同号异目标 409，
 *    目标不存在或取证失败不占号；封存内容永不随后续分装 / 更正 / 隔离变动改写，
 *    比较时用与封存相同的构建器从当前台账重算并逐项对照（未变化项不列出）。
 *  - better-sqlite3 为同步驱动，所有写操作包裹在事务中串行执行，
 *    并发请求争用同一来源瓶时天然保证体积守恒。
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Database = require('better-sqlite3');

const PORT = parseInt(process.env.PORT || '3000', 10);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data', 'lab.db');

fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
CREATE TABLE IF NOT EXISTS bottles (
  id             TEXT PRIMARY KEY,
  kind           TEXT NOT NULL CHECK (kind IN ('original','derived')),
  created_time   TEXT NOT NULL,                 -- 原瓶为采样时间，衍生瓶为创建它的操作时间
  initial_volume INTEGER NOT NULL DEFAULT 0 CHECK (initial_volume >= 0),
  created_op     TEXT                           -- 衍生瓶由哪个操作创建
);
CREATE TABLE IF NOT EXISTS operations (
  op_id         TEXT PRIMARY KEY,
  seq           INTEGER NOT NULL UNIQUE,        -- 入库顺序，用于同一时间戳下的稳定排序
  target_bottle TEXT NOT NULL,
  op_time       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_sources (
  op_id         TEXT NOT NULL REFERENCES operations(op_id) ON DELETE CASCADE,
  source_bottle TEXT NOT NULL,
  volume        INTEGER NOT NULL CHECK (volume > 0),
  PRIMARY KEY (op_id, source_bottle)            -- 同一操作内来源不得重复
);
CREATE INDEX IF NOT EXISTS idx_op_sources_bottle ON operation_sources(source_bottle);
CREATE INDEX IF NOT EXISTS idx_operations_target ON operations(target_bottle);
CREATE TABLE IF NOT EXISTS batches (
  batch_id     TEXT PRIMARY KEY,
  plan_hash    TEXT NOT NULL,                 -- 规范化批次计划的 SHA-256，用于幂等返回与冲突判定
  item_count   INTEGER NOT NULL,
  committed_at TEXT NOT NULL                  -- 服务器提交时刻（ISO 字符串）
);
CREATE TABLE IF NOT EXISTS batch_items (
  batch_id TEXT NOT NULL REFERENCES batches(batch_id),
  idx      INTEGER NOT NULL,                  -- 批内录入顺序（0 起）；同操作时间时按此序核算
  op_id    TEXT NOT NULL UNIQUE REFERENCES operations(op_id),
  PRIMARY KEY (batch_id, idx)
);
CREATE TABLE IF NOT EXISTS correction_confirms (
  token_hash   TEXT PRIMARY KEY,              -- 确认凭据的 SHA-256，用于“同一凭据重试”幂等判定
  op_id        TEXT NOT NULL,
  plan_hash    TEXT NOT NULL,                 -- 首次确认的更正计划哈希
  response     TEXT NOT NULL,                 -- 首次成功响应（JSON），重试时原样返回
  confirmed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantines (
  bottle_id  TEXT PRIMARY KEY REFERENCES bottles(id),  -- 仅已登记原始瓶可隔离（写入时校验）
  reason     TEXT NOT NULL,                            -- 隔离原因（非空）
  updated_at TEXT NOT NULL                             -- 最近一次设置 / 更新原因的时刻（ISO 字符串）
);
CREATE TABLE IF NOT EXISTS evidence_seals (
  seal_id        TEXT PRIMARY KEY,
  bottle_id      TEXT NOT NULL,                   -- 被封存的目标瓶
  ledger_version INTEGER NOT NULL,                -- 封存时的台账版本（证据取自该已提交状态）
  sealed_at      TEXT NOT NULL,                   -- 封存时刻（ISO 字符串）
  digest         TEXT NOT NULL,                   -- 内容摘要：证据规范化 JSON 的 SHA-256
  evidence       TEXT NOT NULL                    -- 完整证据 JSON（原样保存，永不更新）
);
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`);
db.prepare("INSERT OR IGNORE INTO meta (key, value) VALUES ('ledger_version', '0')").run();

// ---------------------------------------------------------------- 工具与校验

class ApiError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details || null;
  }
}

const TIME_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/;

function normId(v, field) {
  if (typeof v !== 'string' || !v.trim()) throw new ApiError(400, 'BAD_ID', `${field}不能为空`);
  return v.trim();
}

function normTime(v, field) {
  if (typeof v !== 'string' || !TIME_RE.test(v.trim())) {
    throw new ApiError(400, 'BAD_TIME', `${field}格式应为 YYYY-MM-DDTHH:MM（如 2026-10-04T09:30）`);
  }
  const t = v.trim();
  return t.length === 16 ? t + ':00' : t; // 统一为 YYYY-MM-DDTHH:MM:SS，字典序即时间序
}

function normVolume(v, field) {
  const n = Number(v);
  if (!Number.isInteger(n) || n <= 0) {
    throw new ApiError(400, 'BAD_VOLUME', `${field}必须为正整数（毫升）`);
  }
  return n;
}

function parseSources(raw) {
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new ApiError(400, 'NO_SOURCES', '每次操作至少需要一个来源瓶');
  }
  const seen = new Set();
  return raw.map((s, i) => {
    const bottle = normId(s && s.bottle, `来源瓶编号（第 ${i + 1} 行）`);
    const volume = normVolume(s && s.volume, `取用体积（第 ${i + 1} 行）`);
    if (seen.has(bottle)) throw new ApiError(400, 'DUP_SOURCE', `同一操作内来源瓶 ${bottle} 重复`);
    seen.add(bottle);
    return { bottle, volume };
  });
}

// ---------------------------------------------------------------- 预编译语句

const qBottle     = db.prepare('SELECT * FROM bottles WHERE id = ?');
const qInsBottle  = db.prepare('INSERT INTO bottles (id, kind, created_time, initial_volume, created_op) VALUES (?, ?, ?, ?, ?)');
const qOp         = db.prepare('SELECT * FROM operations WHERE op_id = ?');
const qInsOp      = db.prepare('INSERT INTO operations (op_id, seq, target_bottle, op_time) VALUES (?, ?, ?, ?)');
const qNextSeq    = db.prepare('SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM operations');
const qOpSources  = db.prepare('SELECT source_bottle, volume FROM operation_sources WHERE op_id = ? ORDER BY source_bottle');
const qInsSource  = db.prepare('INSERT INTO operation_sources (op_id, source_bottle, volume) VALUES (?, ?, ?)');
const qDelSources = db.prepare('DELETE FROM operation_sources WHERE op_id = ?');
const qBatch      = db.prepare('SELECT * FROM batches WHERE batch_id = ?');
const qInsBatch   = db.prepare('INSERT INTO batches (batch_id, plan_hash, item_count, committed_at) VALUES (?, ?, ?, ?)');
const qInsBatchItem = db.prepare('INSERT INTO batch_items (batch_id, idx, op_id) VALUES (?, ?, ?)');
const qVersion    = db.prepare("SELECT value FROM meta WHERE key = 'ledger_version'");
const qBumpVersion = db.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'ledger_version'");
const qCorrConfirmGet = db.prepare('SELECT * FROM correction_confirms WHERE token_hash = ?');
const qCorrConfirmIns = db.prepare('INSERT INTO correction_confirms (token_hash, op_id, plan_hash, response, confirmed_at) VALUES (?, ?, ?, ?, ?)');
const qQuarGet = db.prepare('SELECT * FROM quarantines WHERE bottle_id = ?');
const qQuarAll = db.prepare('SELECT * FROM quarantines ORDER BY bottle_id');
const qQuarIns = db.prepare('INSERT INTO quarantines (bottle_id, reason, updated_at) VALUES (?, ?, ?)');
const qQuarUpd = db.prepare('UPDATE quarantines SET reason = ?, updated_at = ? WHERE bottle_id = ?');
const qQuarDel = db.prepare('DELETE FROM quarantines WHERE bottle_id = ?');
const qSealGet = db.prepare('SELECT * FROM evidence_seals WHERE seal_id = ?');
const qSealAll = db.prepare('SELECT * FROM evidence_seals ORDER BY sealed_at, seal_id');
const qSealIns = db.prepare('INSERT INTO evidence_seals (seal_id, bottle_id, ledger_version, sealed_at, digest, evidence) VALUES (?, ?, ?, ?, ?, ?)');

/**
 * 台账版本号：任何写操作（登记原瓶 / 单条操作 / 更正 / 批次提交）成功提交后递增。
 * 批次提交凭据与版本号绑定；预演后若台账变化，凭据自动失效，须重新预演。
 */
function ledgerVersion() { return parseInt(qVersion.get().value, 10); }

// ------------------------------------------------------- 台账重放（守恒校验）

/** 从数据库加载全量台账：瓶 + 按 (操作时间, 入库顺序) 排序的操作及其来源。 */
function loadLedger() {
  const bottles = db.prepare('SELECT id, kind, initial_volume, created_time FROM bottles').all();
  const ops = db.prepare('SELECT op_id, seq, target_bottle, op_time FROM operations ORDER BY op_time, seq').all();
  const srcRows = db.prepare('SELECT op_id, source_bottle, volume FROM operation_sources ORDER BY source_bottle').all();
  const byOp = new Map();
  for (const s of srcRows) {
    if (!byOp.has(s.op_id)) byOp.set(s.op_id, []);
    byOp.get(s.op_id).push({ bottle: s.source_bottle, volume: s.volume });
  }
  return { bottles, ops: ops.map((o) => ({ ...o, sources: byOp.get(o.op_id) || [] })) };
}

/** 操作排序：先按操作时间，同一时间按入库顺序（批内即录入顺序）。 */
const cmpOps = (a, b) => (a.op_time < b.op_time ? -1 : a.op_time > b.op_time ? 1 : a.seq - b.seq);

/**
 * 按 (操作时间, 入库顺序) 重放操作序列，计算每个瓶的余量。
 * 任一取用使余量为负即记录冲突；冲突瓶按 0 继续推演，以便后续冲突的
 * “可用量”仍然可读。返回 { balances, conflicts }。
 */
function replayLedger(bottleRows, ops) {
  const balances = new Map();
  for (const b of bottleRows) {
    if (b.kind === 'original') balances.set(b.id, b.initial_volume);
  }
  const conflicts = [];
  for (const op of ops) {
    let total = 0;
    for (const s of op.sources) {
      const avail = balances.get(s.bottle) || 0;
      if (s.volume > avail) {
        conflicts.push({ op_id: op.op_id, bottle: s.bottle, requested: s.volume, available: avail });
        balances.set(s.bottle, 0);
      } else {
        balances.set(s.bottle, avail - s.volume);
      }
      total += s.volume;
    }
    balances.set(op.target_bottle, (balances.get(op.target_bottle) || 0) + total);
  }
  return { balances, conflicts };
}

/** 重放当前数据库中的全量台账。 */
function recompute() {
  const { bottles, ops } = loadLedger();
  return replayLedger(bottles, ops);
}

// ---------------------------------------------------------------- 原瓶隔离（质检）

/**
 * 隔离影响范围：按当前已提交来源关系（形成图），从每个隔离原瓶沿下游传播。
 * 只要来源链含隔离原瓶，衍生瓶即受影响；隔离原瓶本身亦受影响（命中自身）。
 * 多个原瓶同时隔离时，一瓶可命中多项；解除其中一个仅移除该原瓶的命中，
 * 仍命中其他隔离原瓶的瓶保持受影响。
 * 返回 Map<受影响瓶编号, 命中的隔离原瓶及原因 [{bottle, reason}]>（命中按原瓶编号排序）。
 * 影响范围在查询 / 校验时基于当前台账实时重建：更正成功后形成图变化，
 * 影响范围随之重算；更正失败回滚后台账不变，影响范围维持原状态。
 */
function quarantineImpactMap() {
  const quars = qQuarAll.all();
  const impact = new Map();
  if (!quars.length) return impact;
  const { derivedMap } = loadCompositionMaps();
  for (const q of quars) {
    const reached = [q.bottle_id, ...downstreamBottles(derivedMap, q.bottle_id)];
    for (const id of reached) {
      if (!impact.has(id)) impact.set(id, []);
      impact.get(id).push({ bottle: q.bottle_id, reason: q.reason });
    }
  }
  for (const hits of impact.values()) {
    hits.sort((a, b) => (a.bottle < b.bottle ? -1 : a.bottle > b.bottle ? 1 : 0));
  }
  return impact;
}

/** 命中明细 → 简短文本，用于错误消息与展示。 */
function hitsText(hits) {
  return hits.map((h) => `${h.bottle}（原因：${h.reason}）`).join('、');
}

/**
 * 校验一批来源瓶是否受隔离影响（单条操作提交用）。
 * 全部可用时返回 null；否则返回 409 ApiError，details 指出被拦截的瓶及各自命中的隔离原瓶与原因。
 */
function quarantineBlockIfAffected(sources) {
  const impact = quarantineImpactMap();
  if (!impact.size) return null;
  const blocked = sources.filter((s) => impact.has(s.bottle))
    .map((s) => ({ bottle: s.bottle, hits: impact.get(s.bottle) }));
  if (!blocked.length) return null;
  return new ApiError(409, 'QUARANTINED_SOURCE',
    `以下来源瓶受隔离影响，不得用于新分装 / 混样：${blocked.map((b) => `${b.bottle}（命中隔离原瓶 ${hitsText(b.hits)}）`).join('；')}`,
    blocked);
}

// ---------------------------------------------------------------- 业务事务

function sameSources(a, b) {
  if (a.length !== b.length) return false;
  const key = (s) => s.map((x) => `${x.bottle}=${x.volume}`).sort().join('|');
  return key(a) === key(b);
}

/** 新建分装/混样操作（事务）。重复操作号且内容一致 → 幂等返回；不一致 → 409。 */
const createOperation = db.transaction((p) => {
  const existing = qOp.get(p.op_id);
  if (existing) {
    const oldSrcs = qOpSources.all(p.op_id).map((s) => ({ bottle: s.source_bottle, volume: s.volume }));
    const identical =
      existing.target_bottle === p.target &&
      existing.op_time === p.op_time &&
      sameSources(oldSrcs, p.sources);
    if (identical) {
      return { ok: true, idempotent: true, message: `操作号 ${p.op_id} 已提交过相同内容，幂等返回，未重复扣量` };
    }
    throw new ApiError(409, 'OP_CONFLICT', `操作号 ${p.op_id} 已存在且内容不一致，请更换操作号`);
  }
  if (qBottle.get(p.target)) {
    throw new ApiError(409, 'BOTTLE_CONFLICT', `目标瓶编号 ${p.target} 已存在（编号冲突）`);
  }
  for (const s of p.sources) {
    const b = qBottle.get(s.bottle);
    if (!b) throw new ApiError(400, 'SOURCE_NOT_FOUND', `来源瓶 ${s.bottle} 不存在`);
    if (p.op_time < b.created_time) {
      throw new ApiError(400, 'TIME_BEFORE_SOURCE',
        `操作时间 ${p.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${b.created_time}`);
    }
  }
  const quarBlock = quarantineBlockIfAffected(p.sources);
  if (quarBlock) throw quarBlock;
  qInsOp.run(p.op_id, qNextSeq.get().next, p.target, p.op_time);
  qInsBottle.run(p.target, 'derived', p.op_time, 0, p.op_id);
  for (const s of p.sources) qInsSource.run(p.op_id, s.bottle, s.volume);

  const { conflicts } = recompute();
  if (conflicts.length) {
    throw new ApiError(409, 'OVERDRAW', '取用量超过来源瓶可用余量，操作已拒绝，数据未变更', conflicts);
  }
  qBumpVersion.run();
  return { ok: true, idempotent: false, message: `操作 ${p.op_id} 已登记` };
});

/** 更正历史操作的取用量（事务）。导致后续超用 → 整体回滚并返回冲突记录。 */
const correctOperation = db.transaction((opId, sources) => {
  const op = qOp.get(opId);
  if (!op) throw new ApiError(404, 'OP_NOT_FOUND', `操作 ${opId} 不存在`);
  for (const s of sources) {
    const b = qBottle.get(s.bottle);
    if (!b) throw new ApiError(400, 'SOURCE_NOT_FOUND', `来源瓶 ${s.bottle} 不存在`);
    if (op.op_time < b.created_time) {
      throw new ApiError(400, 'TIME_BEFORE_SOURCE',
        `操作时间 ${op.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${b.created_time}`);
    }
  }
  qDelSources.run(opId);
  for (const s of sources) qInsSource.run(opId, s.bottle, s.volume);

  const { conflicts } = recompute();
  if (conflicts.length) {
    throw new ApiError(409, 'CORRECTION_REJECTED',
      '更正将导致以下操作超用，整次更正已拒绝，原数据保持不变', conflicts);
  }
  qBumpVersion.run();
  return { ok: true, message: `操作 ${opId} 的取用量已更正，相关瓶余量已按操作顺序重算` };
});

/**
 * 设置原瓶隔离（事务）：仅已登记原始瓶；原因非空。
 * 已隔离且原因相同 → 幂等返回（台账版本不变，已发放凭据不受影响）；
 * 已隔离但原因不同 → 更新原因；新隔离 → 写入。
 * 状态发生变动时台账版本递增，未使用的批次提交凭据与更正确认凭据随之失效。
 */
const setQuarantine = db.transaction((id, reason) => {
  const b = qBottle.get(id);
  if (!b) throw new ApiError(404, 'BOTTLE_NOT_FOUND', `瓶 ${id} 不存在`);
  if (b.kind !== 'original') {
    throw new ApiError(400, 'NOT_ORIGINAL', `仅已登记原始瓶可设置隔离，${id} 为衍生瓶（其来源链中的原瓶被隔离时它会自动受影响）`);
  }
  const cur = qQuarGet.get(id);
  const now = new Date().toISOString();
  if (cur) {
    if (cur.reason === reason) {
      return { ok: true, idempotent: true, bottle: id, reason, updated_at: cur.updated_at,
               message: `原瓶 ${id} 已处于隔离状态且原因相同，幂等返回，未重复变更` };
    }
    qQuarUpd.run(reason, now, id);
    qBumpVersion.run();
    return { ok: true, idempotent: false, updated: true, bottle: id, reason, updated_at: now,
             message: `原瓶 ${id} 的隔离原因已更新，影响范围已按当前来源关系重算` };
  }
  qQuarIns.run(id, reason, now);
  qBumpVersion.run();
  return { ok: true, idempotent: false, bottle: id, reason, updated_at: now,
           message: `原瓶 ${id} 已隔离，其下游衍生瓶全部受影响，受影响瓶不得用于新分装 / 混样` };
});

/** 解除原瓶隔离（事务）。状态变动使台账版本递增，未使用凭据失效。 */
const liftQuarantine = db.transaction((id) => {
  const b = qBottle.get(id);
  if (!b) throw new ApiError(404, 'BOTTLE_NOT_FOUND', `瓶 ${id} 不存在`);
  const cur = qQuarGet.get(id);
  if (!cur) throw new ApiError(409, 'NOT_QUARANTINED', `瓶 ${id} 未处于隔离状态，无需解除`);
  qQuarDel.run(id);
  qBumpVersion.run();
  return { ok: true, bottle: id,
           message: `原瓶 ${id} 已解除隔离；仍命中其他隔离原瓶的瓶保持受影响，影响范围已按当前来源关系重算` };
});

// ---------------------------------------------------------------- 批次（预演 / 提交）

/** 提交凭据签名密钥：默认进程启动时随机生成，重启后旧凭据自动失效（需重新预演）。 */
const TOKEN_SECRET = process.env.TOKEN_SECRET || crypto.randomBytes(32).toString('hex');

/** 逐条规范化批次条目：格式错误收集到对应条目而不中断（预演需逐条反馈）。 */
function normalizeBatchItems(rawItems) {
  const items = [];
  const itemErrors = new Map(); // index -> [{code, message}]
  const pushErr = (i, code, message) => {
    if (!itemErrors.has(i)) itemErrors.set(i, []);
    itemErrors.get(i).push({ code, message });
  };
  rawItems.forEach((raw, i) => {
    const it = { op_id: null, target: null, op_time: null, sources: [] };
    const n = i + 1;
    try { it.op_id = normId(raw && raw.op_id, `操作号（第 ${n} 条）`); } catch (e) { pushErr(i, e.code, e.message); }
    try { it.target = normId(raw && raw.target_bottle, `目标瓶编号（第 ${n} 条）`); } catch (e) { pushErr(i, e.code, e.message); }
    try { it.op_time = normTime(raw && raw.op_time, `操作时间（第 ${n} 条）`); } catch (e) { pushErr(i, e.code, e.message); }
    const rawSrcs = raw && raw.sources;
    if (!Array.isArray(rawSrcs) || rawSrcs.length === 0) {
      pushErr(i, 'NO_SOURCES', `第 ${n} 条操作至少需要一个来源瓶`);
    } else {
      const seen = new Set();
      rawSrcs.forEach((s, j) => {
        let bottle = null; let volume = null; let bad = false;
        try { bottle = normId(s && s.bottle, `来源瓶编号（第 ${n} 条第 ${j + 1} 行）`); } catch (e) { pushErr(i, e.code, e.message); bad = true; }
        try { volume = normVolume(s && s.volume, `取用体积（第 ${n} 条第 ${j + 1} 行）`); } catch (e) { pushErr(i, e.code, e.message); bad = true; }
        if (!bad) {
          if (seen.has(bottle)) pushErr(i, 'DUP_SOURCE', `第 ${n} 条操作内来源瓶 ${bottle} 重复`);
          else { seen.add(bottle); it.sources.push({ bottle, volume }); }
        }
      });
    }
    items.push(it);
  });
  return { items, itemErrors };
}

/**
 * 批次业务校验（预演与提交共用，提交时在事务内重跑）：
 * 编号冲突（批内 / 现有台账）、来源存在性（可引用批内前项生成的瓶）、
 * 操作时间不得早于来源创建时间、来源瓶不得受隔离影响。
 * 隔离影响沿批内传播：前项条目命中隔离来源时，其目标瓶视为受影响，
 * 后续引用该瓶的条目一并拦截（提交时整批回滚，不落库）。返回 Map(index -> errors)。
 */
function validateBatchItems(items) {
  const errMap = new Map();
  const pushErr = (i, code, message) => {
    if (!errMap.has(i)) errMap.set(i, []);
    errMap.get(i).push({ code, message });
  };
  const opFirst = new Map();     // 批内操作号 -> 首次出现的条目下标
  const targetFirst = new Map(); // 批内目标瓶 -> 生成它的条目下标
  items.forEach((it, i) => {
    if (it.op_id && !opFirst.has(it.op_id)) opFirst.set(it.op_id, i);
    if (it.target && !targetFirst.has(it.target)) targetFirst.set(it.target, i);
  });
  const quarImpact = quarantineImpactMap(); // 当前台账中受隔离影响的瓶（含隔离原瓶自身）
  const batchHit = new Map();               // 批内受影响目标瓶 -> 使其受影响的条目下标
  items.forEach((it, i) => {
    if (!it.op_id || !it.target || !it.op_time || it.sources.length === 0) return; // 格式错误已记录
    const n = i + 1;
    if (qOp.get(it.op_id)) pushErr(i, 'OP_CONFLICT', `第 ${n} 条：操作号 ${it.op_id} 已存在（与现有台账冲突）`);
    if (opFirst.get(it.op_id) !== i) pushErr(i, 'OP_DUP_IN_BATCH', `第 ${n} 条：操作号 ${it.op_id} 与第 ${opFirst.get(it.op_id) + 1} 条重复`);
    if (qBottle.get(it.target)) pushErr(i, 'BOTTLE_CONFLICT', `第 ${n} 条：目标瓶编号 ${it.target} 已存在（编号冲突）`);
    if (targetFirst.get(it.target) !== i) pushErr(i, 'TARGET_DUP_IN_BATCH', `第 ${n} 条：目标瓶 ${it.target} 与第 ${targetFirst.get(it.target) + 1} 条重复`);
    let itemHit = false; // 本条目是否命中隔离（命中则其目标瓶在批内视为受影响，向后传播）
    for (const s of it.sources) {
      const b = qBottle.get(s.bottle);
      if (b) {
        if (it.op_time < b.created_time) {
          pushErr(i, 'TIME_BEFORE_SOURCE', `第 ${n} 条：操作时间 ${it.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${b.created_time}`);
        }
        if (quarImpact.has(s.bottle)) {
          pushErr(i, 'QUARANTINED_SOURCE',
            `第 ${n} 条：来源瓶 ${s.bottle} 受隔离影响（命中隔离原瓶 ${hitsText(quarImpact.get(s.bottle))}），不得用于新分装 / 混样`);
          itemHit = true;
        }
        continue;
      }
      const j = targetFirst.get(s.bottle);
      if (j === undefined) {
        pushErr(i, 'SOURCE_NOT_FOUND', `第 ${n} 条：来源瓶 ${s.bottle} 不存在（台账与批内前项中均无）`);
      } else if (j >= i) {
        pushErr(i, 'SOURCE_NOT_YET_CREATED', `第 ${n} 条：来源瓶 ${s.bottle} 由本批第 ${j + 1} 条生成，只能引用录入顺序在前的条目生成的瓶`);
      } else {
        if (it.op_time < items[j].op_time) {
          pushErr(i, 'TIME_BEFORE_SOURCE', `第 ${n} 条：操作时间 ${it.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${items[j].op_time}（本批第 ${j + 1} 条生成）`);
        }
        if (batchHit.has(s.bottle)) {
          pushErr(i, 'QUARANTINED_SOURCE',
            `第 ${n} 条：来源瓶 ${s.bottle} 由本批第 ${batchHit.get(s.bottle) + 1} 条生成，该前项受隔离影响，本条目一并拦截`);
          itemHit = true;
        }
      }
    }
    if (itemHit) batchHit.set(it.target, i);
  });
  return errMap;
}

/**
 * 模拟重放：把批次条目作为虚拟操作（虚拟入库顺序 = 当前最大 seq 之后按录入顺序
 * 连续分配，与提交时的真实行为一致）追加到当前台账末尾，按 (操作时间, 入库顺序)
 * 全量重放。excludeIdx 中的条目（格式非法、无法模拟）不参与。
 * 返回 { conflicts, projected }，projected 为受影响瓶（批内来源 ∪ 目标）的预计余量。
 */
function simulateBatch(items, excludeIdx) {
  const ledger = loadLedger();
  const baseSeq = ledger.ops.reduce((m, o) => Math.max(m, o.seq), 0);
  const vBottles = [];
  const vOps = [];
  items.forEach((it, i) => {
    if (excludeIdx.has(i)) return;
    vBottles.push({ id: it.target, kind: 'derived', initial_volume: 0 });
    vOps.push({
      op_id: it.op_id, seq: baseSeq + vOps.length + 1, target_bottle: it.target, op_time: it.op_time,
      sources: it.sources.map((s) => ({ bottle: s.bottle, volume: s.volume })),
    });
  });
  const allOps = [...ledger.ops, ...vOps].sort(cmpOps);
  const { balances, conflicts } = replayLedger([...ledger.bottles, ...vBottles], allOps);
  const affected = new Set();
  items.forEach((it, i) => {
    if (excludeIdx.has(i)) return;
    affected.add(it.target);
    for (const s of it.sources) affected.add(s.bottle);
  });
  const projected = [...affected].sort().map((id) => ({
    bottle: id,
    in_batch: vBottles.some((b) => b.id === id),
    remaining: balances.get(id) ?? 0,
  }));
  return { conflicts, projected };
}

/** 批次计划的规范化内容哈希：绑定批次号与全部条目，用于凭据与幂等判定。 */
function planHashOf(batchId, items) {
  const canonical = JSON.stringify({
    batch_id: batchId,
    items: items.map((it) => ({
      op_id: it.op_id, target_bottle: it.target, op_time: it.op_time,
      sources: it.sources.map((s) => ({ bottle: s.bottle, volume: s.volume })),
    })),
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

/** 提交凭据：绑定 计划哈希 + 当前台账版本，HMAC 签名防篡改。 */
function signCommitToken(version, planHash) {
  const mac = crypto.createHmac('sha256', TOKEN_SECRET).update(`v1.${version}.${planHash}`).digest('hex');
  return `v1.${version}.${planHash}.${mac}`;
}

function verifyCommitToken(token, planHash) {
  if (typeof token !== 'string' || !token) {
    throw new ApiError(400, 'TOKEN_REQUIRED', '缺少提交凭据 commit_token：请先调用 POST /api/batches/preview 预演');
  }
  const parts = token.split('.');
  if (parts.length !== 4 || parts[0] !== 'v1') {
    throw new ApiError(400, 'BAD_TOKEN', '提交凭据格式无效，请重新预演获取');
  }
  const [, ver, ph, mac] = parts;
  const expect = crypto.createHmac('sha256', TOKEN_SECRET).update(`v1.${ver}.${ph}`).digest('hex');
  const a = Buffer.from(mac); const b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    throw new ApiError(409, 'BAD_TOKEN', '提交凭据无效（可能已过期或被篡改），请重新预演');
  }
  if (ph !== planHash) {
    throw new ApiError(409, 'PLAN_CHANGED', '提交内容与预演时的计划不一致，请重新预演');
  }
  if (parseInt(ver, 10) !== ledgerVersion()) {
    throw new ApiError(409, 'STALE_TOKEN', '预演后台账已发生变化（有新登记 / 操作 / 更正 / 批次提交），预演结果已失效，请重新预演');
  }
}

/** 条目错误列表（按录入顺序），用于响应 details。 */
function itemErrorList(items, errMap) {
  return [...errMap.entries()]
    .sort((x, y) => x[0] - y[0])
    .map(([i, errors]) => ({ index: i, op_id: items[i] ? items[i].op_id : null, errors }));
}

/**
 * 提交批次（单事务）：幂等判定 → 核对凭据 → 重跑业务校验 → 落库 → 重放全账。
 * 任一环节失败整体回滚，整批不落库。
 */
const commitBatch = db.transaction((batchId, items, planHash, token) => {
  const existing = qBatch.get(batchId);
  if (existing) {
    if (existing.plan_hash === planHash) {
      return { ok: true, idempotent: true, batch_id: batchId, committed: existing.item_count,
               message: `批次 ${batchId} 已成功提交过相同内容，幂等返回，未重复扣量` };
    }
    throw new ApiError(409, 'BATCH_CONFLICT', `批次号 ${batchId} 已提交过不同内容，请更换批次号`);
  }
  verifyCommitToken(token, planHash);
  const biz = validateBatchItems(items);
  if (biz.size) {
    throw new ApiError(409, 'BATCH_ITEMS_INVALID', '批次条目校验失败，整批未提交', itemErrorList(items, biz));
  }
  qInsBatch.run(batchId, planHash, items.length, new Date().toISOString());
  items.forEach((it, i) => {
    qInsOp.run(it.op_id, qNextSeq.get().next, it.target, it.op_time);
    qInsBottle.run(it.target, 'derived', it.op_time, 0, it.op_id);
    for (const s of it.sources) qInsSource.run(it.op_id, s.bottle, s.volume);
    qInsBatchItem.run(batchId, i, it.op_id);
  });
  const { balances, conflicts } = recompute();
  if (conflicts.length) {
    const opToIdx = new Map(items.map((it, i) => [it.op_id, i]));
    throw new ApiError(409, 'OVERDRAW', '批次重放全账后出现超用，整批已回滚，数据未变更',
      conflicts.map((c) => ({ index: opToIdx.has(c.op_id) ? opToIdx.get(c.op_id) : null, ...c })));
  }
  qBumpVersion.run();
  const affected = new Set();
  for (const it of items) { affected.add(it.target); for (const s of it.sources) affected.add(s.bottle); }
  return {
    ok: true, idempotent: false, batch_id: batchId, committed: items.length,
    projected_balances: [...affected].sort().map((id) => ({ bottle: id, remaining: balances.get(id) ?? 0 })),
    message: `批次 ${batchId} 已提交：${items.length} 条操作已登记，相关瓶余量已重算`,
  };
});

// ---------------------------------------------------------------- 谱系查询

/** 完整来源谱系（向上递归到原始瓶），带深度保护。 */
function lineage(bottleId, depth = 0) {
  if (depth > 100) return { bottle: bottleId, truncated: true };
  const op = db.prepare('SELECT * FROM operations WHERE target_bottle = ?').get(bottleId);
  if (!op) return { bottle: bottleId, original: true };
  const srcs = qOpSources.all(op.op_id);
  return {
    bottle: bottleId,
    op: { op_id: op.op_id, op_time: op.op_time },
    sources: srcs.map((s) => ({ volume: s.volume, node: lineage(s.source_bottle, depth + 1) })),
  };
}

function opsInvolving(bottleId) {
  const asTarget = db.prepare('SELECT * FROM operations WHERE target_bottle = ?').all(bottleId);
  const asSource = db.prepare(`
    SELECT o.*, s.volume AS drawn_volume FROM operations o
    JOIN operation_sources s ON s.op_id = o.op_id
    WHERE s.source_bottle = ?`).all(bottleId);
  return { as_target: asTarget, as_source: asSource };
}

// ------------------------------------------------- 原瓶贡献量（精确分数）

/** 大整数最大公约数（返回正值；gcd(0,x)=x，全 0 时返回 1）。 */
function bgcd(a, b) {
  a = a < 0n ? -a : a;
  b = b < 0n ? -b : b;
  while (b) { const t = a % b; a = b; b = t; }
  return a || 1n;
}

/** 构造约分后的分数 { n, d }（BigInt，d 恒为正）。 */
function frac(n, d) {
  n = BigInt(n);
  d = BigInt(d);
  if (d === 0n) throw new Error('分数分母为 0');
  if (n === 0n) return { n: 0n, d: 1n };
  if (d < 0n) { n = -n; d = -d; }
  const g = bgcd(n, d);
  return { n: n / g, d: d / g };
}
const fadd = (a, b) => frac(a.n * b.d + b.n * a.d, a.d * b.d);
const fmul = (a, b) => frac(a.n * b.n, a.d * b.d);

/**
 * 分数 → API JSON：
 *  - numerator / denominator：约分后的分子、分母（字符串承载任意精度 BigInt）；
 *  - text：约分文本（分母为 1 时省略分母），单位 mL；
 *  - approx_ml：有限精度近似值（仅展示用，精确值以分子/分母为准）。
 */
function fracJson(f) {
  const safe = f.n <= BigInt(Number.MAX_SAFE_INTEGER) && f.d <= BigInt(Number.MAX_SAFE_INTEGER);
  return {
    numerator: f.n.toString(),
    denominator: f.d.toString(),
    text: f.d === 1n ? f.n.toString() : `${f.n.toString()}/${f.d.toString()}`,
    approx_ml: safe && f.n >= 0n ? Number(f.n) / Number(f.d) : null,
  };
}

/**
 * 构建原瓶贡献量索引。贡献量只取决于“瓶子形成图”（原瓶初始体积 + 衍生瓶创建操作
 * 的来源与取用量），与操作时序、当前余量无关：
 *  - 原瓶贡献 = 自身初始体积；
 *  - 衍生瓶由来源瓶 S 取 v mL 形成时，S 中各原瓶贡献按 v / V_S 分摊
 *    （V_S 为 S 形成时的总体积，内容视均匀）；
 *  - 同一原瓶经不同路径汇入时合并（精确分数相加后约分）；
 *  - 全程 BigInt 分数运算，不做逐层舍入，各项之和严格等于目标瓶形成体积；
 *  - 来源瓶之后的剩余量变化不改变已形成瓶子的配比。
 * 历史更正后调用方以最新台账重建索引，即完成所有下游贡献的重算；
 * 被守恒规则拒绝的更正已回滚，形成图不变，结果自然不变。
 */
function buildCompositionIndex(origMap, derivedMap) {
  const memo = new Map();
  const compose = (id, stack) => {
    const cached = memo.get(id);
    if (cached) return cached;
    if (origMap.has(id)) {
      const v = origMap.get(id);
      const r = { total: v, map: new Map([[id, frac(v, 1n)]]) };
      memo.set(id, r);
      return r;
    }
    const spec = derivedMap.get(id);
    if (!spec) {
      throw new ApiError(409, 'LINEAGE_BROKEN',
        `瓶 ${id} 的来源谱系不完整（缺少形成操作记录或原瓶登记），无法计算原瓶贡献量`);
    }
    if (stack.includes(id)) {
      throw new ApiError(409, 'LINEAGE_CYCLE',
        `瓶 ${id} 的来源谱系存在循环引用，无法计算原瓶贡献量`);
    }
    stack.push(id);
    const map = new Map();
    let total = 0n;
    for (const s of spec.sources) {
      total += s.volume;
      const sub = compose(s.bottle, stack);
      const factor = frac(s.volume, sub.total); // 分摊比例 = 取用量 / 来源瓶形成总体积
      for (const [origId, c] of sub.map) {
        const share = fmul(c, factor);
        map.set(origId, map.has(origId) ? fadd(map.get(origId), share) : share);
      }
    }
    stack.pop();
    const r = { total, map };
    memo.set(id, r);
    return r;
  };
  return {
    /** 返回 { total: BigInt 形成总体积, map: 原瓶编号 -> 约分分数, sum: 各项合计分数 }。 */
    get(id) {
      const r = compose(id, []);
      const sum = [...r.map.values()].reduce((acc, c) => fadd(acc, c), frac(0n, 1n));
      return { total: r.total, map: r.map, sum };
    },
  };
}

/** 从当前已提交台账加载形成图：原瓶初始体积 + 每个衍生瓶创建操作的来源取用量。 */
function loadCompositionMaps() {
  const origMap = new Map();
  db.prepare("SELECT id, initial_volume FROM bottles WHERE kind = 'original'").all()
    .forEach((b) => origMap.set(b.id, BigInt(b.initial_volume)));
  const byOp = new Map();
  db.prepare('SELECT op_id, source_bottle, volume FROM operation_sources ORDER BY source_bottle').all()
    .forEach((s) => {
      if (!byOp.has(s.op_id)) byOp.set(s.op_id, []);
      byOp.get(s.op_id).push({ bottle: s.source_bottle, volume: BigInt(s.volume) });
    });
  const derivedMap = new Map(); // 每个目标瓶只有唯一创建操作（编号冲突在写入时拦截）
  db.prepare('SELECT op_id, target_bottle FROM operations ORDER BY seq').all().forEach((o) => {
    if (!derivedMap.has(o.target_bottle)) {
      derivedMap.set(o.target_bottle, { op_id: o.op_id, sources: byOp.get(o.op_id) || [] });
    }
  });
  return { origMap, derivedMap };
}

/** 当前台账的贡献量索引（查询 / 更正后重算下游均用此入口）。 */
function ledgerCompositionIndex() {
  const { origMap, derivedMap } = loadCompositionMaps();
  return buildCompositionIndex(origMap, derivedMap);
}

/** 组装贡献量查询响应体：原瓶编号、精确贡献量、形成总体积与各项合计。 */
function contributionReport(index, bottleRow) {
  const { total, map, sum } = index.get(bottleRow.id);
  return {
    bottle: bottleRow.id,
    kind: bottleRow.kind,
    created_by_op: bottleRow.created_op || null,
    formation_volume: Number(total), // 取用量均为整数，形成体积为整数 mL
    contributions: [...map.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([origId, c]) => ({ bottle: origId, contribution_ml: fracJson(c) })),
    sum_ml: fracJson(sum),
  };
}

// ------------------------------------------------- 更正预演 / 确认（两阶段）

/** 更正计划的规范化内容哈希：绑定操作号与拟替换的来源列表（顺序敏感，确认内容须与预演严格一致）。 */
function correctionPlanHash(opId, sources) {
  const canonical = JSON.stringify({
    op_id: opId,
    sources: sources.map((s) => ({ bottle: s.bottle, volume: s.volume })),
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

/** 更正确认凭据：绑定 更正计划哈希 + 当前台账版本，HMAC 签名防篡改（域前缀 c1 与批次凭据 v1 隔离）。 */
function signCorrectionToken(version, planHash) {
  const mac = crypto.createHmac('sha256', TOKEN_SECRET).update(`c1.${version}.${planHash}`).digest('hex');
  return `c1.${version}.${planHash}.${mac}`;
}

function verifyCorrectionToken(token, planHash) {
  if (typeof token !== 'string' || !token) {
    throw new ApiError(400, 'TOKEN_REQUIRED', '缺少确认凭据 confirm_token：请先调用 POST /api/operations/:op_id/corrections/preview 预演');
  }
  const parts = token.split('.');
  if (parts.length !== 4 || parts[0] !== 'c1') {
    throw new ApiError(400, 'BAD_TOKEN', '确认凭据格式无效，请重新预演获取');
  }
  const [, ver, ph, mac] = parts;
  const expect = crypto.createHmac('sha256', TOKEN_SECRET).update(`c1.${ver}.${ph}`).digest('hex');
  const a = Buffer.from(mac); const b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    throw new ApiError(409, 'BAD_TOKEN', '确认凭据无效（可能已过期或被篡改），请重新预演');
  }
  if (ph !== planHash) {
    throw new ApiError(409, 'PLAN_CHANGED', '确认内容与预演时的更正计划不一致，请重新预演');
  }
  if (parseInt(ver, 10) !== ledgerVersion()) {
    throw new ApiError(409, 'STALE_TOKEN', '预演后台账已发生变化（有新登记 / 操作 / 更正 / 批次提交），预演结果已失效，请重新预演');
  }
}

/** 由形成图（目标瓶 → 来源列表）计算 bottleId 的全部下游瓶（直接或间接以其为来源的衍生瓶，不含自身）。 */
function downstreamBottles(derivedMap, bottleId) {
  const consumers = new Map(); // 来源瓶 -> [以其为来源的衍生瓶]
  for (const [target, spec] of derivedMap) {
    for (const s of spec.sources) {
      if (!consumers.has(s.bottle)) consumers.set(s.bottle, []);
      consumers.get(s.bottle).push(target);
    }
  }
  const seen = new Set([bottleId]);
  const queue = [bottleId];
  while (queue.length) {
    const cur = queue.shift();
    for (const nxt of consumers.get(cur) || []) {
      if (!seen.has(nxt)) { seen.add(nxt); queue.push(nxt); }
    }
  }
  seen.delete(bottleId);
  return [...seen].sort();
}

/** 贡献报告的可比较签名：形成时总体积 + 各原瓶约分贡献（报告内已按原瓶编号排序）。 */
function contributionSignature(rep) {
  return rep.formation_volume + '|' + rep.contributions
    .map((c) => `${c.bottle}=${c.contribution_ml.numerator}/${c.contribution_ml.denominator}`)
    .join('|');
}

/**
 * 预演更正（不写库）：把操作 opId 的来源替换为 newSources 后，
 *  - 按 (操作时间, 入库顺序) 重放全账，给出余量发生变化的瓶（before/after）与超用冲突；
 *  - 在更正前后的形成图上，给出目标瓶及其下游瓶原瓶精确贡献的前后对照（无变化不列出）。
 * 调用方须先完成来源存在性与时间先后校验。返回 { conflicts, balance_changes, contribution_changes }。
 */
function simulateCorrection(opId, newSources) {
  const { bottles, ops } = loadLedger();
  const before = replayLedger(bottles, ops).balances;
  const afterOps = ops.map((o) => (o.op_id === opId
    ? { ...o, sources: newSources.map((s) => ({ bottle: s.bottle, volume: s.volume })) }
    : o));
  const afterReplay = replayLedger(bottles, afterOps);
  if (afterReplay.conflicts.length) {
    return { conflicts: afterReplay.conflicts, balance_changes: [], contribution_changes: [] };
  }
  const balanceChanges = [];
  for (const [id, b] of before) {
    const a = afterReplay.balances.get(id) ?? 0;
    if (a !== b) balanceChanges.push({ bottle: id, before: b, after: a });
  }
  balanceChanges.sort((x, y) => (x.bottle < y.bottle ? -1 : x.bottle > y.bottle ? 1 : 0));

  const { origMap, derivedMap } = loadCompositionMaps();
  const op = qOp.get(opId);
  const correctedMap = new Map(derivedMap);
  correctedMap.set(op.target_bottle, {
    op_id: opId,
    sources: newSources.map((s) => ({ bottle: s.bottle, volume: BigInt(s.volume) })),
  });
  const idxBefore = buildCompositionIndex(origMap, derivedMap);
  const idxAfter = buildCompositionIndex(origMap, correctedMap);
  const rows = new Map(db.prepare('SELECT id, kind, created_op FROM bottles').all().map((b) => [b.id, b]));
  const affected = [op.target_bottle, ...downstreamBottles(derivedMap, op.target_bottle)];
  const contributionChanges = [];
  for (const id of affected) {
    const bRep = contributionReport(idxBefore, rows.get(id));
    const aRep = contributionReport(idxAfter, rows.get(id));
    if (contributionSignature(bRep) !== contributionSignature(aRep)) {
      contributionChanges.push({
        bottle: id,
        before: { formation_volume: bRep.formation_volume, contributions: bRep.contributions, sum_ml: bRep.sum_ml },
        after: { formation_volume: aRep.formation_volume, contributions: aRep.contributions, sum_ml: aRep.sum_ml },
      });
    }
  }
  return { conflicts: [], balance_changes: balanceChanges, contribution_changes: contributionChanges };
}

/**
 * 更正确认（单事务）：同一凭据已确认 → 幂等返回首次结果；否则
 * 核对凭据（计划一致 + 台账版本未变）→ 重跑业务校验 → 落库 → 重放全账，
 * 任一环节失败整体回滚。版本未变即台账与预演时一致，故差异快照与预演结果相同。
 */
const confirmCorrection = db.transaction((opId, sources, token) => {
  const planHash = correctionPlanHash(opId, sources);
  const tokenKey = crypto.createHash('sha256').update(String(token)).digest('hex');
  const prior = qCorrConfirmGet.get(tokenKey);
  if (prior) {
    if (prior.op_id !== opId || prior.plan_hash !== planHash) {
      throw new ApiError(409, 'PLAN_CHANGED', '确认内容与该凭据首次确认的更正计划不一致，请重新预演');
    }
    return { ...JSON.parse(prior.response), idempotent: true,
             message: `更正凭据已于 ${prior.confirmed_at} 确认过，幂等返回首次结果，未重复更正` };
  }
  verifyCorrectionToken(token, planHash);
  const op = qOp.get(opId);
  if (!op) throw new ApiError(404, 'OP_NOT_FOUND', `操作 ${opId} 不存在`);
  for (const s of sources) {
    const b = qBottle.get(s.bottle);
    if (!b) throw new ApiError(400, 'SOURCE_NOT_FOUND', `来源瓶 ${s.bottle} 不存在`);
    if (op.op_time < b.created_time) {
      throw new ApiError(400, 'TIME_BEFORE_SOURCE',
        `操作时间 ${op.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${b.created_time}`);
    }
  }
  const diff = simulateCorrection(opId, sources); // 落库前计算，与预演结果一致
  qDelSources.run(opId);
  for (const s of sources) qInsSource.run(opId, s.bottle, s.volume);
  const { conflicts } = recompute();
  if (conflicts.length) {
    throw new ApiError(409, 'OVERDRAW', '更正将导致以下操作超用，整次更正已拒绝，原数据保持不变', conflicts);
  }
  qBumpVersion.run();
  const response = {
    ok: true, idempotent: false, op_id: opId,
    balance_changes: diff.balance_changes,
    contribution_changes: diff.contribution_changes,
    ledger_version: ledgerVersion(),
    message: `操作 ${opId} 的更正已确认并原子应用：${diff.balance_changes.length} 个瓶余量、${diff.contribution_changes.length} 个瓶的原瓶贡献已按预演结果重算`,
  };
  qCorrConfirmIns.run(tokenKey, opId, planHash, JSON.stringify(response), new Date().toISOString());
  return response;
});

// ---------------------------------------------------------------- 证据封存（质检复核）

/** 规范化 JSON（对象键序稳定），用于内容摘要，保证同一证据内容的摘要恒定。 */
function canonicalJson(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return '[' + v.map(canonicalJson).join(',') + ']';
  return '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonicalJson(v[k])).join(',') + '}';
}
const sha256Hex = (s) => crypto.createHash('sha256').update(s).digest('hex');

/**
 * 收集目标瓶向上至原瓶的全部形成操作（扁平化）及来源链上的全部瓶编号。
 * 返回 { bottles: [...含目标瓶与全部祖先，按编号排序...], ops: [...按 (操作时间, 操作号) 排序...] }。
 * 谱系不完整（来源瓶未登记 / 衍生瓶缺少形成操作）时抛错，由调用方按“取证失败”处理（不占号）；
 * 循环引用由贡献量索引（buildCompositionIndex）在后续构建证据时检出。
 */
function collectFormation(bottleId) {
  const bottles = new Set();
  const ops = [];
  const queue = [bottleId];
  while (queue.length) {
    const cur = queue.shift();
    if (bottles.has(cur)) continue;
    bottles.add(cur);
    const b = qBottle.get(cur);
    if (!b) {
      throw new ApiError(409, 'LINEAGE_BROKEN',
        `瓶 ${cur} 的来源谱系不完整（来源瓶未登记），无法固化形成证据`);
    }
    if (b.kind === 'original') continue;
    const op = db.prepare('SELECT op_id, op_time, target_bottle FROM operations WHERE target_bottle = ?').get(cur);
    if (!op) {
      throw new ApiError(409, 'LINEAGE_BROKEN',
        `瓶 ${cur} 的来源谱系不完整（缺少形成操作记录），无法固化形成证据`);
    }
    const srcs = qOpSources.all(op.op_id);
    ops.push({
      op_id: op.op_id, op_time: op.op_time, target_bottle: op.target_bottle,
      sources: srcs.map((s) => ({ bottle: s.source_bottle, volume: s.volume })),
    });
    for (const s of srcs) queue.push(s.source_bottle);
  }
  ops.sort((a, b) => (a.op_time < b.op_time ? -1 : a.op_time > b.op_time ? 1
    : a.op_id < b.op_id ? -1 : a.op_id > b.op_id ? 1 : 0));
  return { bottles: [...bottles].sort(), ops };
}

/**
 * 从当前已提交台账状态构建目标瓶的形成证据。封存与“与现况比较”共用同一构建器，
 * 保证比较时重算的口径与封存时完全一致。证据内容：
 *  - lineage：向上至原瓶的完整来源谱系（树）；
 *  - formation_ops：各形成操作的操作号、时间、目标瓶、来源瓶与取用量（扁平化）；
 *  - balances：目标瓶及来源链各瓶的当时余量；
 *  - contributions：精确原瓶贡献（沿用约分分数 mL 格式）；
 *  - quarantine：目标瓶自身隔离状态与命中的隔离原瓶及原因。
 * 瓶不存在 → 404；谱系不完整 / 循环 → 409（取证失败，调用方不得占号）。
 */
function buildEvidence(bottleId) {
  const b = qBottle.get(bottleId);
  if (!b) throw new ApiError(404, 'BOTTLE_NOT_FOUND', `瓶 ${bottleId} 不存在`);
  const { bottles: chainBottles, ops } = collectFormation(bottleId);
  const { balances } = recompute();
  const impact = quarantineImpactMap();
  const quar = qQuarGet.get(bottleId);
  return {
    bottle: b.id,
    kind: b.kind,
    created_time: b.created_time,
    created_by_op: b.created_op || null,
    ledger_version: ledgerVersion(),
    lineage: lineage(bottleId),
    formation_ops: ops,
    balances: chainBottles.map((id) => ({ bottle: id, remaining: balances.get(id) ?? 0 })),
    contributions: contributionReport(ledgerCompositionIndex(), b),
    quarantine: {
      self: quar ? { reason: quar.reason, updated_at: quar.updated_at } : null,
      affected: impact.has(bottleId),
      hits: impact.get(bottleId) || [],
    },
  };
}

/** 封存记录行 → API 响应（evidence 原样解析返回）。 */
function sealRecordJson(row) {
  return {
    seal_id: row.seal_id,
    bottle: row.bottle_id,
    sealed_at: row.sealed_at,
    ledger_version: row.ledger_version,
    digest: row.digest,
    evidence: JSON.parse(row.evidence),
  };
}

/**
 * 新建证据封存（事务）：在同一已提交台账状态下固化目标瓶的形成证据，
 * 返回封存时间与内容摘要（证据规范化 JSON 的 SHA-256）。
 * 只写 evidence_seals 表：不修改台账、不重算余量、不递增台账版本，
 * 已发放的批次提交凭据与更正确认凭据不受影响。
 * 同号同目标 → 幂等返回原封存；同号异目标 → 409；
 * 目标不存在或取证失败 → 抛错回滚，该封存号不被占用。
 */
const createSeal = db.transaction((sealId, bottleId) => {
  const existing = qSealGet.get(sealId);
  if (existing) {
    if (existing.bottle_id === bottleId) {
      return { ...sealRecordJson(existing), ok: true, idempotent: true,
               message: `封存号 ${sealId} 已对瓶 ${bottleId} 封存过（封存时间 ${existing.sealed_at}），幂等返回原封存，未重新取证` };
    }
    throw new ApiError(409, 'SEAL_CONFLICT',
      `封存号 ${sealId} 已用于瓶 ${existing.bottle_id}，不能再对瓶 ${bottleId} 封存，请更换封存号`);
  }
  const evidence = buildEvidence(bottleId); // 目标不存在 / 取证失败在此抛错，事务回滚不占号
  const sealedAt = new Date().toISOString();
  const digest = sha256Hex(canonicalJson(evidence));
  qSealIns.run(sealId, bottleId, evidence.ledger_version, sealedAt, digest, JSON.stringify(evidence));
  return {
    ok: true, idempotent: false, seal_id: sealId, bottle: bottleId,
    sealed_at: sealedAt, ledger_version: evidence.ledger_version, digest, evidence,
    message: `瓶 ${bottleId} 的形成证据已封存（封存号 ${sealId}，台账版本 ${evidence.ledger_version}）；封存不改变台账与余量，内容随后不可改写`,
  };
});

/**
 * 逐项比较已封存证据与按当前台账重算的证据：来源关系（形成操作的来源边）、取用量、
 * 余量、原瓶贡献、隔离命中的新增（added）/ 消失（removed）/ 变化（changed）；
 * 未变化项不列出。返回的 changes 为空对象即“与现况一致”。
 */
function diffEvidence(sealed, current) {
  const changes = {};
  const byKey = (arr, keyFn) => { const m = new Map(); for (const x of arr) m.set(keyFn(x), x); return m; };
  const sortBy = (arr, ...keys) => arr.sort((a, b) => {
    for (const k of keys) { if (a[k] < b[k]) return -1; if (a[k] > b[k]) return 1; }
    return 0;
  });

  // —— 来源关系（形成操作 → 来源瓶 的边）与取用量 ——
  const edgesOf = (ev) => {
    const m = new Map();
    for (const op of ev.formation_ops) {
      for (const s of op.sources) {
        m.set(op.op_id + '→' + s.bottle, {
          op_id: op.op_id, target_bottle: op.target_bottle, source_bottle: s.bottle, volume: s.volume,
        });
      }
    }
    return m;
  };
  const e0 = edgesOf(sealed); const e1 = edgesOf(current);
  const linAdded = []; const linRemoved = []; const volChanged = [];
  for (const [k, e] of e1) { if (!e0.has(k)) linAdded.push(e); }
  for (const [k, e] of e0) {
    const now = e1.get(k);
    if (!now) linRemoved.push(e);
    else if (now.volume !== e.volume) {
      volChanged.push({ op_id: e.op_id, target_bottle: e.target_bottle, source_bottle: e.source_bottle, before: e.volume, after: now.volume });
    }
  }
  sortBy(linAdded, 'op_id', 'source_bottle'); sortBy(linRemoved, 'op_id', 'source_bottle'); sortBy(volChanged, 'op_id', 'source_bottle');
  const lineageCh = {};
  if (linAdded.length) lineageCh.added = linAdded;
  if (linRemoved.length) lineageCh.removed = linRemoved;
  if (Object.keys(lineageCh).length) changes.lineage = lineageCh;
  if (volChanged.length) changes.volumes = { changed: volChanged };

  // —— 余量（目标瓶及来源链各瓶）——
  const b0 = byKey(sealed.balances, (x) => x.bottle);
  const b1 = byKey(current.balances, (x) => x.bottle);
  const balAdded = []; const balRemoved = []; const balChanged = [];
  for (const [id, x] of b1) { if (!b0.has(id)) balAdded.push({ bottle: id, after: x.remaining }); }
  for (const [id, x] of b0) {
    const now = b1.get(id);
    if (!now) balRemoved.push({ bottle: id, before: x.remaining });
    else if (now.remaining !== x.remaining) balChanged.push({ bottle: id, before: x.remaining, after: now.remaining });
  }
  sortBy(balAdded, 'bottle'); sortBy(balRemoved, 'bottle'); sortBy(balChanged, 'bottle');
  const balCh = {};
  if (balAdded.length) balCh.added = balAdded;
  if (balRemoved.length) balCh.removed = balRemoved;
  if (balChanged.length) balCh.changed = balChanged;
  if (Object.keys(balCh).length) changes.balances = balCh;

  // —— 精确原瓶贡献（约分分数 mL；按分子 / 分母精确比较）——
  const c0 = byKey(sealed.contributions.contributions, (x) => x.bottle);
  const c1 = byKey(current.contributions.contributions, (x) => x.bottle);
  const sameFrac = (a, b) => a.numerator === b.numerator && a.denominator === b.denominator;
  const conAdded = []; const conRemoved = []; const conChanged = [];
  for (const [id, x] of c1) { if (!c0.has(id)) conAdded.push({ bottle: id, contribution_ml: x.contribution_ml }); }
  for (const [id, x] of c0) {
    const now = c1.get(id);
    if (!now) conRemoved.push({ bottle: id, contribution_ml: x.contribution_ml });
    else if (!sameFrac(now.contribution_ml, x.contribution_ml)) {
      conChanged.push({ bottle: id, before: x.contribution_ml, after: now.contribution_ml });
    }
  }
  sortBy(conAdded, 'bottle'); sortBy(conRemoved, 'bottle'); sortBy(conChanged, 'bottle');
  const conCh = {};
  if (conAdded.length) conCh.added = conAdded;
  if (conRemoved.length) conCh.removed = conRemoved;
  if (conChanged.length) conCh.changed = conChanged;
  if (sealed.contributions.formation_volume !== current.contributions.formation_volume) {
    conCh.formation_volume = { before: sealed.contributions.formation_volume, after: current.contributions.formation_volume };
  }
  if (Object.keys(conCh).length) changes.contributions = conCh;

  // —— 隔离命中（自身隔离状态 + 命中的隔离原瓶及原因）——
  const quarCh = {};
  const selfBefore = sealed.quarantine.self ? sealed.quarantine.self.reason : null;
  const selfAfter = current.quarantine.self ? current.quarantine.self.reason : null;
  if (selfBefore !== selfAfter) quarCh.self = { before: selfBefore, after: selfAfter };
  const h0 = byKey(sealed.quarantine.hits, (x) => x.bottle);
  const h1 = byKey(current.quarantine.hits, (x) => x.bottle);
  const hitAdded = []; const hitRemoved = []; const hitChanged = [];
  for (const [id, x] of h1) { if (!h0.has(id)) hitAdded.push({ bottle: id, reason: x.reason }); }
  for (const [id, x] of h0) {
    const now = h1.get(id);
    if (!now) hitRemoved.push({ bottle: id, reason: x.reason });
    else if (now.reason !== x.reason) hitChanged.push({ bottle: id, before_reason: x.reason, after_reason: now.reason });
  }
  sortBy(hitAdded, 'bottle'); sortBy(hitRemoved, 'bottle'); sortBy(hitChanged, 'bottle');
  const hitsCh = {};
  if (hitAdded.length) hitsCh.added = hitAdded;
  if (hitRemoved.length) hitsCh.removed = hitRemoved;
  if (hitChanged.length) hitsCh.changed = hitChanged;
  if (Object.keys(hitsCh).length) quarCh.hits = hitsCh;
  if (Object.keys(quarCh).length) changes.quarantine = quarCh;

  return changes;
}

// ---------------------------------------------------------------- HTTP 层

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const wrap = (fn) => (req, res) => {
  try {
    fn(req, res);
  } catch (e) {
    if (e instanceof ApiError) {
      return res.status(e.status).json({ error: { code: e.code, message: e.message, details: e.details } });
    }
    console.error(e);
    res.status(500).json({ error: { code: 'INTERNAL', message: '服务器内部错误' } });
  }
};

/** 登记原始瓶（事务）：提交后台账版本递增，已发放的批次凭据随之失效。 */
const createBottle = db.transaction((id, t, v) => {
  if (qBottle.get(id)) throw new ApiError(409, 'BOTTLE_CONFLICT', `瓶编号 ${id} 已存在（编号冲突）`);
  qInsBottle.run(id, 'original', t, v, null);
  qBumpVersion.run();
  return { ok: true, message: `原始瓶 ${id} 已登记` };
});

app.post('/api/bottles', wrap((req, res) => {
  const id = normId(req.body.bottle_id, '瓶编号');
  const t = normTime(req.body.sample_time, '采样时间');
  const v = normVolume(req.body.initial_volume, '初始体积');
  res.status(201).json(createBottle(id, t, v));
}));

app.post('/api/operations', wrap((req, res) => {
  const p = {
    op_id: normId(req.body.op_id, '操作号'),
    target: normId(req.body.target_bottle, '目标瓶编号'),
    op_time: normTime(req.body.op_time, '操作时间'),
    sources: parseSources(req.body.sources),
  };
  const result = createOperation(p);
  res.status(result.idempotent ? 200 : 201).json(result);
}));

app.post('/api/operations/:opId/correct', wrap((req, res) => {
  const sources = parseSources(req.body.sources);
  res.json(correctOperation(req.params.opId, sources));
}));

/**
 * 更正预演（不写库）：提交操作号与拟替换的来源瓶、取用毫升数，返回该更正对当前台账的影响
 * （因重放而变化的瓶余量 + 目标瓶及其下游瓶更正前后的原瓶精确贡献，无变化项不列出）
 * 与确认凭据。操作号 / 来源不存在、来源重复、体积无效、时间早于来源创建、重放后任一操作
 * 超用时，返回具体原因及冲突明细，不签发确认凭据。
 */
app.post('/api/operations/:opId/corrections/preview', wrap((req, res) => {
  const opId = req.params.opId;
  const errors = [];
  let conflicts = [];
  const op = qOp.get(opId);
  if (!op) errors.push({ code: 'OP_NOT_FOUND', message: `操作 ${opId} 不存在` });
  const sources = [];
  const rawSrcs = req.body.sources;
  if (!Array.isArray(rawSrcs) || rawSrcs.length === 0) {
    errors.push({ code: 'NO_SOURCES', message: '每次操作至少需要一个来源瓶' });
  } else {
    const seen = new Set();
    rawSrcs.forEach((s, i) => {
      let bottle = null; let volume = null; let bad = false;
      try { bottle = normId(s && s.bottle, `来源瓶编号（第 ${i + 1} 行）`); } catch (e) { errors.push({ code: e.code, message: e.message }); bad = true; }
      try { volume = normVolume(s && s.volume, `取用体积（第 ${i + 1} 行）`); } catch (e) { errors.push({ code: e.code, message: e.message }); bad = true; }
      if (!bad) {
        if (seen.has(bottle)) errors.push({ code: 'DUP_SOURCE', message: `同一操作内来源瓶 ${bottle} 重复` });
        else { seen.add(bottle); sources.push({ bottle, volume }); }
      }
    });
  }
  let diff = { balance_changes: [], contribution_changes: [] };
  if (op && errors.length === 0) {
    for (const s of sources) {
      const b = qBottle.get(s.bottle);
      if (!b) { errors.push({ code: 'SOURCE_NOT_FOUND', message: `来源瓶 ${s.bottle} 不存在` }); continue; }
      if (op.op_time < b.created_time) {
        errors.push({ code: 'TIME_BEFORE_SOURCE',
          message: `操作时间 ${op.op_time} 早于来源瓶 ${s.bottle} 的创建时间 ${b.created_time}` });
      }
    }
    if (errors.length === 0) {
      const sim = simulateCorrection(opId, sources);
      conflicts = sim.conflicts;
      for (const c of conflicts) {
        errors.push({ code: 'OVERDRAW',
          message: `更正将导致操作 ${c.op_id} 从瓶 ${c.bottle} 需取 ${c.requested} mL，该时刻可用仅 ${c.available} mL` });
      }
      if (errors.length === 0) {
        diff = { balance_changes: sim.balance_changes, contribution_changes: sim.contribution_changes };
      }
    }
  }
  const ok = errors.length === 0;
  const version = ledgerVersion();
  res.json({
    ok,
    op_id: opId,
    plan_hash: correctionPlanHash(opId, sources),
    ledger_version: version,
    errors,
    conflicts,
    balance_changes: diff.balance_changes,
    contribution_changes: diff.contribution_changes,
    confirm_token: ok ? signCorrectionToken(version, correctionPlanHash(opId, sources)) : null,
    message: ok
      ? `预演通过：更正 ${opId} 将变化 ${diff.balance_changes.length} 个瓶余量、${diff.contribution_changes.length} 个瓶的原瓶贡献；凭据已绑定当前台账（版本 ${version}），确认前若台账变化需重新预演`
      : '预演未通过：请修正下列问题后重新预演（未写入任何数据，未签发确认凭据）',
  });
}));

/**
 * 更正确认（单事务，原子应用或整体回滚）：确认内容须与预演完全相同且台账版本未变，
 * 成功时应用结果与预演一致；同一凭据重试 → 200 幂等返回首次结果，不重复更正。
 */
app.post('/api/operations/:opId/corrections/confirm', wrap((req, res) => {
  const sources = parseSources(req.body.sources);
  const result = confirmCorrection(req.params.opId, sources, req.body.confirm_token);
  res.status(result.idempotent ? 200 : 201).json(result);
}));

app.get('/api/bottles', wrap((req, res) => {
  const { balances } = recompute();
  const impact = quarantineImpactMap();
  const quarMap = new Map(qQuarAll.all().map((q) => [q.bottle_id, q]));
  const rows = db.prepare('SELECT * FROM bottles ORDER BY created_time, id').all()
    .map((b) => ({
      ...b,
      remaining: balances.get(b.id) ?? 0,
      quarantine: quarMap.has(b.id)
        ? { reason: quarMap.get(b.id).reason, updated_at: quarMap.get(b.id).updated_at }
        : null,
      affected: impact.has(b.id),
      quarantine_hits: impact.get(b.id) || [],
    }));
  res.json(rows);
}));

app.get('/api/bottles/:id', wrap((req, res) => {
  const b = qBottle.get(req.params.id);
  if (!b) throw new ApiError(404, 'BOTTLE_NOT_FOUND', `瓶 ${req.params.id} 不存在`);
  const { balances } = recompute();
  const impact = quarantineImpactMap();
  const quar = qQuarGet.get(b.id);
  res.json({
    ...b,
    remaining: balances.get(b.id) ?? 0,
    quarantine: quar ? { reason: quar.reason, updated_at: quar.updated_at } : null,
    affected: impact.has(b.id),
    quarantine_hits: impact.get(b.id) || [],
    lineage: lineage(b.id),
    operations: opsInvolving(b.id),
    contributions: contributionReport(ledgerCompositionIndex(), b),
  });
}));

/**
 * 按瓶编号查询各原始瓶对该瓶的精确贡献量（分数 mL）：
 * 返回原瓶编号、约分后的精确贡献量、目标瓶形成时总体积与各项合计；
 * 原瓶 / 衍生瓶均可查询；瓶不存在 → 404 BOTTLE_NOT_FOUND。
 */
app.get('/api/bottles/:id/contributions', wrap((req, res) => {
  const b = qBottle.get(req.params.id);
  if (!b) throw new ApiError(404, 'BOTTLE_NOT_FOUND', `瓶 ${req.params.id} 不存在`);
  res.json(contributionReport(ledgerCompositionIndex(), b));
}));

// ---------------------------------------------------------------- 隔离 HTTP 接口

/**
 * 设置原瓶隔离：{reason} 非空。仅已登记原始瓶；新隔离 → 201，更新原因 → 200，
 * 已隔离且原因相同 → 200 幂等返回。状态变动使台账版本递增，
 * 未使用的批次提交凭据与更正确认凭据随之失效（需重新预演）。
 */
app.put('/api/bottles/:id/quarantine', wrap((req, res) => {
  const id = normId(req.params.id, '瓶编号');
  const reason = normId(req.body && req.body.reason, '隔离原因');
  const result = setQuarantine(id, reason);
  res.status(result.idempotent || result.updated ? 200 : 201).json(result);
}));

/** 解除原瓶隔离：未隔离 → 409 NOT_QUARANTINED；解除后台账版本递增。 */
app.delete('/api/bottles/:id/quarantine', wrap((req, res) => {
  res.json(liftQuarantine(normId(req.params.id, '瓶编号')));
}));

/** 当前全部隔离原瓶及原因。 */
app.get('/api/quarantines', wrap((req, res) => {
  res.json(qQuarAll.all().map((q) => ({ bottle: q.bottle_id, reason: q.reason, updated_at: q.updated_at })));
}));

/**
 * 隔离影响范围：按当前已提交来源关系列出受影响瓶（隔离原瓶自身 + 来源链含隔离原瓶的
 * 全部下游衍生瓶），每瓶给出命中的隔离原瓶及原因。查询时实时重建，故更正成功后
 * 影响范围自动重算；被守恒规则拒绝的更正已回滚，影响范围维持原状态。
 */
app.get('/api/quarantines/impact', wrap((req, res) => {
  const impact = quarantineImpactMap();
  const kinds = new Map(db.prepare('SELECT id, kind FROM bottles').all().map((b) => [b.id, b.kind]));
  res.json({
    ledger_version: ledgerVersion(),
    quarantined: qQuarAll.all().map((q) => ({ bottle: q.bottle_id, reason: q.reason, updated_at: q.updated_at })),
    affected_bottles: [...impact.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([id, hits]) => ({
        bottle: id,
        kind: kinds.get(id) || null,
        self_quarantined: hits.some((h) => h.bottle === id),
        hits,
      })),
  });
}));

app.get('/api/operations', wrap((req, res) => {
  const ops = db.prepare(`
    SELECT o.*, bi.batch_id AS batch_id FROM operations o
    LEFT JOIN batch_items bi ON bi.op_id = o.op_id
    ORDER BY o.op_time, o.seq`).all();
  const srcs = db.prepare('SELECT * FROM operation_sources ORDER BY source_bottle').all();
  const byOp = {};
  for (const s of srcs) (byOp[s.op_id] = byOp[s.op_id] || []).push({ bottle: s.source_bottle, volume: s.volume });
  res.json(ops.map((o) => {
    const list = byOp[o.op_id] || [];
    return { ...o, sources: list, total: list.reduce((a, s) => a + s.volume, 0) };
  }));
}));

// ---------------------------------------------------------------- 证据封存 HTTP 接口

/**
 * 新建证据封存：{seal_id, bottle}。在同一已提交台账状态下固化目标瓶的形成证据
 * （完整来源谱系、各形成操作时间与取用量、当时余量、精确原瓶贡献、隔离命中），
 * 返回封存时间与内容摘要（SHA-256）。封存不修改台账、不递增台账版本，
 * 已发放的批次提交 / 更正确认凭据不受影响。
 * 同号同目标 → 200 幂等返回原封存；同号异目标 → 409 SEAL_CONFLICT；
 * 目标不存在 → 404 BOTTLE_NOT_FOUND；取证失败（谱系不完整 / 循环）→ 409；失败均不占号。
 */
app.post('/api/seals', wrap((req, res) => {
  const sealId = normId(req.body && req.body.seal_id, '封存号');
  const bottleId = normId(req.body && req.body.bottle, '目标瓶编号');
  const result = createSeal(sealId, bottleId);
  res.status(result.idempotent ? 200 : 201).json(result);
}));

/** 全部封存摘要（不含证据正文）。 */
app.get('/api/seals', wrap((req, res) => {
  res.json(qSealAll.all().map((r) => ({
    seal_id: r.seal_id, bottle: r.bottle_id, sealed_at: r.sealed_at,
    ledger_version: r.ledger_version, digest: r.digest,
  })));
}));

/**
 * 按封存号查询原样证据。封存内容一旦落库不再更新：
 * 后续分装 / 混样、历史更正、隔离设置或解除都不会改写已封存证据。
 */
app.get('/api/seals/:sealId', wrap((req, res) => {
  const row = qSealGet.get(req.params.sealId);
  if (!row) throw new ApiError(404, 'SEAL_NOT_FOUND', `封存号 ${req.params.sealId} 不存在`);
  res.json(sealRecordJson(row));
}));

/**
 * 与现况比较：从同一当前台账状态用与封存相同的构建器重算目标瓶的形成证据，
 * 逐项列出来源关系、取用量、余量、原瓶贡献、隔离命中的新增 / 消失 / 变化；
 * 未变化项不列出，完全一致时 changed=false 且 changes={}。
 */
app.get('/api/seals/:sealId/compare', wrap((req, res) => {
  const row = qSealGet.get(req.params.sealId);
  if (!row) throw new ApiError(404, 'SEAL_NOT_FOUND', `封存号 ${req.params.sealId} 不存在`);
  const sealed = JSON.parse(row.evidence);
  const current = buildEvidence(row.bottle_id); // 与封存同一构建器、同一口径
  const changes = diffEvidence(sealed, current);
  const changed = Object.keys(changes).length > 0;
  res.json({
    seal_id: row.seal_id,
    bottle: row.bottle_id,
    sealed_at: row.sealed_at,
    digest: row.digest,
    sealed_ledger_version: row.ledger_version,
    current_ledger_version: ledgerVersion(),
    compared_at: new Date().toISOString(),
    changed,
    changes,
    message: changed
      ? '目标瓶现况与封存证据存在差异，详见 changes（仅列出新增 / 消失 / 变化项）'
      : '目标瓶现况与封存证据一致：来源关系、取用量、余量、原瓶贡献、隔离命中均无变化',
  });
}));

// ---------------------------------------------------------------- 批次 HTTP 接口

/**
 * 批次预演（不写库）：逐条可行性 + 受影响瓶预计余量 + 提交凭据。
 * 凭据绑定 批次计划哈希 与 当前台账版本；全部条目通过才发放。
 */
app.post('/api/batches/preview', wrap((req, res) => {
  const batchId = normId(req.body.batch_id, '批次号');
  const rawItems = req.body.items;
  if (!Array.isArray(rawItems) || rawItems.length === 0) {
    throw new ApiError(400, 'NO_ITEMS', '批次至少需要一条操作（items 为非空数组）');
  }
  const { items, itemErrors } = normalizeBatchItems(rawItems);
  const topErrors = [];
  const planHash = planHashOf(batchId, items);
  const existing = qBatch.get(batchId);
  if (existing) {
    topErrors.push(existing.plan_hash === planHash
      ? { code: 'BATCH_ALREADY_COMMITTED', message: `批次号 ${batchId} 已成功提交过相同内容，重复提交将幂等返回，不会重复扣量` }
      : { code: 'BATCH_CONFLICT', message: `批次号 ${batchId} 已提交过不同内容，请更换批次号` });
  }
  // 业务校验（编号冲突 / 来源存在性 / 时间先后），与提交共用同一实现
  const biz = validateBatchItems(items);
  for (const [i, errs] of biz) {
    if (!itemErrors.has(i)) itemErrors.set(i, []);
    itemErrors.get(i).push(...errs);
  }
  // 模拟重放：格式非法的条目无法参与，其余条目按计划追加到当前台账末尾
  const brokenIdx = new Set([...itemErrors.keys()].filter((i) => !items[i].op_id || !items[i].op_time || !items[i].target));
  const sim = simulateBatch(items, brokenIdx);
  const opToIdx = new Map();
  items.forEach((it, i) => { if (it.op_id && !opToIdx.has(it.op_id)) opToIdx.set(it.op_id, i); });
  for (const c of sim.conflicts) {
    const err = { code: 'OVERDRAW', message: `超用：从瓶 ${c.bottle} 需取 ${c.requested} mL，该时刻可用仅 ${c.available} mL` };
    if (opToIdx.has(c.op_id)) {
      const i = opToIdx.get(c.op_id);
      if (!itemErrors.has(i)) itemErrors.set(i, []);
      itemErrors.get(i).push(err);
    } else {
      topErrors.push({ code: 'LEDGER_CONFLICT', message: `现有台账操作 ${c.op_id} 从瓶 ${c.bottle} 需取 ${c.requested} mL，可用仅 ${c.available} mL` });
    }
  }
  const itemResults = items.map((it, i) => ({
    index: i, op_id: it.op_id, ok: !itemErrors.has(i), errors: itemErrors.get(i) || [],
  }));
  const ok = topErrors.length === 0 && itemResults.every((r) => r.ok);

  // 预计原瓶贡献：把格式完整的批次条目作为“虚拟形成图”叠加到当前台账之上
  // （只依赖来源与取用量，不依赖余量 / 时序），逐条计算可行条目目标瓶的预计贡献。
  // 该构图与提交后实际形成图一致，故提交后查询结果与预演必然相同；
  // 预演不写库，台账版本与凭据逻辑不受影响。
  const { origMap, derivedMap } = loadCompositionMaps();
  items.forEach((it, i) => {
    if (it.target && it.sources.length > 0 && !derivedMap.has(it.target)) {
      derivedMap.set(it.target, {
        op_id: it.op_id || `preview:${i}`,
        sources: it.sources.map((s) => ({ bottle: s.bottle, volume: BigInt(s.volume) })),
      });
    }
  });
  const projectedIndex = buildCompositionIndex(origMap, derivedMap);
  const projectedContributions = [];
  for (const r of itemResults) {
    if (!r.ok) continue;
    const it = items[r.index];
    try {
      const rep = contributionReport(projectedIndex, { id: it.target, kind: 'derived', created_op: it.op_id });
      projectedContributions.push({
        index: r.index, op_id: it.op_id, target_bottle: it.target,
        formation_volume: rep.formation_volume, contributions: rep.contributions, sum_ml: rep.sum_ml,
      });
    } catch (e) {
      // 引用了谱系不完整的批内瓶（如前项因来源不存在而不可行），该条目不展示预计贡献
    }
  }

  const version = ledgerVersion();
  res.json({
    ok,
    batch_id: batchId,
    plan_hash: planHash,
    ledger_version: version,
    errors: topErrors,
    items: itemResults,
    projected_balances: sim.projected,
    projected_contributions: projectedContributions,
    commit_token: ok ? signCommitToken(version, planHash) : null,
    message: ok
      ? `预演通过：批次 ${batchId} 共 ${items.length} 条操作可提交；凭据已绑定当前台账（版本 ${version}），提交前若台账变化需重新预演`
      : '预演未通过：请修正下列问题后重新预演（未写入任何数据）',
  });
}));

/**
 * 批次提交（单事务，整批落库或整体回滚）：
 * 相同批次号 + 相同内容 → 幂等返回（200）；相同批次号 + 不同内容 → 409。
 */
app.post('/api/batches/commit', wrap((req, res) => {
  const batchId = normId(req.body.batch_id, '批次号');
  const rawItems = req.body.items;
  if (!Array.isArray(rawItems) || rawItems.length === 0) {
    throw new ApiError(400, 'NO_ITEMS', '批次至少需要一条操作（items 为非空数组）');
  }
  const { items, itemErrors } = normalizeBatchItems(rawItems);
  if (itemErrors.size) {
    throw new ApiError(400, 'BATCH_ITEMS_INVALID', '批次条目存在格式错误，整批未提交', itemErrorList(items, itemErrors));
  }
  const result = commitBatch(batchId, items, planHashOf(batchId, items), req.body.commit_token);
  res.status(result.idempotent ? 200 : 201).json(result);
}));

app.get('/api/batches', wrap((req, res) => {
  res.json(db.prepare('SELECT * FROM batches ORDER BY committed_at, batch_id').all());
}));

app.get('/api/batches/:batchId', wrap((req, res) => {
  const b = qBatch.get(req.params.batchId);
  if (!b) throw new ApiError(404, 'BATCH_NOT_FOUND', `批次 ${req.params.batchId} 不存在`);
  const items = db.prepare(`
    SELECT bi.idx, o.op_id, o.target_bottle, o.op_time
    FROM batch_items bi JOIN operations o ON o.op_id = bi.op_id
    WHERE bi.batch_id = ? ORDER BY bi.idx`).all(b.batch_id)
    .map((r) => ({
      index: r.idx, op_id: r.op_id, target_bottle: r.target_bottle, op_time: r.op_time,
      sources: qOpSources.all(r.op_id).map((s) => ({ bottle: s.source_bottle, volume: s.volume })),
    }));
  res.json({ ...b, items });
}));

// ---------------------------------------------------------------- 初始化数据

function seed() {
  if (db.prepare('SELECT COUNT(*) AS c FROM bottles').get().c > 0) return;
  db.transaction(() => {
    qInsBottle.run('W2026-001', 'original', '2026-09-30T08:00:00', 1000, null);
    qInsBottle.run('W2026-002', 'original', '2026-09-30T09:00:00', 800, null);
    qInsBottle.run('W2026-003', 'original', '2026-10-01T08:30:00', 500, null);

    qInsOp.run('OP-0001', 1, 'MIX-001', '2026-10-01T10:00:00');
    qInsBottle.run('MIX-001', 'derived', '2026-10-01T10:00:00', 0, 'OP-0001');
    qInsSource.run('OP-0001', 'W2026-001', 200);
    qInsSource.run('OP-0001', 'W2026-002', 150);

    qInsOp.run('OP-0002', 2, 'ALQ-001', '2026-10-02T09:00:00');
    qInsBottle.run('ALQ-001', 'derived', '2026-10-02T09:00:00', 0, 'OP-0002');
    qInsSource.run('OP-0002', 'MIX-001', 100);

    qBumpVersion.run();
  })();
  console.log('[init] 数据库为空，已写入示例数据（3 个原始瓶 + 2 条操作）');
}

seed();

const server = app.listen(PORT, () => {
  console.log(`水质实验室样本溯源工作台已启动: http://localhost:${PORT}`);
  console.log(`数据库文件: ${DB_PATH}`);
});

process.on('SIGTERM', () => server.close(() => { db.close(); process.exit(0); }));
process.on('SIGINT', () => server.close(() => { db.close(); process.exit(0); }));
