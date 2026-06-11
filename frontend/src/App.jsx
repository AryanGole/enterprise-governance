import { useState, useEffect, useCallback, useRef } from "react";

// ── Mock API data ─────────────────────────────────────────────────────────────
const MOCK = {
  kpis: { datasets: 1284, governanceScore: 73, lineageEdges: 3891, openAlerts: 7 },
  datasets: [
    { id: "1", name: "daily_revenue_summary", qualifiedName: "finance.mart.daily_revenue_summary", domain: "Finance", sourceSystem: "Snowflake", classification: "CONFIDENTIAL", governanceStatus: "APPROVED", ownerEmail: "r.chen@enterprise.com", ownerTeam: "Finance Analytics", isPii: false, score: 88, tier: "PLATINUM", columnCount: 4, tags: ["finance","mart","certified"], updateFrequency: "DAILY", schemaVersion: 7 },
    { id: "2", name: "risk_exposure_agg", qualifiedName: "risk.staging.risk_exposure_agg", domain: "Risk", sourceSystem: "Spark", classification: "RESTRICTED", governanceStatus: "PENDING_REVIEW", ownerEmail: "r.singh@enterprise.com", ownerTeam: "Risk Analytics", isPii: false, score: 62, tier: "GOLD", columnCount: 11, tags: ["risk","regulatory"], updateFrequency: "DAILY", schemaVersion: 3 },
    { id: "3", name: "customer_360_profile", qualifiedName: "marketing.gold.customer_360_profile", domain: "Marketing", sourceSystem: "dbt", classification: "RESTRICTED", governanceStatus: "PENDING_REVIEW", ownerEmail: "s.agarwal@enterprise.com", ownerTeam: "Marketing Analytics", isPii: true, score: 54, tier: "SILVER", columnCount: 28, tags: ["pii","crm"], updateFrequency: "DAILY", schemaVersion: 5 },
    { id: "4", name: "transaction_ledger", qualifiedName: "finance.raw.transaction_ledger", domain: "Finance", sourceSystem: "Kafka", classification: "RESTRICTED", governanceStatus: "BREAKING", ownerEmail: "d.kim@enterprise.com", ownerTeam: "Finance Engineering", isPii: false, score: 41, tier: "SILVER", columnCount: 5, tags: ["raw","transactions"], updateFrequency: "REAL_TIME", schemaVersion: 14 },
    { id: "5", name: "credit_scoring_features_v3", qualifiedName: "ml.features.credit_scoring_features_v3", domain: "Credit", sourceSystem: "Python", classification: "CONFIDENTIAL", governanceStatus: "DRAFT", ownerEmail: "k.murphy@enterprise.com", ownerTeam: "ML Engineering", isPii: false, score: 33, tier: "BRONZE", columnCount: 48, tags: ["ml","features"], updateFrequency: "DAILY", schemaVersion: 1 },
    { id: "6", name: "ops_pipeline_metrics", qualifiedName: "operations.mart.ops_pipeline_metrics", domain: "Operations", sourceSystem: "Airflow", classification: "INTERNAL", governanceStatus: "APPROVED", ownerEmail: "platform@enterprise.com", ownerTeam: "Data Platform", isPii: false, score: 79, tier: "GOLD", columnCount: 9, tags: ["ops","monitoring"], updateFrequency: "HOURLY", schemaVersion: 2 },
    { id: "7", name: "dim_products", qualifiedName: "reference.dim.dim_products", domain: "Reference", sourceSystem: "S3", classification: "INTERNAL", governanceStatus: "APPROVED", ownerEmail: "refdata@enterprise.com", ownerTeam: "Reference Data", isPii: false, score: 85, tier: "PLATINUM", columnCount: 14, tags: ["dimension","scd"], updateFrequency: "WEEKLY", schemaVersion: 4 },
    { id: "8", name: "raw_fx_rates", qualifiedName: "market_data.raw.raw_fx_rates", domain: "Market Data", sourceSystem: "API", classification: "INTERNAL", governanceStatus: "APPROVED", ownerEmail: "marketdata@enterprise.com", ownerTeam: "Market Data", isPii: false, score: 72, tier: "GOLD", columnCount: 6, tags: ["fx","bloomberg"], updateFrequency: "DAILY", schemaVersion: 2 },
  ],
  auditLogs: [
    { id: "a1", action: "SCHEMA_BREAKING_CHANGE", entity: "transaction_ledger v14", actor: "airflow-governance-dag", time: "02:14", severity: "critical", detail: "column_removed: amount_usd" },
    { id: "a2", action: "APPROVE", entity: "daily_revenue_summary governance", actor: "r.chen@enterprise.com", time: "01:52", severity: "success", detail: "Approved by Finance Steward" },
    { id: "a3", action: "CREATE", entity: "credit_scoring_features_v3", actor: "k.murphy@enterprise.com", time: "Yesterday 16:41", severity: "info", detail: "48 columns · CONFIDENTIAL" },
    { id: "a4", action: "UPDATE", entity: "customer_360_profile owner", actor: "admin@enterprise.com", time: "Yesterday 14:20", severity: "warning", detail: "j.patel → s.agarwal" },
    { id: "a5", action: "LINEAGE_REGISTERED", entity: "stg_transactions → int_revenue", actor: "airflow-governance-dag", time: "Yesterday 02:18", severity: "info", detail: "pipeline=finance_etl_daily" },
    { id: "a6", action: "SCHEMA_VERSION", entity: "risk_exposure_agg v3", actor: "airflow-governance-dag", time: "2 days ago", severity: "success", detail: "2 changes · non-breaking" },
  ],
  dictionary: [
    { id: "d1", term: "Net Revenue", domain: "Finance", definition: "Gross revenue minus refunds, chargebacks, and promotional discounts.", certified: true, steward: "r.chen@enterprise.com", synonyms: ["Net Sales"] },
    { id: "d2", term: "Risk Exposure", domain: "Risk", definition: "Maximum potential loss from a counterparty under stress scenario assumptions.", certified: true, steward: "r.singh@enterprise.com", synonyms: ["Credit Exposure"] },
    { id: "d3", term: "Customer 360", domain: "Marketing", definition: "Unified customer profile combining CRM, transactional, and behavioral data.", certified: false, steward: "s.agarwal@enterprise.com", synonyms: ["Golden Record"] },
    { id: "d4", term: "FX Rate", domain: "Market Data", definition: "End-of-day exchange rate between two currency pairs sourced from Bloomberg.", certified: true, steward: "marketdata@enterprise.com", synonyms: ["Exchange Rate"] },
    { id: "d5", term: "Governance Score", domain: "Platform", definition: "0–100 composite score measuring data asset governance maturity across 5 dimensions.", certified: true, steward: "platform@enterprise.com", synonyms: [] },
  ],
  impact: {
    rootDataset: "transaction_ledger",
    riskLevel: "CRITICAL",
    affectedCount: 23,
    recommendation: "Freeze deployments. Notify all downstream owners. Convene incident bridge.",
    affected: [
      { name: "daily_revenue_summary", path: "finance.mart · 1 hop", risk: "CRITICAL", hops: 1 },
      { name: "risk_exposure_agg", path: "risk.staging · 1 hop", risk: "CRITICAL", hops: 1 },
      { name: "Finance P&L Dashboard", path: "Tableau · 2 hops", risk: "HIGH", hops: 2 },
      { name: "credit_scoring_features_v3", path: "ml.features · 2 hops", risk: "HIGH", hops: 2 },
      { name: "ops_pipeline_metrics", path: "operations.mart · 3 hops", risk: "MEDIUM", hops: 3 },
    ]
  }
};

// ── Helpers ───────────────────────────────────────────────────────────────────
const classColors = {
  RESTRICTED: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
  CONFIDENTIAL: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300",
  INTERNAL: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
  PUBLIC: "bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-300",
};
const statusColors = {
  APPROVED: "bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-300",
  PENDING_REVIEW: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300",
  DRAFT: "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400",
  BREAKING: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
  DEPRECATED: "bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-500",
};
const domainColors = {
  Finance: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
  Risk: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
  Marketing: "bg-purple-100 text-purple-700 dark:bg-purple-950 dark:text-purple-300",
  Credit: "bg-orange-100 text-orange-700 dark:bg-orange-950 dark:text-orange-300",
  Operations: "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400",
  Reference: "bg-teal-100 text-teal-700 dark:bg-teal-950 dark:text-teal-300",
  "Market Data": "bg-indigo-100 text-indigo-700 dark:bg-indigo-950 dark:text-indigo-300",
};
const tierColors = {
  PLATINUM: { bar: "#9B59B6", bg: "bg-purple-100 text-purple-700" },
  GOLD:     { bar: "#F39C12", bg: "bg-amber-100 text-amber-700" },
  SILVER:   { bar: "#7F8C8D", bg: "bg-gray-100 text-gray-600" },
  BRONZE:   { bar: "#E24B4A", bg: "bg-red-100 text-red-700" },
};
const scoreColor = (s) => s >= 80 ? "#16A34A" : s >= 60 ? "#CA8A04" : s >= 40 ? "#EA580C" : "#DC2626";
const riskColors = { CRITICAL: "bg-red-100 text-red-700", HIGH: "bg-amber-100 text-amber-700", MEDIUM: "bg-blue-100 text-blue-700", LOW: "bg-green-100 text-green-700" };

function Badge({ text, className }) {
  return <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${className}`}>{text}</span>;
}
function ScoreBar({ score, tier }) {
  const color = tierColors[tier]?.bar || "#888";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${score}%`, background: color }} />
      </div>
      <span className="text-xs font-semibold w-6" style={{ color }}>{score}</span>
    </div>
  );
}
function Stat({ label, value, sub, color }) {
  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
      <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">{label}</div>
      <div className={`text-2xl font-semibold leading-none mb-1 ${color || "text-gray-900 dark:text-white"}`}>{value}</div>
      {sub && <div className="text-xs text-gray-400">{sub}</div>}
    </div>
  );
}

// ── Lineage SVG Canvas ────────────────────────────────────────────────────────
function LineageCanvas({ selectedDataset }) {
  const nodes = [
    { id: "n1", label: "raw_transactions", sub: "finance.raw", x: 30, y: 40, type: "source" },
    { id: "n2", label: "raw_fx_rates", sub: "market_data.raw", x: 30, y: 115, type: "source" },
    { id: "n3", label: "dim_products", sub: "reference.dim", x: 30, y: 190, type: "source" },
    { id: "n4", label: "stg_transactions", sub: "dbt · staging", x: 220, y: 75, type: "staging" },
    { id: "n5", label: "int_revenue", sub: "dbt · intermediate", x: 400, y: 105, type: "intermediate" },
    { id: "n6", label: "daily_revenue", sub: "finance.mart", x: 565, y: 105, type: "mart" },
    { id: "n7", label: "risk_exposure", sub: "risk.staging", x: 400, y: 190, type: "risk" },
  ];
  const edges = [
    { from: { x: 158, y: 60 }, to: { x: 220, y: 95 } },
    { from: { x: 158, y: 135 }, to: { x: 220, y: 110 } },
    { from: { x: 158, y: 210 }, to: { x: 220, y: 120 } },
    { from: { x: 358, y: 95 }, to: { x: 400, y: 122 } },
    { from: { x: 358, y: 145 }, to: { x: 400, y: 145 } },
    { from: { x: 378, y: 135 }, to: { x: 400, y: 200 } },
    { from: { x: 540, y: 122 }, to: { x: 565, y: 122 } },
  ];
  const typeColors = {
    source: { fill: "#EBF4FF", stroke: "#3B82F6", text: "#1D4ED8" },
    staging: { fill: "#ECFDF5", stroke: "#10B981", text: "#065F46" },
    intermediate: { fill: "#F5F3FF", stroke: "#8B5CF6", text: "#5B21B6" },
    mart: { fill: "#FDF4FF", stroke: "#A21CAF", text: "#701A75" },
    risk: { fill: "#FEF2F2", stroke: "#EF4444", text: "#991B1B" },
  };
  return (
    <svg viewBox="0 0 720 270" className="w-full h-full" role="img" aria-label="Data lineage graph">
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M2 1.5L7.5 5L2 8.5" fill="none" stroke="#94A3B8" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
        </marker>
      </defs>
      {edges.map((e, i) => (
        <line key={i} x1={e.from.x} y1={e.from.y} x2={e.to.x} y2={e.to.y} stroke="#CBD5E1" strokeWidth="1.5" markerEnd="url(#arrow)" />
      ))}
      {nodes.map((n) => {
        const c = typeColors[n.type] || typeColors.source;
        const w = 128, h = 46;
        return (
          <g key={n.id} style={{ cursor: "pointer" }}>
            <rect x={n.x} y={n.y} width={w} height={h} rx="8" fill={c.fill} stroke={c.stroke} strokeWidth="1.5" />
            <text x={n.x + w / 2} y={n.y + 17} textAnchor="middle" fontSize="11" fontWeight="600" fill={c.text} fontFamily="system-ui">{n.label}</text>
            <text x={n.x + w / 2} y={n.y + 33} textAnchor="middle" fontSize="9.5" fill="#64748B" fontFamily="system-ui">{n.sub}</text>
          </g>
        );
      })}
      <text x="79" y="258" textAnchor="middle" fontSize="10" fill="#94A3B8" fontFamily="system-ui">Source</text>
      <text x="284" y="258" textAnchor="middle" fontSize="10" fill="#94A3B8" fontFamily="system-ui">Staging (dbt)</text>
      <text x="464" y="258" textAnchor="middle" fontSize="10" fill="#94A3B8" fontFamily="system-ui">Intermediate</text>
      <text x="629" y="258" textAnchor="middle" fontSize="10" fill="#94A3B8" fontFamily="system-ui">Mart</text>
    </svg>
  );
}

// ── Schema Diff ───────────────────────────────────────────────────────────────
const schemaDiff = [
  { type: "removed", col: "amount_usd", detail: "FLOAT NOT NULL · COLUMN_REMOVED", breaking: true },
  { type: "added",   col: "amount_local_currency", detail: "FLOAT NOT NULL · COLUMN_ADDED", breaking: true },
  { type: "added",   col: "currency_code", detail: "VARCHAR(3) NULLABLE · COLUMN_ADDED", breaking: false },
  { type: "changed", col: "transaction_date", detail: "DATE → TIMESTAMP · TYPE_CHANGED (widening)", breaking: false },
];

// ── Pages ─────────────────────────────────────────────────────────────────────

function Dashboard({ onNavigate }) {
  const { kpis, datasets, auditLogs } = MOCK;
  const domainCounts = datasets.reduce((acc, d) => { acc[d.domain] = (acc[d.domain] || 0) + 1; return acc; }, {});
  const maxDomain = Math.max(...Object.values(domainCounts));

  return (
    <div className="space-y-5">
      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat label="Total Datasets" value={kpis.datasets.toLocaleString()} sub="↑ 48 this month" />
        <Stat label="Governance Score" value={`${kpis.governanceScore}/100`} sub="↑ 6pts vs last quarter" color="text-blue-600 dark:text-blue-400" />
        <Stat label="Lineage Edges" value={kpis.lineageEdges.toLocaleString()} sub="Across 12 domains" />
        <Stat label="Open Alerts" value={kpis.openAlerts} sub="3 breaking schema changes" color="text-red-600 dark:text-red-400" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Recent datasets table */}
        <div className="lg:col-span-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Recent Datasets</h2>
            <button onClick={() => onNavigate("catalog")} className="text-xs text-blue-600 hover:text-blue-700">View all →</button>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="border-b border-gray-100 dark:border-gray-700">
                <th className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-5 py-2.5">Dataset</th>
                <th className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-3 py-2.5">Domain</th>
                <th className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-3 py-2.5">Status</th>
                <th className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-3 py-2.5 min-w-24">Score</th>
              </tr></thead>
              <tbody>
                {datasets.slice(0, 5).map(d => (
                  <tr key={d.id} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30 cursor-pointer transition-colors" onClick={() => onNavigate("catalog")}>
                    <td className="px-5 py-3">
                      <div className="font-medium text-gray-900 dark:text-white text-xs">{d.name}</div>
                      <div className="text-xs text-gray-400 font-mono">{d.qualifiedName.split(".").slice(0, 2).join(".")}</div>
                    </td>
                    <td className="px-3 py-3"><Badge text={d.domain} className={domainColors[d.domain] || "bg-gray-100 text-gray-600"} /></td>
                    <td className="px-3 py-3"><Badge text={d.governanceStatus === "PENDING_REVIEW" ? "Pending" : d.governanceStatus === "BREAKING" ? "⚠ Breaking" : d.governanceStatus.charAt(0) + d.governanceStatus.slice(1).toLowerCase()} className={statusColors[d.governanceStatus]} /></td>
                    <td className="px-3 py-3 w-32"><ScoreBar score={d.score} tier={d.tier} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Right panel — domain + posture */}
        <div className="space-y-4">
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">Domain Breakdown</h2>
            <div className="space-y-2">
              {Object.entries(domainCounts).sort(([,a],[,b])=>b-a).map(([domain, count]) => (
                <div key={domain} className="flex items-center gap-2">
                  <span className="text-xs text-gray-500 w-20 shrink-0 truncate">{domain}</span>
                  <div className="flex-1 h-3 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                    <div className="h-full rounded-full bg-blue-500" style={{ width: `${(count / maxDomain) * 100}%` }} />
                  </div>
                  <span className="text-xs font-medium text-gray-700 dark:text-gray-300 w-4 text-right">{count}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">Governance Posture</h2>
            <div className="grid grid-cols-3 gap-2 mb-3">
              {[["Approved","68%","#16A34A"],["Pending","22%","#CA8A04"],["Draft","10%","#DC2626"]].map(([l,v,c])=>(
                <div key={l} className="text-center py-2 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                  <div className="text-lg font-bold" style={{color:c}}>{v}</div>
                  <div className="text-xs text-gray-500">{l}</div>
                </div>
              ))}
            </div>
            <div className="h-2 rounded-full overflow-hidden flex">
              <div className="bg-green-500 h-full" style={{width:"68%"}}/>
              <div className="bg-amber-500 h-full" style={{width:"22%"}}/>
              <div className="bg-red-500 h-full" style={{width:"10%"}}/>
            </div>
            <div className="text-xs text-gray-400 mt-2">PII datasets: 128 · RESTRICTED: 43</div>
          </div>
        </div>
      </div>

      {/* Audit log */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Recent Activity</h2>
          <button onClick={() => onNavigate("audit")} className="text-xs text-blue-600 hover:text-blue-700">Full audit log →</button>
        </div>
        <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
          {auditLogs.slice(0, 4).map(log => {
            const sev = { critical: { bg: "bg-red-100 dark:bg-red-950", ic: "text-red-500" }, success: { bg: "bg-green-100 dark:bg-green-950", ic: "text-green-500" }, warning: { bg: "bg-amber-100 dark:bg-amber-950", ic: "text-amber-500" }, info: { bg: "bg-blue-100 dark:bg-blue-950", ic: "text-blue-500" } }[log.severity];
            const icons = { critical: "⚠", success: "✓", warning: "↻", info: "+" };
            return (
              <div key={log.id} className="flex items-start gap-3 px-5 py-3 hover:bg-gray-50 dark:hover:bg-gray-700/20 transition-colors">
                <div className={`w-7 h-7 rounded-lg ${sev.bg} flex items-center justify-center shrink-0 mt-0.5`}>
                  <span className={`text-xs font-bold ${sev.ic}`}>{icons[log.severity]}</span>
                </div>
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-semibold text-gray-900 dark:text-white">{log.action} — {log.entity}</div>
                  <div className="text-xs text-gray-400 mt-0.5">{log.actor} · {log.detail}</div>
                </div>
                <div className="text-xs text-gray-400 shrink-0">{log.time}</div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function Catalog() {
  const [search, setSearch] = useState("");
  const [filterDomain, setFilterDomain] = useState("");
  const [filterClass, setFilterClass] = useState("");
  const [filterPii, setFilterPii] = useState(false);
  const [selected, setSelected] = useState(null);

  const filtered = MOCK.datasets.filter(d => {
    if (search && !d.name.toLowerCase().includes(search.toLowerCase()) && !d.qualifiedName.toLowerCase().includes(search.toLowerCase())) return false;
    if (filterDomain && d.domain !== filterDomain) return false;
    if (filterClass && d.classification !== filterClass) return false;
    if (filterPii && !d.isPii) return false;
    return true;
  });
  const domains = [...new Set(MOCK.datasets.map(d => d.domain))];

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-4">
        <div className="flex flex-wrap gap-3 items-center">
          <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search datasets..." className="flex-1 min-w-48 text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-gray-50 dark:bg-gray-700 text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500" />
          <select value={filterDomain} onChange={e => setFilterDomain(e.target.value)} className="text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-gray-50 dark:bg-gray-700 text-gray-700 dark:text-gray-300 focus:outline-none">
            <option value="">All domains</option>
            {domains.map(d => <option key={d}>{d}</option>)}
          </select>
          <select value={filterClass} onChange={e => setFilterClass(e.target.value)} className="text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-gray-50 dark:bg-gray-700 text-gray-700 dark:text-gray-300 focus:outline-none">
            <option value="">All classifications</option>
            {["PUBLIC","INTERNAL","CONFIDENTIAL","RESTRICTED"].map(c => <option key={c}>{c}</option>)}
          </select>
          <label className="flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 cursor-pointer select-none">
            <input type="checkbox" checked={filterPii} onChange={e => setFilterPii(e.target.checked)} className="rounded" />
            PII only
          </label>
          <span className="text-xs text-gray-400 ml-auto">{filtered.length} results</span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Table */}
        <div className={`${selected ? "lg:col-span-2" : "lg:col-span-3"} bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden`}>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="border-b border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/50">
                {["Dataset","Source","Domain","Classification","Owner","Status","Score"].map(h => (
                  <th key={h} className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-4 py-3">{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {filtered.map(d => (
                  <tr key={d.id} onClick={() => setSelected(selected?.id === d.id ? null : d)} className={`border-b border-gray-50 dark:border-gray-700/50 hover:bg-blue-50 dark:hover:bg-blue-900/20 cursor-pointer transition-colors ${selected?.id === d.id ? "bg-blue-50 dark:bg-blue-900/30" : ""}`}>
                    <td className="px-4 py-3">
                      <div className="font-medium text-gray-900 dark:text-white text-xs">{d.name}</div>
                      <div className="text-xs text-gray-400 font-mono">{d.qualifiedName}</div>
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">{d.sourceSystem}</td>
                    <td className="px-4 py-3"><Badge text={d.domain} className={domainColors[d.domain] || "bg-gray-100 text-gray-600"} /></td>
                    <td className="px-4 py-3"><Badge text={d.classification} className={classColors[d.classification]} /></td>
                    <td className="px-4 py-3 text-xs text-gray-500 truncate max-w-24">{d.ownerEmail.split("@")[0]}</td>
                    <td className="px-4 py-3"><Badge text={d.governanceStatus === "PENDING_REVIEW" ? "Pending" : d.governanceStatus === "BREAKING" ? "⚠ Breaking" : d.governanceStatus.charAt(0)+d.governanceStatus.slice(1).toLowerCase()} className={statusColors[d.governanceStatus]} /></td>
                    <td className="px-4 py-3 w-28"><ScoreBar score={d.score} tier={d.tier} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Detail panel */}
        {selected && (
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-5 space-y-4">
            <div className="flex items-start justify-between">
              <div>
                <div className="font-semibold text-gray-900 dark:text-white">{selected.name}</div>
                <div className="text-xs text-gray-400 font-mono mt-0.5">{selected.qualifiedName}</div>
              </div>
              <button onClick={() => setSelected(null)} className="text-gray-400 hover:text-gray-600 text-lg leading-none">×</button>
            </div>
            <div className="space-y-2">
              {[
                ["Domain", selected.domain],
                ["Source", selected.sourceSystem],
                ["Classification", selected.classification],
                ["Owner", selected.ownerEmail],
                ["Team", selected.ownerTeam],
                ["Frequency", selected.updateFrequency],
                ["Schema v", `v${selected.schemaVersion}`],
                ["Columns", selected.columnCount],
                ["PII", selected.isPii ? "Yes ⚠" : "No"],
              ].map(([k, v]) => (
                <div key={k} className="flex justify-between text-xs">
                  <span className="text-gray-400">{k}</span>
                  <span className="font-medium text-gray-700 dark:text-gray-300">{v}</span>
                </div>
              ))}
            </div>
            <div>
              <div className="text-xs text-gray-400 mb-1">Tags</div>
              <div className="flex flex-wrap gap-1">{selected.tags.map(t => <span key={t} className="text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-2 py-0.5 rounded">{t}</span>)}</div>
            </div>
            <div>
              <div className="flex justify-between items-center mb-1">
                <span className="text-xs text-gray-400">Governance Score</span>
                <Badge text={selected.tier} className={tierColors[selected.tier]?.bg || "bg-gray-100"} />
              </div>
              <ScoreBar score={selected.score} tier={selected.tier} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function LineagePage() {
  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Lineage Graph — daily_revenue_summary</h2>
            <p className="text-xs text-gray-400 mt-0.5">4 source datasets · 3 transformation hops · 5 downstream consumers</p>
          </div>
          <div className="flex gap-2">
            {[["Upstream","bg-blue-100 text-blue-700"],["Both","bg-purple-100 text-purple-700 font-semibold"],["Downstream","bg-green-100 text-green-700"]].map(([l, c]) => (
              <span key={l} className={`text-xs px-2.5 py-1 rounded-full cursor-pointer ${c}`}>{l}</span>
            ))}
          </div>
        </div>
        <div className="p-4 h-64 bg-gray-50 dark:bg-gray-900/40">
          <LineageCanvas />
        </div>
        <div className="px-5 py-2.5 border-t border-gray-100 dark:border-gray-700 text-xs text-gray-400 flex gap-4">
          {[["Source", "#3B82F6"],["Staging (dbt)", "#10B981"],["Intermediate", "#8B5CF6"],["Mart", "#A21CAF"],["Risk", "#EF4444"]].map(([l,c]) => (
            <span key={l} className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-sm inline-block" style={{background:c}}/>{l}</span>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { title: "Cross-Domain Flows", items: [["Finance → Risk","47 edges"],["Marketing → Finance","12 edges"],["Market Data → Finance","28 edges"],["Risk → Operations","8 edges"]] },
          { title: "Orphaned Datasets", items: [["raw_legacy_accounts","Reference"],["old_campaign_data","Marketing"],["test_transactions_2022","Finance"],["tmp_customer_ids","Unknown"]] },
          { title: "Most Connected", items: [["transaction_ledger","23 downstream"],["customer_360_profile","17 downstream"],["dim_products","31 consumers"],["raw_fx_rates","19 consumers"]] },
        ].map(panel => (
          <div key={panel.title} className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700">
              <h3 className="text-sm font-semibold text-gray-900 dark:text-white">{panel.title}</h3>
            </div>
            <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {panel.items.map(([name, detail]) => (
                <div key={name} className="px-4 py-2.5 flex justify-between items-center hover:bg-gray-50 dark:hover:bg-gray-700/30 cursor-pointer">
                  <span className="text-xs font-medium text-gray-800 dark:text-gray-200">{name}</span>
                  <span className="text-xs text-gray-400">{detail}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function SchemaEvolution() {
  const [activeDataset, setActiveDataset] = useState("transaction_ledger");
  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Schema Diff — transaction_ledger v13 → v14</h2>
            <p className="text-xs text-gray-400 mt-0.5">Detected by airflow-governance-dag · 2 minutes ago</p>
          </div>
          <Badge text="⚠ Breaking Change" className="bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300" />
        </div>
        <div className="p-5 space-y-2">
          {schemaDiff.map((row, i) => {
            const cfg = {
              removed: { bg: "bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-800", text: "text-red-700 dark:text-red-300", icon: "−", iconColor: "text-red-500" },
              added:   { bg: "bg-green-50 dark:bg-green-950/50 border border-green-200 dark:border-green-800", text: "text-green-700 dark:text-green-300", icon: "+", iconColor: "text-green-500" },
              changed: { bg: "bg-amber-50 dark:bg-amber-950/50 border border-amber-200 dark:border-amber-800", text: "text-amber-700 dark:text-amber-300", icon: "~", iconColor: "text-amber-500" },
            }[row.type];
            return (
              <div key={i} className={`flex items-center gap-3 px-4 py-2.5 rounded-lg font-mono text-xs ${cfg.bg}`}>
                <span className={`font-bold w-4 ${cfg.iconColor}`}>{cfg.icon}</span>
                <span className={`font-semibold ${cfg.text}`}>{row.col}</span>
                <span className="text-gray-500 ml-2">{row.detail}</span>
                {row.breaking && <Badge text="BREAKING" className="ml-auto bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300" />}
              </div>
            );
          })}
        </div>
        <div className="mx-5 mb-5 p-4 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 rounded-lg flex gap-3 text-xs text-red-700 dark:text-red-300">
          <span className="text-base">⚠</span>
          <div><strong>Impact:</strong> 23 downstream datasets and 4 dashboards reference <code className="font-mono bg-red-100 dark:bg-red-900 px-1 rounded">amount_usd</code>. Pipeline failures expected unless consumers are updated before deployment.</div>
        </div>
      </div>

      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Version History</h2>
        </div>
        <table className="w-full text-sm">
          <thead><tr className="border-b border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/50">
            {["Version","Date","Changes","Breaking","Author"].map(h => <th key={h} className="text-left text-xs font-medium text-gray-400 uppercase tracking-wide px-5 py-2.5">{h}</th>)}
          </tr></thead>
          <tbody>
            {[
              ["v14","Today 02:14","4 changes",true,"airflow-dag"],
              ["v13","Yesterday","1 change",false,"d.kim@enterprise"],
              ["v12","3 days ago","2 changes",false,"airflow-dag"],
              ["v11","1 week ago","6 changes",true,"d.kim@enterprise"],
              ["v10","2 weeks ago","1 change",false,"airflow-dag"],
            ].map(([v,d,c,b,a]) => (
              <tr key={v} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/20">
                <td className="px-5 py-3 font-mono text-xs font-semibold text-gray-900 dark:text-white">{v}</td>
                <td className="px-5 py-3 text-xs text-gray-500">{d}</td>
                <td className="px-5 py-3 text-xs text-gray-600 dark:text-gray-300">{c}</td>
                <td className="px-5 py-3"><Badge text={b ? "Yes" : "No"} className={b ? "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300" : "bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-300"} /></td>
                <td className="px-5 py-3 text-xs text-gray-500">{a}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ImpactPage() {
  const { impact } = MOCK;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <Stat label="Affected Datasets" value={impact.affectedCount} sub="Direct + transitive" color="text-red-600" />
        <Stat label="Affected Dashboards" value="4" sub="Finance + Risk BI" color="text-amber-600" />
        <Stat label="Risk Level" value={impact.riskLevel} sub={impact.recommendation.slice(0,36)+"…"} color="text-red-600" />
      </div>
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Downstream Blast Radius — transaction_ledger v14 schema change</h2>
        </div>
        <div className="p-5 mb-1 bg-red-50 dark:bg-red-950/30 border-b border-red-200 dark:border-red-800 flex gap-3 text-xs text-red-700 dark:text-red-300">
          <span className="text-base shrink-0">🚨</span>
          <div><strong>Recommendation:</strong> {impact.recommendation}</div>
        </div>
        <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
          {impact.affected.map((item, i) => (
            <div key={i} className="flex items-center gap-4 px-5 py-3.5 hover:bg-gray-50 dark:hover:bg-gray-700/20 transition-colors">
              <div className="w-6 h-6 rounded-full bg-gray-100 dark:bg-gray-700 flex items-center justify-center text-xs font-bold text-gray-500">{item.hops}</div>
              <div className="flex-1">
                <div className="text-sm font-medium text-gray-900 dark:text-white">{item.name}</div>
                <div className="text-xs text-gray-400 mt-0.5">{item.path}</div>
              </div>
              <Badge text={item.risk} className={riskColors[item.risk]} />
            </div>
          ))}
          <div className="px-5 py-3 text-xs text-gray-400">+ 18 more downstream assets</div>
        </div>
      </div>
    </div>
  );
}

function GovernancePage() {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
        {[["Approved Datasets","874","text-green-600"],["Pending Review","282","text-amber-600"],["PII Datasets","128","text-red-600"],["Avg. Gov Score","73","text-blue-600"],["RESTRICTED Class.","43","text-purple-600"],["Orphaned Datasets","67","text-gray-500"]].map(([l,v,c]) => (
          <Stat key={l} label={l} value={v} color={c} />
        ))}
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Pending Approvals</h2>
            <Badge text="3" className="bg-red-100 text-red-700" />
          </div>
          <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
            {[
              { name: "risk_exposure_agg", team: "Risk", by: "r.singh@enterprise.com", avatar: "RS", color: "bg-blue-500" },
              { name: "customer_360_profile", team: "Marketing", by: "k.martinez@enterprise.com", avatar: "KM", color: "bg-purple-500" },
              { name: "ml_fraud_features_v2", team: "ML Engineering", by: "a.puri@enterprise.com", avatar: "AP", color: "bg-teal-500" },
            ].map(item => (
              <div key={item.name} className="flex items-center gap-3 px-5 py-3.5 hover:bg-gray-50 dark:hover:bg-gray-700/20 transition-colors">
                <div className={`w-8 h-8 rounded-full ${item.color} flex items-center justify-center text-xs font-bold text-white shrink-0`}>{item.avatar}</div>
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-semibold text-gray-900 dark:text-white">{item.name}</div>
                  <div className="text-xs text-gray-400">{item.team} · {item.by}</div>
                </div>
                <button className="text-xs bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-700 px-3 py-1 rounded-lg hover:bg-blue-100 transition-colors">Review</button>
              </div>
            ))}
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-white mb-4">Compliance Coverage</h2>
          <div className="space-y-3.5">
            {[["GDPR-tagged datasets","94%","bg-green-500"],["SOX-compliant pipelines","81%","bg-blue-500"],["PCI datasets with RESTRICTED class.","73%","bg-amber-500"],["HIPAA datasets with DLP controls","58%","bg-red-500"]].map(([l,v,c]) => (
              <div key={l}>
                <div className="flex justify-between text-xs mb-1.5">
                  <span className="text-gray-600 dark:text-gray-400">{l}</span>
                  <span className="font-semibold text-gray-900 dark:text-white">{v}</span>
                </div>
                <div className="h-1.5 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                  <div className={`h-full rounded-full ${c}`} style={{width:v}}/>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function AuditLog() {
  const sevConfig = {
    critical: { bg: "bg-red-50 dark:bg-red-950/40", border: "border-l-4 border-red-400", icon: "⚠", ic: "text-red-500" },
    success: { bg: "bg-green-50 dark:bg-green-950/40", border: "border-l-4 border-green-400", icon: "✓", ic: "text-green-500" },
    warning: { bg: "bg-amber-50 dark:bg-amber-950/40", border: "border-l-4 border-amber-400", icon: "↻", ic: "text-amber-500" },
    info: { bg: "bg-blue-50 dark:bg-blue-950/40", border: "border-l-4 border-blue-400", icon: "+", ic: "text-blue-500" },
  };
  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Immutable Audit Log</h2>
            <p className="text-xs text-gray-400 mt-0.5">SOX-compliant · Last 24h · 847 events</p>
          </div>
          <div className="flex gap-2">
            {["All","Critical","Approvals","Schema"].map(f => (
              <button key={f} className="text-xs px-3 py-1 rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors first:bg-gray-100 dark:first:bg-gray-700">{f}</button>
            ))}
          </div>
        </div>
        <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
          {MOCK.auditLogs.map(log => {
            const cfg = sevConfig[log.severity];
            return (
              <div key={log.id} className={`flex items-start gap-4 px-5 py-3.5 hover:bg-gray-50 dark:hover:bg-gray-700/20 transition-colors ${cfg.bg} ${cfg.border}`}>
                <span className={`text-sm font-bold mt-0.5 ${cfg.ic} shrink-0`}>{cfg.icon}</span>
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-semibold text-gray-900 dark:text-white">{log.action}</div>
                  <div className="text-xs text-gray-600 dark:text-gray-300 mt-0.5">{log.entity}</div>
                  <div className="text-xs text-gray-400 mt-0.5">{log.actor} · {log.detail}</div>
                </div>
                <div className="text-xs text-gray-400 shrink-0 text-right">
                  <div>{log.time}</div>
                  <div className="mt-1 font-mono text-gray-300 dark:text-gray-600 text-xs">#{log.id}</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function Dictionary() {
  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-sm font-semibold text-gray-900 dark:text-white">Business Data Dictionary</h2>
            <p className="text-xs text-gray-400 mt-0.5">{MOCK.dictionary.filter(t=>t.certified).length} certified of {MOCK.dictionary.length} terms</p>
          </div>
          <button className="text-xs bg-blue-600 text-white px-3 py-1.5 rounded-lg hover:bg-blue-700 transition-colors">+ Add term</button>
        </div>
        <div className="divide-y divide-gray-50 dark:divide-gray-700/50">
          {MOCK.dictionary.map(term => (
            <div key={term.id} className="px-5 py-4 hover:bg-gray-50 dark:hover:bg-gray-700/20 cursor-pointer transition-colors">
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-semibold text-gray-900 dark:text-white text-sm">{term.term}</span>
                    {term.certified && <Badge text="✓ Certified" className="bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-300" />}
                    <Badge text={term.domain} className={domainColors[term.domain] || "bg-gray-100 text-gray-600"} />
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">{term.definition}</p>
                  {term.synonyms.length > 0 && (
                    <div className="mt-1.5 flex gap-1 flex-wrap">
                      <span className="text-xs text-gray-400">Also: </span>
                      {term.synonyms.map(s => <span key={s} className="text-xs text-gray-500 italic">{s}</span>)}
                    </div>
                  )}
                </div>
                <div className="text-xs text-gray-400 shrink-0">{term.steward.split("@")[0]}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── App Shell ─────────────────────────────────────────────────────────────────
const NAV = [
  { id: "dashboard", label: "Dashboard", icon: "▦" },
  { id: "catalog", label: "Data Catalog", icon: "⊞" },
  { id: "lineage", label: "Lineage Graph", icon: "⋯" },
  { id: "governance", label: "Governance", icon: "⊛", badge: 3 },
  { id: "schema", label: "Schema Evolution", icon: "⊕" },
  { id: "impact", label: "Impact Analysis", icon: "◎" },
  { id: "audit", label: "Audit Log", icon: "≡" },
  { id: "dictionary", label: "Data Dictionary", icon: "⊟" },
];
const PAGE_TITLES = { dashboard: "Governance Overview", catalog: "Data Catalog", lineage: "Lineage Graph", governance: "Governance Controls", schema: "Schema Evolution", impact: "Impact Analysis", audit: "Audit Log", dictionary: "Data Dictionary" };

export default function App() {
  const [page, setPage] = useState("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const pages = { dashboard: Dashboard, catalog: Catalog, lineage: LineagePage, governance: GovernancePage, schema: SchemaEvolution, impact: ImpactPage, audit: AuditLog, dictionary: Dictionary };
  const PageComponent = pages[page];

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900 font-sans text-gray-900 dark:text-white overflow-hidden">
      {/* Sidebar */}
      <aside className={`${sidebarOpen ? "w-52" : "w-14"} shrink-0 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col transition-all duration-200 overflow-hidden`}>
        <div className="flex items-center gap-2.5 px-4 py-4 border-b border-gray-100 dark:border-gray-700">
          <div className="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center shrink-0">
            <span className="text-white text-xs font-bold">DG</span>
          </div>
          {sidebarOpen && (
            <div className="min-w-0">
              <div className="text-xs font-semibold text-gray-900 dark:text-white leading-tight">DataGovernance</div>
              <div className="text-xs text-gray-400">Enterprise v2.1</div>
            </div>
          )}
        </div>
        <nav className="flex-1 py-3 overflow-y-auto">
          {NAV.map(item => (
            <button key={item.id} onClick={() => setPage(item.id)} className={`w-full flex items-center gap-2.5 px-4 py-2 text-xs transition-colors ${page === item.id ? "bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 font-semibold border-r-2 border-blue-600" : "text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700/40 hover:text-gray-900 dark:hover:text-white"}`}>
              <span className="shrink-0 text-sm w-4 text-center">{item.icon}</span>
              {sidebarOpen && <span className="flex-1 text-left">{item.label}</span>}
              {sidebarOpen && item.badge && <span className="ml-auto bg-red-500 text-white text-xs font-bold px-1.5 py-0.5 rounded-full leading-none">{item.badge}</span>}
            </button>
          ))}
        </nav>
        <div className="border-t border-gray-100 dark:border-gray-700 p-3">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-full bg-blue-600 flex items-center justify-center text-xs font-bold text-white shrink-0">SA</div>
            {sidebarOpen && <div className="min-w-0"><div className="text-xs font-medium text-gray-800 dark:text-gray-200 truncate">S. Agarwal</div><div className="text-xs text-gray-400">Data Steward</div></div>}
          </div>
        </div>
      </aside>

      {/* Main */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Topbar */}
        <header className="h-12 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 flex items-center px-4 gap-3 shrink-0">
          <button onClick={() => setSidebarOpen(v => !v)} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-lg leading-none">☰</button>
          <h1 className="text-sm font-semibold text-gray-900 dark:text-white flex-1">{PAGE_TITLES[page]}</h1>
          <div className="flex items-center gap-2 bg-gray-100 dark:bg-gray-700 rounded-lg px-3 py-1.5 text-xs text-gray-400 w-48">
            <span>🔍</span><span>Search datasets…</span>
          </div>
          <div className="relative">
            <button className="w-8 h-8 flex items-center justify-center rounded-lg border border-gray-200 dark:border-gray-600 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors">🔔</button>
            <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 bg-red-500 rounded-full"/>
          </div>
          <button className="w-8 h-8 flex items-center justify-center rounded-lg border border-gray-200 dark:border-gray-600 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors">⚙</button>
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto p-5">
          <PageComponent onNavigate={setPage} />
        </main>
      </div>
    </div>
  );
}
