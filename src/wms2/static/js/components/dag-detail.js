/**
 * Alpine dagDetail — fetches and exposes DAG detail + history.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('dagDetail', (dagId) => ({
        dagId: dagId,
        dag: null,
        history: [],
        jobs: [],
        nodeLog: [],
        showNodeLog: false,
        nodeFilter: '',
        performance: null,
        loading: true,
        error: null,

        init() {
            this.fetchAll();
            window.addEventListener('wms2:refresh', () => this.fetchAll());
        },

        async fetchAll() {
            try {
                this.loading = true;
                this.error = null;
                const [d, h, j, nl, perf] = await Promise.all([
                    WMS2_API.getDAG(this.dagId),
                    WMS2_API.getDAGHistory(this.dagId).catch(() => []),
                    WMS2_API.getDAGJobs(this.dagId).catch(() => []),
                    WMS2_API.getDAGNodeLog(this.dagId).catch(() => []),
                    WMS2_API.getDAGPerformance(this.dagId).catch(() => null),
                ]);
                this.dag = d;
                this.history = h;
                this.jobs = j;
                this.nodeLog = nl;
                this.performance = perf;
            } catch (e) {
                this.error = e.message;
            } finally {
                this.loading = false;
            }
        },

        get nodeLogNodes() {
            const names = new Set(this.nodeLog.map(e => e.node));
            return [...names].sort();
        },

        get filteredNodeLog() {
            const log = this.nodeLog.slice().reverse();
            if (!this.nodeFilter) return log;
            return log.filter(e => e.node === this.nodeFilter);
        },

        eventClass(ev) {
            const ok = { submit: true, execute: true };
            const bad = { held: true, aborted: true, shadow_exception: true };
            const warn = { evicted: true, released: true };
            if (ev.event === 'terminated') {
                return ev.detail.startsWith('exit 0') ? 'event-ok' : 'event-bad';
            }
            if (ev.event === 'post_script') {
                return ev.detail === 'exit 0' ? 'event-ok' : 'event-bad';
            }
            if (bad[ev.event]) return 'event-bad';
            if (warn[ev.event]) return 'event-warn';
            if (ok[ev.event]) return 'event-ok';
            return '';
        },

        fmtWallTime(secs) {
            if (secs == null) return '—';
            const h = Math.floor(secs / 3600);
            const m = Math.floor((secs % 3600) / 60);
            const s = Math.floor(secs % 60);
            if (h > 0) return h + 'h ' + String(m).padStart(2, '0') + 'm';
            if (m > 0) return m + 'm ' + String(s).padStart(2, '0') + 's';
            return s + 's';
        },

        get hasPerformance() {
            if (!this.performance) return false;
            return this.performance.dag_metrics.length > 0 ||
                (this.performance.step_data && this.performance.step_data.wu_metrics);
        },

        get roundStepMetrics() {
            if (!this.performance || !this.performance.step_data) return [];
            const sd = this.performance.step_data;
            const wuMetrics = sd.wu_metrics;
            if (!Array.isArray(wuMetrics) || wuMetrics.length === 0) return [];
            // Aggregate across WUs for this round
            const stepAccum = {};
            for (const wu of wuMetrics) {
                const perStep = wu.per_step;
                if (!perStep) continue;
                for (const [stepNum, s] of Object.entries(perStep)) {
                    if (!stepAccum[stepNum]) {
                        stepAccum[stepNum] = { n: 0, cpu: 0, rss: 0, vsize: 0, tpe: 0, thr: 0, wall: 0,
                            read: 0, write: 0, evProc: 0, evWrit: 0 };
                    }
                    const a = stepAccum[stepNum];
                    const nj = s.num_jobs || 1;
                    a.n += nj;
                    if (s.cpu_efficiency?.mean != null) a.cpu += s.cpu_efficiency.mean * nj;
                    if (s.peak_rss_mb?.mean != null) a.rss += s.peak_rss_mb.mean * nj;
                    if (s.peak_vsize_mb?.mean != null) a.vsize += s.peak_vsize_mb.mean * nj;
                    if (s.time_per_event_sec?.mean != null) a.tpe += s.time_per_event_sec.mean * nj;
                    if (s.throughput_ev_s?.mean != null) a.thr += s.throughput_ev_s.mean * nj;
                    if (s.wall_time_sec?.mean != null) a.wall += s.wall_time_sec.mean * nj;
                    if (s.read_mb?.mean != null) a.read += s.read_mb.mean * nj;
                    if (s.write_mb?.mean != null) a.write += s.write_mb.mean * nj;
                    if (s.events_processed?.total != null) a.evProc += s.events_processed.total;
                    if (s.events_written?.total != null) a.evWrit += s.events_written.total;
                }
            }
            return Object.entries(stepAccum)
                .sort(([a], [b]) => Number(a) - Number(b))
                .map(([stepNum, a]) => ({
                    step: stepNum,
                    num_jobs: a.n,
                    cpu_efficiency: a.n ? (a.cpu / a.n * 100).toFixed(1) : '—',
                    peak_rss_mb: a.n ? (a.rss / a.n).toFixed(0) : '—',
                    peak_vsize_mb: a.n ? (a.vsize / a.n).toFixed(0) : '—',
                    time_per_event: a.n ? (a.tpe / a.n).toFixed(2) : '—',
                    throughput: a.n ? (a.thr / a.n).toFixed(4) : '—',
                    wall_time: a.n ? (a.wall / a.n).toFixed(0) : '—',
                    read_mb: a.n ? (a.read / a.n).toFixed(1) : '—',
                    write_mb: a.n ? (a.write / a.n).toFixed(1) : '—',
                    events_processed: a.evProc || '—',
                    events_written: a.evWrit || '—',
                }));
        },

        get totalNodes() {
            return this.dag ? (this.dag.total_nodes || 0) : 0;
        },

        segPct(count) {
            if (!this.totalNodes) return '0%';
            return (100 * (count || 0) / this.totalNodes) + '%';
        },

        get wuCompleted() {
            if (!this.dag) return 0;
            const wu = this.dag.completed_work_units;
            if (Array.isArray(wu)) return wu.length;
            return wu || 0;
        },

        get wuPct() {
            if (!this.dag || !this.dag.total_work_units) return 0;
            return Math.min(100, 100 * this.wuCompleted / this.dag.total_work_units);
        },

        get nodeCountsByType() {
            if (!this.dag || !this.dag.node_counts) return [];
            const nc = this.dag.node_counts;
            const order = ['processing', 'landing', 'merge', 'cleanup'];
            return order
                .filter(t => nc[t] != null && nc[t] > 0)
                .map(t => ({ type: t.charAt(0).toUpperCase() + t.slice(1), count: nc[t] }));
        },
    }));
});
