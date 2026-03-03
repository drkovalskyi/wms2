/**
 * WMS2 shared utilities.
 */
const STATUS_COLORS = {
    new:           'secondary',
    submitted:     'secondary',
    queued:        'info',
    pilot_running: 'info',
    planning:      'info',
    active:        'primary',
    stopping:      'warning',
    resubmitting:  'warning',
    paused:        'warning',
    held:          'danger',
    completed:     'success',
    partial:       'warning',
    failed:        'danger',
    aborted:       'danger',
    archived:      'success',
    open:          'info',
    // HTCondor job statuses
    idle:          'secondary',
    running:       'primary',
    removed:       'danger',
};

// Display labels for statuses that are misleading in raw form
const STATUS_LABELS = {
    archived: 'done',
};

function statusBadge(status) {
    const cls = STATUS_COLORS[status] || 'secondary';
    const label = STATUS_LABELS[status] || status;
    return `<mark class="badge badge--${cls}">${label}</mark>`;
}

function fmtAge(isoStr) {
    if (!isoStr) return '—';
    const ms = Date.now() - new Date(isoStr).getTime();
    if (ms < 0) return 'just now';
    const sec = Math.floor(ms / 1000);
    const m = Math.floor(sec / 60);
    const h = Math.floor(m / 60);
    const d = Math.floor(h / 24);
    if (d > 0) return `${d}d ${h % 24}h`;
    if (h > 0) return `${h}h ${m % 60}m`;
    if (m > 0) return `${m}m`;
    return `${sec}s`;
}

function fmtNum(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString();
}

function fmtPct(n) {
    if (n == null) return '—';
    return Number(n).toFixed(1) + '%';
}

function fmtDateTime(isoStr) {
    if (!isoStr) return '—';
    return new Date(isoStr).toLocaleString();
}

/**
 * Parse step_metrics JSON (structure: {rounds: {"0": {wu_metrics: [{per_step: {"1": {...}}}]}}})
 * into a flat array suitable for display: [{step, avg_cpu_efficiency, avg_peak_rss, ...}]
 */
function parseStepMetrics(sm) {
    if (!sm || typeof sm !== 'object') return [];
    // Already a flat array (legacy format)
    if (Array.isArray(sm)) return sm;

    const rounds = sm.rounds;
    if (!rounds || typeof rounds !== 'object') return [];

    // Accumulate weighted sums across all rounds and WUs per step
    const stepAccum = {};  // stepNum -> {cpu_eff_sum, rss_sum, tpe_sum, thr_sum, count}

    for (const [_roundNum, roundData] of Object.entries(rounds)) {
        const wuMetrics = roundData.wu_metrics;
        if (!Array.isArray(wuMetrics)) continue;
        for (const wu of wuMetrics) {
            const perStep = wu.per_step;
            if (!perStep || typeof perStep !== 'object') continue;
            for (const [stepNum, stepData] of Object.entries(perStep)) {
                if (!stepAccum[stepNum]) {
                    stepAccum[stepNum] = { cpu_sum: 0, rss_sum: 0, tpe_sum: 0, thr_sum: 0, count: 0 };
                }
                const acc = stepAccum[stepNum];
                const nJobs = stepData.num_jobs || 1;
                if (stepData.cpu_efficiency && stepData.cpu_efficiency.mean != null) {
                    acc.cpu_sum += stepData.cpu_efficiency.mean * nJobs;
                }
                if (stepData.peak_rss_mb && stepData.peak_rss_mb.mean != null) {
                    acc.rss_sum += stepData.peak_rss_mb.mean * nJobs;
                }
                if (stepData.time_per_event_sec && stepData.time_per_event_sec.mean != null) {
                    acc.tpe_sum += stepData.time_per_event_sec.mean * nJobs;
                }
                if (stepData.throughput_ev_s && stepData.throughput_ev_s.mean != null) {
                    acc.thr_sum += stepData.throughput_ev_s.mean * nJobs;
                }
                acc.count += nJobs;
            }
        }
    }

    return Object.entries(stepAccum)
        .sort(([a], [b]) => Number(a) - Number(b))
        .map(([stepNum, acc]) => ({
            step: 'Step ' + stepNum,
            avg_cpu_efficiency: acc.count ? (acc.cpu_sum / acc.count) * 100 : null,
            avg_peak_rss: acc.count ? acc.rss_sum / acc.count : null,
            avg_time_per_event: acc.count ? acc.tpe_sum / acc.count : null,
            avg_throughput: acc.count ? acc.thr_sum / acc.count : null,
        }));
}

function fmtMidTruncate(str, maxLen = 50) {
    if (!str) return '—';
    if (str.length <= maxLen) return str;
    const keep = maxLen - 3; // room for '...'
    const head = Math.ceil(keep * 0.6);
    const tail = keep - head;
    return str.slice(0, head) + '...' + str.slice(-tail);
}
