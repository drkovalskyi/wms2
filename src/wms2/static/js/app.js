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
    held:          'danger',
    completed:     'success',
    partial:       'warning',
    failed:        'danger',
    aborted:       'danger',
    // HTCondor job statuses
    idle:          'secondary',
    running:       'primary',
    removed:       'danger',
};

function statusBadge(status) {
    const cls = STATUS_COLORS[status] || 'secondary';
    return `<mark class="badge badge--${cls}">${status}</mark>`;
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

function fmtMidTruncate(str, maxLen = 50) {
    if (!str) return '—';
    if (str.length <= maxLen) return str;
    const keep = maxLen - 3; // room for '...'
    const head = Math.ceil(keep * 0.6);
    const tail = keep - head;
    return str.slice(0, head) + '...' + str.slice(-tail);
}
