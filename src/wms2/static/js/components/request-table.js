/**
 * Alpine requestTable — filterable/sortable request list.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('requestTable', () => ({
        requests: [],
        loading: true,
        error: null,
        filterStatus: '',
        filterCampaign: '',
        sortCol: 'updated_at',
        sortAsc: false,

        init() {
            this.fetchData();
            window.addEventListener('wms2:refresh', () => this.fetchData());
        },

        async fetchData() {
            try {
                this.loading = true;
                this.error = null;
                const params = new URLSearchParams();
                if (this.filterStatus) params.set('status', this.filterStatus);
                params.set('limit', '500');
                const raw = await WMS2_API.listRequests(params.toString());
                // Progress is now included in the API response — no
                // extra per-request calls needed.
                this.requests = raw.map(r => ({
                    ...r,
                    pool: (r.request_data && r.request_data._condor_pool) || 'local',
                    _progress: r.progress_pct,
                    _events_produced: r.events_produced || 0,
                    _target_events: r.target_events || 0,
                }));
            } catch (e) {
                this.error = e.message;
            } finally {
                this.loading = false;
            }
        },

        get filtered() {
            let list = this.requests;
            if (this.filterCampaign) {
                const q = this.filterCampaign.toLowerCase();
                list = list.filter(r => (r.request_name || '').toLowerCase().includes(q)
                    || (r.pool || '').toLowerCase().includes(q)
                    || (r.campaign || '').toLowerCase().includes(q));
            }
            list = [...list].sort((a, b) => {
                let va = a[this.sortCol], vb = b[this.sortCol];
                if (va == null) va = '';
                if (vb == null) vb = '';
                if (typeof va === 'string') va = va.toLowerCase();
                if (typeof vb === 'string') vb = vb.toLowerCase();
                if (va < vb) return this.sortAsc ? -1 : 1;
                if (va > vb) return this.sortAsc ? 1 : -1;
                return 0;
            });
            return list;
        },

        sort(col) {
            if (this.sortCol === col) {
                this.sortAsc = !this.sortAsc;
            } else {
                this.sortCol = col;
                this.sortAsc = true;
            }
        },

        sortIcon(col) {
            if (this.sortCol !== col) return '';
            return this.sortAsc ? ' \u25B2' : ' \u25BC';
        },

        applyStatusFilter() {
            this.fetchData();
        },
    }));
});
