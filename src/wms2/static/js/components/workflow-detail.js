/**
 * Alpine workflowDetail — fetches and exposes workflow, blocks, and DAG data.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('workflowDetail', (workflowId) => ({
        workflowId: workflowId,
        workflow: null,
        blocks: [],
        dag: null,
        allDags: [],
        outputDatasets: [],
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
                this.workflow = await WMS2_API.getWorkflow(this.workflowId);

                const [blks, dags, ods] = await Promise.all([
                    WMS2_API.getWorkflowBlocks(this.workflowId).catch(() => []),
                    WMS2_API.getWorkflowDags(this.workflowId).catch(() => []),
                    WMS2_API.getWorkflowOutputDatasets(this.workflowId).catch(() => []),
                ]);
                this.blocks = blks;
                this.allDags = dags;
                this.outputDatasets = ods;

                if (this.workflow.dag_id) {
                    this.dag = await WMS2_API.getDAG(this.workflow.dag_id).catch(() => null);
                } else {
                    this.dag = null;
                }
            } catch (e) {
                this.error = e.message;
            } finally {
                this.loading = false;
            }
        },

        get progressPct() {
            if (!this.workflow) return 0;
            return this.workflow.progress_pct || 0;
        },

        get stepMetrics() {
            if (!this.workflow || !this.workflow.step_metrics) return [];
            return parseStepMetrics(this.workflow.step_metrics);
        },

        get splittingDisplay() {
            if (!this.workflow) return {};
            const params = { ...(this.workflow.splitting_params || {}) };
            const tf = (this.workflow.config_data || {}).test_fraction;
            if (tf && tf < 1) {
                params.test_fraction = tf;
                // Show effective events per job if events_per_job exists
                if (params.events_per_job) {
                    params.effective_events_per_job = Math.round(params.events_per_job * tf);
                }
            }
            return params;
        },

        get testFraction() {
            const tf = (this.workflow?.config_data || {}).test_fraction;
            return (tf && tf < 1) ? tf : null;
        },

        get configData() {
            if (!this.workflow) return {};
            return this.workflow.config_data || {};
        },

        get isGen() {
            return !!this.configData._is_gen;
        },

        get pileupDatasets() {
            const steps = this.configData.manifest_steps || [];
            const seen = new Set();
            const result = [];
            for (const s of steps) {
                for (const key of ['mc_pileup', 'data_pileup']) {
                    const ds = s[key];
                    if (ds && !seen.has(ds)) {
                        seen.add(ds);
                        result.push({ type: key === 'mc_pileup' ? 'MC Pileup' : 'Data Pileup', dataset: ds });
                    }
                }
            }
            return result;
        },

        get stepsInfo() {
            const steps = this.configData.manifest_steps || [];
            return steps.map((s, i) => ({
                index: i + 1,
                name: s.name || `Step ${i + 1}`,
                cmssw: s.cmssw_version || '—',
                global_tag: s.global_tag || '—',
                scram_arch: s.scram_arch || '—',
            }));
        },

        get adaptiveInfo() {
            const sm = this.workflow?.step_metrics;
            if (!sm || !sm.adaptive_params) return null;
            const ap = sm.adaptive_params;
            const cd = this.workflow.config_data || {};
            const summary = ap.metrics_summary || {};
            return {
                original_memory: cd.memory_mb,
                tuned_memory: ap.tuned_memory_mb,
                original_nthreads: cd.multicore,
                tuned_nthreads: ap.tuned_nthreads,
                memory_source: ap.memory_source,
                peak_rss: summary.peak_rss_mb ? Math.round(summary.peak_rss_mb) : null,
                cpu_eff: summary.weighted_cpu_eff,
                rounds_completed: sm.rounds_completed || 0,
            };
        },

        get reqmgrUrl() {
            if (!this.workflow) return null;
            return 'https://cmsweb.cern.ch/reqmgr2/fetch?rid=' + encodeURIComponent(this.workflow.request_name);
        },
    }));
});
