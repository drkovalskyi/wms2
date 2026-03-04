/**
 * Alpine workflowDetail — fetches and exposes workflow, blocks, and DAG data.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('workflowDetail', (workflowId) => ({
        workflowId: workflowId,
        workflow: null,
        dag: null,
        allDags: [],
        outputDatasets: [],
        loading: true,
        error: null,
        selectedMetricsRound: 'all',  // 'all' or round number string

        init() {
            this.fetchAll();
            window.addEventListener('wms2:refresh', () => this.fetchAll());
        },

        async fetchAll() {
            try {
                this.loading = true;
                this.error = null;
                this.workflow = await WMS2_API.getWorkflow(this.workflowId);

                const [dags, ods] = await Promise.all([
                    WMS2_API.getWorkflowDags(this.workflowId).catch(() => []),
                    WMS2_API.getWorkflowOutputDatasets(this.workflowId).catch(() => []),
                ]);
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

        /** Available round numbers for the metrics dropdown. */
        get metricsRoundOptions() {
            const sm = this.workflow?.step_metrics;
            if (!sm || !sm.rounds) return [];
            return Object.keys(sm.rounds).sort((a, b) => Number(a) - Number(b));
        },

        /** Step metrics filtered by selected round. */
        get stepMetrics() {
            if (!this.workflow || !this.workflow.step_metrics) return [];
            if (this.selectedMetricsRound === 'all') {
                return parseStepMetrics(this.workflow.step_metrics);
            }
            // Parse single round
            const sm = this.workflow.step_metrics;
            const rounds = sm.rounds || {};
            const roundData = rounds[this.selectedMetricsRound];
            if (!roundData) return [];
            const fake = { rounds: { [this.selectedMetricsRound]: roundData } };
            return parseStepMetrics(fake);
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

        /**
         * Merged round history — combines round performance metrics with
         * DAG status/dates into a single table.
         *
         * Groups DAGs by round number (extracted from dag_file_path).
         * For rounds with rescue DAGs, shows the latest DAG's status.
         */
        get roundHistory() {
            const sm = this.workflow?.step_metrics;
            const cd = this.workflow?.config_data || {};
            const origMem = cd.memory_mb;
            const origThreads = cd.multicore;
            const rounds = sm?.rounds || {};

            // Build DAG-by-round index: extract round from dag_file_path
            const dagsByRound = {};
            for (const d of this.allDags) {
                const m = (d.dag_file_path || '').match(/round_(\d+)/);
                const rnd = m ? Number(m[1]) : 0;
                if (!dagsByRound[rnd]) dagsByRound[rnd] = [];
                dagsByRound[rnd].push(d);
            }
            // Sort each round's DAGs by created_at (latest last)
            for (const rnd of Object.keys(dagsByRound)) {
                dagsByRound[rnd].sort((a, b) =>
                    (a.created_at || '').localeCompare(b.created_at || ''));
            }

            // Collect all round numbers from both sources
            const allRounds = new Set();
            for (const rk of Object.keys(rounds)) allRounds.add(Number(rk));
            for (const rk of Object.keys(dagsByRound)) allRounds.add(Number(rk));
            const sortedRounds = [...allRounds].sort((a, b) => a - b);

            const history = [];
            for (const roundNum of sortedRounds) {
                const rk = String(roundNum);
                const rd = rounds[rk];
                const dags = dagsByRound[roundNum] || [];
                const latestDag = dags.length > 0 ? dags[dags.length - 1] : null;

                // Resources used for this round
                let memUsed, threadsUsed;
                if (rd?.resource_params) {
                    memUsed = rd.resource_params.memory_mb;
                    threadsUsed = rd.resource_params.nthreads;
                } else if (roundNum === 0) {
                    memUsed = origMem;
                    threadsUsed = origThreads;
                } else {
                    const prevRd = rounds[String(roundNum - 1)];
                    const prevAp = prevRd?.adaptive_params || sm?.adaptive_params;
                    if (prevAp && roundNum > 0) {
                        memUsed = prevAp.tuned_memory_mb || origMem;
                        threadsUsed = prevAp.tuned_nthreads || origThreads;
                    } else {
                        memUsed = origMem;
                        threadsUsed = origThreads;
                    }
                }

                // Compute average CPU efficiency and peak memory from wu_metrics
                let cpuEff = null, peakMem = null;
                const wuMetrics = rd?.wu_metrics || [];
                if (wuMetrics.length > 0) {
                    let cpuSum = 0, cpuWeight = 0, maxMem = 0;
                    for (const wu of wuMetrics) {
                        const ps = wu.per_step || {};
                        for (const [_sn, sd] of Object.entries(ps)) {
                            const nJobs = sd.num_jobs || 1;
                            if (sd.cpu_efficiency?.mean != null) {
                                cpuSum += sd.cpu_efficiency.mean * nJobs;
                                cpuWeight += nJobs;
                            }
                            if (sd.peak_rss_mb?.mean != null && sd.peak_rss_mb.mean > maxMem) {
                                maxMem = sd.peak_rss_mb.mean;
                            }
                        }
                    }
                    if (cpuWeight > 0) cpuEff = cpuSum / cpuWeight;
                    if (maxMem > 0) peakMem = Math.round(maxMem);
                }

                history.push({
                    round: roundNum,
                    status: latestDag?.status || null,
                    work_units: rd?.work_units || latestDag?.total_work_units || 0,
                    nodes_done: rd?.nodes_done || latestDag?.nodes_done || 0,
                    nodes_failed: rd?.nodes_failed || latestDag?.nodes_failed || 0,
                    memory_used: memUsed,
                    threads_used: threadsUsed,
                    cpu_eff: cpuEff,
                    peak_memory: peakMem,
                    created_at: latestDag?.created_at || null,
                    dag_id: latestDag?.id || null,
                    num_dags: dags.length,
                });
            }
            return history;
        },

        /** Latest adaptive optimization result summary. */
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
                measured_memory: ap.measured_memory_mb || null,
                peak_rss: summary.peak_rss_mb ? Math.round(summary.peak_rss_mb) : null,
                cpu_eff: summary.weighted_cpu_eff,
                rounds_completed: sm.rounds_completed || 0,
                per_step: ap.per_step || null,
            };
        },

        get reqmgrUrl() {
            if (!this.workflow) return null;
            return 'https://cmsweb.cern.ch/reqmgr2/fetch?rid=' + encodeURIComponent(this.workflow.request_name);
        },
    }));
});
