/**
 * Alpine requestDetail — fetches and exposes request + workflow + DAG data.
 * Includes action methods (stop, release, fail, restart) with toast feedback.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('requestDetail', (requestName) => ({
        name: requestName,
        request: null,
        workflow: null,
        dag: null,
        errors: null,
        loading: true,
        error: null,

        // Action state
        actionLoading: false,
        showStopDialog: false,
        showFailDialog: false,
        showRestartDialog: false,
        stopReason: 'Operator-initiated clean stop',

        // Priority profile editing
        editingPriority: false,
        prioHigh: 5,
        prioNominal: 3,
        prioSwitchFraction: 0.5,

        init() {
            this.fetchAll();
            window.addEventListener('wms2:refresh', () => this.fetchAll());
        },

        async fetchAll() {
            try {
                this.loading = true;
                this.error = null;
                this.request = await WMS2_API.getRequest(this.name);

                // Fetch workflow, DAG, and errors in parallel (graceful failures)
                const [wf, errs] = await Promise.all([
                    WMS2_API.getWorkflowByRequest(this.name).catch(() => null),
                    WMS2_API.getRequestErrors(this.name).catch(() => null),
                ]);
                this.workflow = wf;
                this.errors = errs;

                if (wf && wf.dag_id) {
                    this.dag = await WMS2_API.getDAG(wf.dag_id).catch(() => null);
                } else {
                    this.dag = null;
                }
            } catch (e) {
                this.error = e.message;
            } finally {
                this.loading = false;
            }
        },

        get hasWorkflow() { return this.workflow != null; },
        get hasDAG() { return this.dag != null; },
        get hasErrors() {
            if (!this.errors) return false;
            const e = this.errors;
            return (e.total_failures || 0) > 0 || (e.site_errors && e.site_errors.length > 0);
        },

        get progressPct() {
            if (!this.workflow) return 0;
            return this.workflow.progress_pct || 0;
        },

        get stepMetrics() {
            if (!this.workflow || !this.workflow.step_metrics) return [];
            return parseStepMetrics(this.workflow.step_metrics);
        },

        get transitions() {
            if (!this.request || !this.request.status_transitions) return [];
            return this.request.status_transitions;
        },

        // Action visibility helpers
        get canStop() {
            return this.request && ['active', 'pilot_running'].includes(this.request.status);
        },
        get canRelease() {
            return this.request && ['held', 'paused'].includes(this.request.status);
        },
        get canFail() {
            return this.request && ['held', 'partial'].includes(this.request.status);
        },
        get canRestart() {
            return this.request && ['held', 'partial'].includes(this.request.status);
        },
        get hasActions() {
            return this.canStop || this.canRelease || this.canFail || this.canRestart;
        },

        get testFraction() {
            const cd = (this.workflow && this.workflow.config_data) || {};
            const tf = cd.test_fraction;
            return (tf && tf < 1) ? tf : null;
        },

        get priorityProfile() {
            const rd = (this.request && this.request.request_data) || {};
            const pp = rd._priority_profile;
            if (pp) return pp;
            // Fall back to workflow config_data
            const cd = (this.workflow && this.workflow.config_data) || {};
            return cd.priority_profile || null;
        },

        get currentJobPriority() {
            const rd = (this.request && this.request.request_data) || {};
            return rd._current_job_priority;
        },

        startEditPriority() {
            const pp = this.priorityProfile || {};
            this.prioHigh = pp.high != null ? pp.high : 5;
            this.prioNominal = pp.nominal != null ? pp.nominal : 3;
            this.prioSwitchFraction = pp.switch_fraction != null ? pp.switch_fraction : 0.5;
            this.editingPriority = true;
        },

        async savePriority() {
            this.actionLoading = true;
            try {
                await WMS2_API.updatePriorityProfile(this.name, {
                    high: parseInt(this.prioHigh),
                    nominal: parseInt(this.prioNominal),
                    switch_fraction: parseFloat(this.prioSwitchFraction),
                });
                this.toast('success', 'Priority profile updated');
                this.editingPriority = false;
                await this.fetchAll();
            } catch (e) {
                this.toast('error', 'Failed to update priority: ' + e.message);
            } finally {
                this.actionLoading = false;
            }
        },

        // Toast helper
        toast(type, message) {
            window.dispatchEvent(new CustomEvent('wms2:toast', {
                detail: { type, message }
            }));
        },

        // Actions
        async doStop() {
            this.actionLoading = true;
            try {
                const result = await WMS2_API.stopRequest(this.name, this.stopReason);
                this.toast('success', result.message);
                this.showStopDialog = false;
                this.stopReason = 'Operator-initiated clean stop';
                await this.fetchAll();
            } catch (e) {
                this.toast('error', 'Stop failed: ' + e.message);
            } finally {
                this.actionLoading = false;
            }
        },

        async doRelease() {
            this.actionLoading = true;
            try {
                const result = await WMS2_API.releaseRequest(this.name);
                this.toast('success', result.message);
                await this.fetchAll();
            } catch (e) {
                this.toast('error', 'Release failed: ' + e.message);
            } finally {
                this.actionLoading = false;
            }
        },

        async doFail() {
            this.actionLoading = true;
            try {
                const result = await WMS2_API.failRequest(this.name);
                this.toast('success', result.message);
                this.showFailDialog = false;
                await this.fetchAll();
            } catch (e) {
                this.toast('error', 'Fail failed: ' + e.message);
            } finally {
                this.actionLoading = false;
            }
        },

        async doRestart() {
            this.actionLoading = true;
            try {
                const result = await WMS2_API.restartRequest(this.name);
                this.toast('success', result.message);
                this.showRestartDialog = false;
                // Redirect to the new request
                if (result.new_request_name) {
                    window.location.href = '/ui/requests/' + encodeURIComponent(result.new_request_name);
                } else {
                    await this.fetchAll();
                }
            } catch (e) {
                this.toast('error', 'Restart failed: ' + e.message);
            } finally {
                this.actionLoading = false;
            }
        },
    }));
});
