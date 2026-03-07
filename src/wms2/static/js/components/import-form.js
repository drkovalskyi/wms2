/**
 * Alpine importForm — handles request import form with ReqMgr2 preview.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('importForm', () => ({
        requestName: '',
        sandboxMode: 'cmssw',
        testFraction: '',
        requestNumEvents: '',
        eventsPerJob: '',
        filesPerJob: '',
        maxFiles: '',
        processingVersion: '',
        workUnitsPerRound: '',
        condorPool: 'local',
        allowedSites: '',
        highPriority: 5,
        nominalPriority: 3,
        prioritySwitchFraction: 0.5,
        dryRun: false,
        submitting: false,
        error: null,
        progressStep: '',
        _progressTimer: null,
        showReplaceDialog: false,
        existingStatus: '',

        // Preview state
        preview: null,
        previewLoading: false,
        previewError: null,

        // Steps shown during import (timed estimates)
        _progressSteps: [
            { msg: 'Fetching request from ReqMgr2...', delay: 0 },
            { msg: 'Creating sandbox and extracting config...', delay: 3000 },
            { msg: 'Resolving input data and pileup...', delay: 8000 },
            { msg: 'Planning DAG and writing submit files...', delay: 15000 },
            { msg: 'Submitting DAG to HTCondor...', delay: 25000 },
            { msg: 'Still working — large requests take longer...', delay: 45000 },
        ],

        get isValid() {
            return this.requestName.trim().length > 0;
        },

        get estimatedTotalWUs() {
            if (!this.preview || !this.preview.is_gen) return null;
            const p = this.preview;
            let totalEvents = parseInt(this.requestNumEvents) || p.request_num_events || 0;
            if (!totalEvents) return null;
            const filterEff = p.filter_efficiency || 1.0;
            if (filterEff < 1.0) totalEvents = totalEvents / filterEff;
            const epj = parseInt(this.eventsPerJob) || p.events_per_job || 100000;
            const numJobs = Math.ceil(totalEvents / epj);
            const jpwu = p.defaults.jobs_per_work_unit;
            return Math.ceil(numJobs / jpwu);
        },

        get estimatedRounds() {
            const totalWUs = this.estimatedTotalWUs;
            if (totalWUs === null) return null;
            const firstRoundWUs = this.preview.defaults.first_round_work_units;
            const wupr = parseInt(this.workUnitsPerRound) || this.preview.defaults.work_units_per_round;
            const remaining = totalWUs - firstRoundWUs;
            if (remaining <= 0) return 1;
            return 1 + Math.ceil(remaining / wupr);
        },

        async loadPreview() {
            const name = this.requestName.trim();
            if (!name || this.previewLoading) return;
            this.preview = null;
            this.previewError = null;
            this.previewLoading = true;
            try {
                this.preview = await WMS2_API.previewRequest(name);
                // Auto-populate form fields from ReqMgr2
                if (this.preview.request_num_events && !this.requestNumEvents)
                    this.requestNumEvents = this.preview.request_num_events;
                if (this.preview.events_per_job && !this.eventsPerJob)
                    this.eventsPerJob = this.preview.events_per_job;
                if (this.preview.files_per_job && !this.filesPerJob)
                    this.filesPerJob = this.preview.files_per_job;
                if (this.preview.processing_version && !this.processingVersion)
                    this.processingVersion = this.preview.processing_version;
                if (!this.workUnitsPerRound)
                    this.workUnitsPerRound = this.preview.defaults.work_units_per_round;
            } catch (e) {
                this.previewError = e.message;
            } finally {
                this.previewLoading = false;
            }
        },

        _startProgress() {
            this._timers = [];
            for (const step of this._progressSteps) {
                const t = setTimeout(() => { this.progressStep = step.msg; }, step.delay);
                this._timers.push(t);
            }
        },

        _stopProgress() {
            if (this._timers) {
                this._timers.forEach(t => clearTimeout(t));
                this._timers = [];
            }
            this.progressStep = '';
        },

        _buildBody(replace = false) {
            const body = {
                request_name: this.requestName.trim(),
                sandbox_mode: this.sandboxMode,
                dry_run: this.dryRun,
            };
            if (this.testFraction) body.test_fraction = parseFloat(this.testFraction);
            if (this.requestNumEvents) body.request_num_events = parseInt(this.requestNumEvents);
            if (this.eventsPerJob) body.events_per_job = parseInt(this.eventsPerJob);
            if (this.filesPerJob) body.files_per_job = parseInt(this.filesPerJob);
            if (this.maxFiles) body.max_files = parseInt(this.maxFiles);
            if (this.processingVersion) body.processing_version = parseInt(this.processingVersion);
            if (this.workUnitsPerRound) body.work_units_per_round = parseInt(this.workUnitsPerRound);
            body.condor_pool = this.condorPool;
            if (this.condorPool === 'global' && this.allowedSites.trim()) {
                body.allowed_sites = this.allowedSites.trim();
            }
            body.high_priority = parseInt(this.highPriority);
            body.nominal_priority = parseInt(this.nominalPriority);
            body.priority_switch_fraction = parseFloat(this.prioritySwitchFraction);
            if (replace) body.replace = true;
            return body;
        },

        async submit() {
            if (!this.isValid || this.submitting) return;
            this.submitting = true;
            this.error = null;
            this._startProgress();

            try {
                const result = await WMS2_API.importRequest(this._buildBody());
                window.dispatchEvent(new CustomEvent('wms2:toast', {
                    detail: { type: 'success', message: result.message }
                }));
                window.location.href = '/ui/requests/' + encodeURIComponent(result.request_name);
            } catch (e) {
                if (e.status === 409) {
                    const m = e.message.match(/status:\s*(\w+)/);
                    const status = m ? m[1] : 'unknown';
                    if (status === 'failed' || status === 'aborted') {
                        this.existingStatus = status;
                        this.showReplaceDialog = true;
                    } else {
                        this.error = e.message;
                        window.dispatchEvent(new CustomEvent('wms2:toast', {
                            detail: { type: 'error', message: e.message }
                        }));
                    }
                } else {
                    this.error = e.message;
                    window.dispatchEvent(new CustomEvent('wms2:toast', {
                        detail: { type: 'error', message: 'Import failed: ' + e.message }
                    }));
                }
            } finally {
                this._stopProgress();
                this.submitting = false;
            }
        },

        async doReplace() {
            this.showReplaceDialog = false;
            this.submitting = true;
            this.error = null;
            this._startProgress();

            try {
                const result = await WMS2_API.importRequest(this._buildBody(true));
                window.dispatchEvent(new CustomEvent('wms2:toast', {
                    detail: { type: 'success', message: result.message }
                }));
                window.location.href = '/ui/requests/' + encodeURIComponent(result.request_name);
            } catch (e) {
                this.error = e.message;
                window.dispatchEvent(new CustomEvent('wms2:toast', {
                    detail: { type: 'error', message: 'Import failed: ' + e.message }
                }));
            } finally {
                this._stopProgress();
                this.submitting = false;
            }
        },
    }));
});
