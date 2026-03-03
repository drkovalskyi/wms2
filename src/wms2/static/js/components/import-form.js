/**
 * Alpine importForm — handles request import form.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('importForm', () => ({
        requestName: '',
        sandboxMode: 'cmssw',
        testFraction: '',
        eventsPerJob: '',
        filesPerJob: '',
        maxFiles: '',
        processingVersion: '',
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
            if (this.eventsPerJob) body.events_per_job = parseInt(this.eventsPerJob);
            if (this.filesPerJob) body.files_per_job = parseInt(this.filesPerJob);
            if (this.maxFiles) body.max_files = parseInt(this.maxFiles);
            if (this.processingVersion) body.processing_version = parseInt(this.processingVersion);
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
