/**
 * Alpine settingsPage — displays configuration and lifecycle controls.
 * Supports inline editing of operational parameters.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('settingsPage', () => ({
        settings: null,
        lifecycleRunning: false,
        loading: true,
        error: null,
        restarting: false,
        editing: false,
        saving: false,
        editValues: {},

        // Fields that can be edited (must match server-side EDITABLE_FIELDS)
        editableFields: new Set([
            'lifecycle_cycle_interval', 'max_active_dags',
            'jobs_per_work_unit', 'first_round_work_units',
            'work_units_per_round', 'target_merged_size_kb',
            'error_hold_threshold', 'error_max_rescue_attempts',
            'default_memory_per_core', 'max_memory_per_core', 'safety_margin',
            'site_ban_duration_days', 'site_ban_min_failures', 'site_ban_failure_ratio',
            'log_level', 'default_pilot_priority', 'stageout_mode',
        ]),

        init() {
            this.fetchAll();
            window.addEventListener('wms2:refresh', () => this.fetchAll());
        },

        async fetchAll() {
            try {
                this.loading = true;
                this.error = null;
                const [settings, status] = await Promise.all([
                    WMS2_API.getLifecycleSettings(),
                    WMS2_API.lifecycleStatus().catch(() => null),
                ]);
                this.settings = settings;
                this.lifecycleRunning = status ? status.running : false;
            } catch (e) {
                this.error = e.message;
            } finally {
                this.loading = false;
            }
        },

        toast(type, message) {
            window.dispatchEvent(new CustomEvent('wms2:toast', {
                detail: { type, message }
            }));
        },

        isEditable(field) {
            return this.editableFields.has(field);
        },

        toggleEdit() {
            if (this.editing) {
                this.editing = false;
                this.editValues = {};
            } else {
                // Snapshot current values for editing
                this.editValues = {};
                for (const field of this.editableFields) {
                    if (field in this.settings) {
                        this.editValues[field] = this.settings[field];
                    }
                }
                this.editing = true;
            }
        },

        async doSave() {
            // Collect only changed values
            const changes = {};
            for (const [key, val] of Object.entries(this.editValues)) {
                if (val !== this.settings[key]) {
                    // Coerce types: number fields -> number, string fields -> string
                    if (typeof this.settings[key] === 'number') {
                        changes[key] = Number(val);
                    } else {
                        changes[key] = val;
                    }
                }
            }
            if (Object.keys(changes).length === 0) {
                this.toast('info', 'No changes to save');
                this.editing = false;
                return;
            }
            this.saving = true;
            try {
                await WMS2_API.updateSettings(changes);
                this.toast('success', 'Settings saved, restarting lifecycle manager...');
                await WMS2_API.restartLifecycle();
                this.editing = false;
                this.editValues = {};
                await this.fetchAll();
                this.toast('success', 'Lifecycle manager restarted with new settings');
            } catch (e) {
                this.toast('error', 'Save failed: ' + e.message);
            } finally {
                this.saving = false;
            }
        },

        async doRestart() {
            this.restarting = true;
            try {
                const result = await WMS2_API.restartLifecycle();
                this.toast('success', result.message);
                await this.fetchAll();
            } catch (e) {
                this.toast('error', 'Restart failed: ' + e.message);
            } finally {
                this.restarting = false;
            }
        },
    }));
});
