/**
 * Alpine settingsPage — displays configuration and lifecycle controls.
 */
document.addEventListener('alpine:init', () => {
    Alpine.data('settingsPage', () => ({
        settings: null,
        lifecycleRunning: false,
        loading: true,
        error: null,
        restarting: false,

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
