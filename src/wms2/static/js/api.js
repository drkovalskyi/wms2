/**
 * WMS2 API client — thin fetch wrapper.
 */
const WMS2_API = (() => {
    const prefix = document.querySelector('meta[name="api-prefix"]')?.content || '/api/v1';

    async function get(path) {
        const resp = await fetch(prefix + path);
        if (!resp.ok) {
            const body = await resp.json().catch(() => null);
            throw new Error(body?.detail || `${resp.status} ${resp.statusText}`);
        }
        return resp.json();
    }

    async function post(path, body = {}) {
        const resp = await fetch(prefix + path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!resp.ok) {
            const data = await resp.json().catch(() => null);
            throw new Error(data?.detail || `${resp.status} ${resp.statusText}`);
        }
        return resp.json();
    }

    return {
        // Status & monitoring
        status:            ()           => get('/status'),
        lifecycleStatus:   ()           => get('/lifecycle/status'),
        admissionQueue:    ()           => get('/admission/queue'),

        // Requests
        listRequests:      (params='')  => get('/requests' + (params ? '?' + params : '')),
        getRequest:        (name)       => get('/requests/' + encodeURIComponent(name)),
        getRequestErrors:  (name)       => get('/requests/' + encodeURIComponent(name) + '/errors'),

        // Request actions
        stopRequest:       (name, reason) => post('/requests/' + encodeURIComponent(name) + '/stop', { reason }),
        releaseRequest:    (name)       => post('/requests/' + encodeURIComponent(name) + '/release'),
        failRequest:       (name)       => post('/requests/' + encodeURIComponent(name) + '/fail'),
        restartRequest:    (name)       => post('/requests/' + encodeURIComponent(name) + '/restart'),

        // Workflows
        getWorkflowByRequest: (name)    => get('/workflows/by-request/' + encodeURIComponent(name)),
        getWorkflow:       (id)         => get('/workflows/' + encodeURIComponent(id)),
        getWorkflowBlocks: (id)         => get('/workflows/' + encodeURIComponent(id) + '/blocks'),

        // DAGs
        getDAG:            (id)         => get('/dags/' + encodeURIComponent(id)),
        getDAGHistory:     (id)         => get('/dags/' + encodeURIComponent(id) + '/history'),
        getDAGJobs:        (id)         => get('/dags/' + encodeURIComponent(id) + '/jobs'),

        // Import
        importRequest:     (body)       => post('/import', body),
    };
})();
