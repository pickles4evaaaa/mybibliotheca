/* Repair-job progress UI for the dynamically loaded Settings > Repairs panel. */
(function repairSettingsModule(window) {
    'use strict';

    let pollTimer = null;

    function stopPolling() {
        if (pollTimer) {
            window.clearInterval(pollTimer);
            pollTimer = null;
        }
    }

    function updateRepairStatus(status, job) {
        const countEl = status.querySelector('.repair-job-count');
        const detailEl = status.querySelector('.repair-job-detail');
        const progressEl = status.querySelector('.repair-job-progress');
        const headingEl = status.querySelector('strong');
        const processed = Number(job.processed || 0);
        const total = Number(job.total || 0);
        const updated = Number(job.updated || 0);
        const skipped = Number(job.skipped || 0);
        const failed = Number(job.failed || 0);
        const state = String(job.status || '').toLowerCase();
        const percent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

        if (countEl) countEl.textContent = `${processed}/${total || '…'}`;
        if (progressEl) {
            progressEl.style.width = `${percent}%`;
            progressEl.setAttribute('aria-valuenow', percent);
        }

        status.classList.remove('alert-info', 'alert-success', 'alert-danger');
        if (state === 'completed') {
            status.classList.add('alert-success');
            if (headingEl) headingEl.innerHTML = '<i class="bi bi-check-circle me-1"></i>Repair complete';
            if (detailEl) detailEl.textContent = `${updated} updated, ${skipped} left for manual review${failed ? `, ${failed} failed` : ''}.`;
        } else if (state === 'failed') {
            status.classList.add('alert-danger');
            if (headingEl) headingEl.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>Repair stopped';
            if (detailEl) detailEl.textContent = job.error || 'The repair stopped unexpectedly. Check the application logs.';
        } else {
            status.classList.add('alert-info');
            const phase = job.phase || (state === 'queued' ? 'Queued' : 'Working');
            if (headingEl) headingEl.innerHTML = `<i class="bi bi-hourglass-split me-1"></i>${phase}`;
            if (detailEl) detailEl.textContent = total
                ? 'Working in the background. You can leave this page open or come back later.'
                : 'Preparing the repair and counting affected books…';
        }
        return state;
    }

    window.__initRepairJobProgress = function initRepairJobProgress(root) {
        stopPolling();

        const status = root && root.querySelector('[data-repair-job-status]');
        const statusUrl = status && status.getAttribute('data-status-url');
        if (!status || !statusUrl) return;

        const poll = async () => {
            try {
                const response = await fetch(statusUrl, { credentials: 'same-origin', cache: 'no-store' });
                if (!response.ok) return;

                const payload = await response.json();
                if (!payload.ok || !payload.job) return;

                const state = updateRepairStatus(status, payload.job);
                if (state === 'completed' || state === 'failed') stopPolling();
            } catch (error) {
                console.debug('Repair progress request failed', error);
            }
        };

        poll();
        pollTimer = window.setInterval(poll, 2000);
    };
})(window);
