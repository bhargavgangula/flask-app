const startButton = document.getElementById('start-button');
const stopButton = document.getElementById('stop-button');
const downloadButton = document.getElementById('download-csv');

const statusMessage = document.getElementById('status-message');
const linkProgressBar = document.getElementById('link-progress-bar');
const detailProgressBar = document.getElementById('detail-progress-bar');
const linksFound = document.getElementById('links-found');
const scrapedCount = document.getElementById('scraped-count');
const resultsBody = document.getElementById('results-body');

let statusInterval;

function startPollingStatus() {
    if (statusInterval) clearInterval(statusInterval);
    statusInterval = setInterval(async () => {
        try {
            const response = await fetch('/status');
            const data = await response.json();
            updateUI(data);

            if (!data.scraping_active && startButton.disabled) {
                stopPollingStatus();
                startButton.disabled = false;
                stopButton.disabled = true;
                if(data.results_df && data.results_df.length > 0) {
                   downloadButton.disabled = false;
                }
            }
        } catch (error) {
            console.error('Error polling status:', error);
            statusMessage.textContent = 'Error connecting to server. Polling stopped.';
            stopPollingStatus();
        }
    }, 2000);
}

function stopPollingStatus() {
    clearInterval(statusInterval);
    statusInterval = null;
}

function updateUI(data) {
    statusMessage.textContent = data.status_message;

    const linkProgress = (data.link_collection_progress * 100).toFixed(1);
    linkProgressBar.style.width = `${linkProgress}%`;
    linkProgressBar.textContent = `${linkProgress}%`;

    const detailProgress = (data.detail_scraping_progress * 100).toFixed(1);
    detailProgressBar.style.width = `${detailProgress}%`;
    detailProgressBar.textContent = `${detailProgress}%`;

    linksFound.textContent = data.link_count;
    scrapedCount.textContent = `${data.scraped_count} / ${data.total_to_scrape}`;
}

async function fetchAndDisplayResults() {
    try {
        const response = await fetch('/get-results');
        const results = await response.json();
        resultsBody.innerHTML = '';
        results.forEach(item => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${item.Name || ''}</td>
                <td>${item.Address || ''}</td>
                <td>${item.Website ? `<a href="${item.Website}" target="_blank">${item.Website}</a>` : ''}</td>
                <td>${item["Final Email"] || ''}</td>   <!-- ✅ FIXED -->
                <td>${item.Category || ''}</td>
            `;
            resultsBody.appendChild(row);
        });
        if(results.length > 0) {
            downloadButton.disabled = false;
        }
    } catch (error) {
        console.error('Error fetching results:', error);
    }
}

startButton.addEventListener('click', async () => {
    const config = {
        general_search_term: document.getElementById('general-search-term').value,
        categories: document.getElementById('categories').value.split(',').map(c => c.trim()).filter(Boolean),
        zipcodes: document.getElementById('zipcodes').value.split('\n').map(z => z.trim()).filter(Boolean),
        max_workers: parseInt(document.getElementById('max-workers').value),
        max_scrolls: parseInt(document.getElementById('max-scrolls').value),
        scroll_pause: parseFloat(document.getElementById('scroll-pause').value),
        scrape_timeout: parseInt(document.getElementById('scrape-timeout').value),
        headless_mode: document.getElementById('headless-mode').value === 'true',
    };

    startButton.disabled = true;
    stopButton.disabled = false;
    downloadButton.disabled = true;
    resultsBody.innerHTML = '';

    try {
        const response = await fetch('/start-scraping', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        const data = await response.json();
        statusMessage.textContent = data.message;
        if (response.ok) {
            startPollingStatus();
        } else {
            startButton.disabled = false;
            stopButton.disabled = true;
        }
    } catch (error) {
        console.error('Error starting scraping:', error);
        statusMessage.textContent = 'Failed to start scraping. Check console for errors.';
        startButton.disabled = false;
        stopButton.disabled = true;
    }
});

stopButton.addEventListener('click', async () => {
    try {
        const response = await fetch('/stop-scraping', { method: 'POST' });
        const data = await response.json();
        statusMessage.textContent = data.message;
        stopButton.disabled = true;
    } catch (error) {
        console.error('Error stopping scraping:', error);
        statusMessage.textContent = 'Failed to send stop signal.';
    }
});

downloadButton.addEventListener('click', () => {
    window.location.href = '/download-csv';
});

setInterval(fetchAndDisplayResults, 10000);
