const averagePriceEl = document.getElementById('average-price');
const ordersFeed = document.getElementById('orders-feed');
const alertsFeed = document.getElementById('alerts-feed');
const dot = document.getElementById('connection-dot');
const statusText = document.getElementById('connection-text');

let ws;

function connect() {
    ws = new WebSocket(`ws://${window.location.host}/ws`);

    ws.onopen = () => {
        dot.className = 'dot connected';
        statusText.textContent = 'Live Connected';
    };

    ws.onclose = () => {
        dot.className = 'dot disconnected';
        statusText.textContent = 'Disconnected. Reconnecting...';
        setTimeout(connect, 2000);
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMessage(data);
    };
}

function handleMessage(data) {
    switch (data.type) {
        case 'init':
        case 'order_success':
            if (data.average) {
                updateAverage(data.average);
            }
            break;
        case 'order_received':
            addOrder(data.order);
            break;
        case 'order_retry':
            addAlert(data.orderId, `Temporary failure. Retry ${data.retry_count}/${data.max_retries}`, 'retry');
            break;
        case 'order_dlq':
            addAlert(data.orderId, `Moved to DLQ: ${data.reason}`, 'dlq');
            break;
    }
}

// Animation for price update
let currentDisplayPrice = 0;
function updateAverage(newPrice) {
    // Simple fast animation to count up/down to new price
    const duration = 500;
    const start = currentDisplayPrice;
    const end = newPrice;
    const startTime = performance.now();

    function step(timestamp) {
        const progress = Math.min((timestamp - startTime) / duration, 1);
        const current = start + (end - start) * progress;
        averagePriceEl.textContent = current.toFixed(2);
        
        if (progress < 1) {
            requestAnimationFrame(step);
        } else {
            currentDisplayPrice = newPrice;
        }
    }
    requestAnimationFrame(step);
}

function addOrder(order) {
    const el = document.createElement('div');
    el.className = 'feed-item';
    el.innerHTML = `
        <div class="item-header">
            <span class="item-id">ID: ${order.orderId}</span>
            <span class="item-product">${order.product}</span>
        </div>
        <div class="item-price">$${order.price.toFixed(2)}</div>
    `;
    prependToFeed(ordersFeed, el);
}

function addAlert(orderId, message, type) {
    const el = document.createElement('div');
    el.className = `feed-item ${type}`;
    el.innerHTML = `
        <div class="item-header">
            <span class="item-id">ID: ${orderId}</span>
            <span>System Alert</span>
        </div>
        <div class="alert-message">${message}</div>
    `;
    prependToFeed(alertsFeed, el);
}

function prependToFeed(container, element) {
    container.insertBefore(element, container.firstChild);
    
    // Keep max 50 items to prevent memory issues
    if (container.children.length > 50) {
        container.removeChild(container.lastChild);
    }
}

// Start connection
connect();
