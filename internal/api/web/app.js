// Дашборд Sentinel. Данные приходят SSE-потоком /api/stream:
// EventSource сам переподключается при обрывах — это его киллер-фича
// против самодельных setInterval-поллеров.

const grid = document.getElementById("grid");
const empty = document.getElementById("empty");
const dot = document.getElementById("conn-dot");
const connText = document.getElementById("conn-text");
const updated = document.getElementById("updated");

// Свежесть инцидента -> цвет карточки. last_seen пришёл из Mongo.
function freshness(lastSeen) {
    const age = (Date.now() - new Date(lastSeen)) / 1000;
    if (age < 5 * 60) return "red";      // горело меньше 5 минут назад
    if (age < 60 * 60) return "yellow";  // в последний час
    return "green";                      // остывшее
}

function esc(s) {
    return String(s).replace(/[&<>"]/g, ch =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
}

function render(list) {
    grid.replaceChildren();
    empty.classList.toggle("hidden", list.length > 0);
    for (const inc of list) {
        const card = document.createElement("div");
        card.className = "card " + freshness(inc.last_seen);
        card.innerHTML = `
            <span class="count">×${inc.count}</span>
            <div class="kind">${esc(inc.kind)}</div>
            <div class="pod">${esc(inc.namespace)}/${esc(inc.pod)}</div>
            <div class="meta">
                контейнер: ${esc(inc.container || "—")} ·
                причина: ${esc(inc.reason || "—")} ·
                рестартов: ${inc.restarts}<br>
                замечен: ${new Date(inc.first_seen).toLocaleString()}<br>
                последний раз: ${new Date(inc.last_seen).toLocaleString()}
            </div>`;
        grid.appendChild(card);
    }
    updated.textContent = new Date().toLocaleTimeString();
}

function setState(on, text) {
    dot.className = "dot " + (on ? "on" : "off");
    connText.textContent = text;
}

// Первичная отрисовка — не ждём первый тик SSE.
fetch("/api/incidents").then(r => r.json()).then(render).catch(() => { });

const es = new EventSource("/api/stream");
es.onopen = () => setState(true, "живое обновление");
es.onerror = () => setState(false, "переподключение…");
es.onmessage = (ev) => {
    setState(true, "живое обновление");
    render(JSON.parse(ev.data));
};
