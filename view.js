/* KinoLink — страница зрителя (режиссёр). Один файл, без установки.
   Мультизрители: у каждой вкладки свой случайный viewerId; объявления зрителя
   идут в kl/<code>/v/<id>, его личный оффер — из kl/<code>/o/<id>,
   ответ публикуется в kl/<code>/a/<id>. */
'use strict';

window.__vdiag = { stage: 'boot', err: [] };
window.onerror = (m, s, l, c) => { window.__vdiag.err.push(String(m) + ' @' + l + ':' + c); };

const $ = (id) => document.getElementById(id);
const MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
const STUN_LIST = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.yandex.net:3478' },
  { urls: 'stun:stun.3cx.com:3478' },
  { urls: 'stun:stun.voiparound.com:3478' },
  { urls: ['turn:openrelay.metered.ca:443?transport=tcp',
           'turn:openrelay.metered.ca:80?transport=tcp',
           'turn:openrelay.metered.ca:443',
           'turn:openrelay.metered.ca:80'],
    username: 'openrelayproject', credential: 'openrelayproject' },
  { urls: ['turn:freestun.net:3478?transport=tcp', 'turn:freestun.net:3478'],
    username: 'free', credential: 'free' }
];

/* У каждой вкладки/загрузки — свой id зрителя. Намеренно НЕ сохраняется:
   перезагрузка страницы = новый зритель, отправитель создаст свежий эфир. */
const VIEWER_ID = 'v' + Math.random().toString(36).slice(2, 9) + Date.now().toString(36).slice(-4);
window.__vdiag.vid = VIEWER_ID;

/* MQTT-клиент: предпочитаем полную библиотеку mqtt.js (mqtt.min.js рядом со
   страницей) — она сама держит keepalive, переподписывается и, главное,
   очередит исходящие публикации (queueQoSzero) на время обрыва связи.
   Мини-клиент ниже — запасной вариант, если библиотека не загрузилась. */
function klMqttConnect(url, opts) {
  if (typeof window !== 'undefined' && window.mqtt && typeof window.mqtt.connect === 'function') {
    return window.mqtt.connect(url, {
      clientId: opts.clientId,
      connectTimeout: opts.connectTimeout || 10000,
      reconnectPeriod: opts.reconnectPeriod || 4000,
      keepalive: 25,
      clean: true,
      resubscribe: true,
      queueQoSzero: true   // ответ режиссёру не потеряется даже в окне обрыва
    });
  }
  return klMqttMini(url, opts);
}

/* Минимальный MQTT 3.1.1 поверх WebSocket (QoS 0) — запасной вариант.
   API совместим с используемым здесь подмножеством mqtt.js: connect/on/subscribe/publishAsync/connected. */
function klMqttMini(url, opts) {
  const enc = new TextEncoder(), dec = new TextDecoder();
  const handlers = {};
  let ws = null, connected = false, killed = false, pktId = 1, pingT = null, connT = null, recT = null;
  const api = {
    get connected() { return connected; },
    on(ev, fn) { (handlers[ev] = handlers[ev] || []).push(fn); return api; },
    subscribe(topic) {
      const t = enc.encode(topic);
      const body = new Uint8Array(2 + 2 + t.length + 1);
      body[0] = pktId >> 8; body[1] = pktId++ & 255;
      body[2] = t.length >> 8; body[3] = t.length & 255; body.set(t, 4); body[4 + t.length] = 0;
      send(packet(0x82, body));
    },
    async publishAsync(topic, payload) {
      const t = enc.encode(topic), d = enc.encode(String(payload));
      const body = new Uint8Array(2 + t.length + d.length);
      body[0] = t.length >> 8; body[1] = t.length & 255; body.set(t, 2); body.set(d, 2 + t.length);
      send(packet(0x30, body));
    },
    end() { killed = true; clearTimeout(recT); try { ws && ws.close(); } catch (_) {} }
  };
  function emit(ev, a, b) { (handlers[ev] || []).forEach((f) => { try { f(a, b); } catch (_) {} }); }
  function packet(typeFlags, body) {
    let n = body.length, len = [];
    do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 0x80; len.push(d); } while (n > 0);
    const out = new Uint8Array(1 + len.length + body.length);
    out[0] = typeFlags; out.set(len, 1); out.set(body, 1 + len.length);
    return out;
  }
  function send(u8) { if (ws && ws.readyState === 1) ws.send(u8); }
  function parseVarLen(d, p) { let m = 1, v = 0, b; do { b = d[p++]; v += (b & 127) * m; m *= 128; } while (b & 128); return [v, p]; }
  function onPacket(d) {
    const type = d[0] >> 4;
    const [rl, p0] = parseVarLen(d, 1);
    const p = p0;
    if (type === 2) { // CONNACK
      if (d[p + 1] === 0) {
        connected = true;
        clearTimeout(connT);
        pingT = setInterval(() => send(new Uint8Array([0xC0, 0x00])), 30000);
        emit('connect');
      } else {
        emit('error', new Error('MQTT refused, code ' + d[p + 1]));
      }
    } else if (type === 3) { // PUBLISH (QoS 0)
      const tl = (d[p] << 8) | d[p + 1];
      const topic = dec.decode(d.subarray(p + 2, p + 2 + tl));
      emit('message', topic, dec.decode(d.subarray(p + 2 + tl)));
    } else if (type === 13) { /* PINGRESP */ }
  }
  function open() {
    ws = new WebSocket(url, 'mqtt');
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      const c = enc.encode(opts.clientId || 'kl_' + Math.random().toString(36).slice(2, 10));
      const vh = enc.encode('MQTT');
      const body = new Uint8Array(2 + vh.length + 4 + 2 + c.length);
      let o = 0;
      body[o++] = vh.length >> 8; body[o++] = vh.length & 255; body.set(vh, o); o += vh.length;
      body[o++] = 4;        // уровень протокола MQTT 3.1.1
      body[o++] = 2;        // clean session
      body[o++] = 0; body[o++] = 60; // keepalive
      body[o++] = c.length >> 8; body[o++] = c.length & 255; body.set(c, o);
      send(packet(0x10, body));
      clearTimeout(connT);
      connT = setTimeout(() => { try { ws.close(); } catch (_) {} }, opts.connectTimeout || 9000);
    };
    ws.onmessage = (ev) => onPacket(new Uint8Array(ev.data));
    ws.onerror = () => emit('error', new Error('ошибка WebSocket'));
    ws.onclose = () => {
      clearInterval(pingT); clearTimeout(connT);
      if (connected) { connected = false; emit('offline'); }
      if (!killed) recT = setTimeout(open, opts.reconnectPeriod || 4000);
    };
  }
  open();
  return api;
}

const b64d = (s) => new TextDecoder().decode(Uint8Array.from(
  atob(s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - s.length % 4) % 4)),
  (c) => c.charCodeAt(0)));
const b64e = (s) => btoa(String.fromCharCode(...new TextEncoder().encode(s)))
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

const hash = new URLSearchParams(location.hash.slice(1));
const code = (hash.get('c') || '').replace(/[^A-Za-z0-9_-]/g, '');
const manualOffer = hash.get('o');

let pc = null;
let inboundStream = null;
let mc = null;
let live = null;
let busy = false;
let gotStream = false;
let curOfferVer = null;
let discoTimer = null;
let pendingAnswer = null;   // ответ, ожидающий доставки отправителю
let ansTimer = null;

/* Объявление «я смотрю эфир»: отправитель по этому сигналу создаёт для нас
   личный peer connection и публикует оффер в наш топик. Повторяем периодически
   — отправитель по этим же сообщениям понимает, что мы ещё на странице. */
function announceViewer() {
  if (!code || !mc) return;
  try { mc.publishAsync(`kl/${code}/v/${VIEWER_ID}`, '1').catch(() => {}); } catch (_) {}
}

function publishAnswer() {
  if (!pendingAnswer || !mc) return;
  // retain=true: отправитель подхватит ответ при ЛЮБОМ переподключении —
  // живые публикации на ненадёжном канале до него часто не доходят.
  // qos=1: брокер подтвердит приём, а клиент повторит неподтверждённое —
  // иначе ответ может уйти в «полумёртвый» сокет и пропасть бесследно.
  mc.publishAsync(`kl/${code}/a/${VIEWER_ID}`, pendingAnswer, { retain: true, qos: 1 }).catch(() => {});
}
function startAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = setInterval(publishAnswer, 8000);
}
function stopAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = null;
  pendingAnswer = null;
}
// Разрыв эфира НЕ должен отменять доставку ответа: отправитель может
// ожить через минуту и подхватить retained-ответ — поэтому здесь только
// гасим таймер периодики, сам ответ и цикл публикации сохраняются.
function pauseAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = null;
}

function teardownToWaiting() {
  gotStream = false;
  stopViewerStats();
  pauseAnswerRetransmit();
  // ретрансляцию продолжаем: publishAnswer вызывается и из цикла waitForOffer
  startAnswerRetransmit();
  try { if (pc) pc.close(); } catch (_) {}
  pc = null;
  $('player').style.display = 'none';
  $('topbar').style.display = 'none';
  $('stage').style.display = 'flex';
  $('playBtn').style.display = 'none';
  if (code) {
    big('Связь прервана.', 'Не закрывайте страницу — подключимся снова, как только эфир возобновится.');
    waitForOffer();
  } else {
    big('Связь прервана.', 'Обновите страницу или попросите новую ссылку.');
  }
}

/* Показать, каким путём пошёл трафик (direct/relay) — для диагностики */
function showConnPath() {
  setTimeout(async () => {
    try {
      const st = await pc.getStats();
      st.forEach((r) => {
        if (r.type === 'candidate-pair' && r.nominated && r.state === 'succeeded') {
          const l = st.get(r.localCandidateId), rm = st.get(r.remoteCandidateId);
          const how = (l ? l.candidateType : '?') + ' → ' + (rm ? rm.candidateType : '?');
          window.__vdiag.path = how;
        }
      });
    } catch (_) {}
  }, 2500);
}

/* Живой индикатор в панели: сколько видео/звука реально доходит.
   Если цифры стоят на нуле — проблема в сети, а не в телефоне. */
let vStatsTimer = null, vLastVideo = 0, vLastAudio = 0, vZeroSince = 0;
function stopViewerStats() { clearInterval(vStatsTimer); vStatsTimer = null; }
function startViewerStats() {
  stopViewerStats();
  vLastVideo = 0; vLastAudio = 0; vZeroSince = Date.now();
  const tag = $('netTag');
  vStatsTimer = setInterval(async () => {
    if (!pc) { stopViewerStats(); return; }
    try {
      const st = await pc.getStats();
      let vb = 0, ab = 0;
      st.forEach((r) => {
        if (r.type === 'inbound-rtp' && r.kind === 'video') vb += r.bytesReceived || 0;
        if (r.type === 'inbound-rtp' && r.kind === 'audio') ab += r.bytesReceived || 0;
      });
      const vkbps = Math.round((vb - vLastVideo) * 8 / 1000 / 2);
      const akbps = Math.round((ab - vLastAudio) * 8 / 1000 / 2);
      vLastVideo = vb; vLastAudio = ab;
      const path = window.__vdiag.path || '';
      if (vb + ab > 0) vZeroSince = 0;
      if (tag) {
        if (vb + ab === 0 && vZeroSince && Date.now() - vZeroSince > 6000) {
          tag.textContent = '⚠ видео не доходит (' + (path || 'канал неизвестен') + ')';
          tag.style.color = '#ff8484';
        } else {
          tag.textContent = '▸ ' + vkbps + ' кбит/с видео · ' + akbps + ' кбит/с звук' + (path ? ' · ' + path : '');
          tag.style.color = '';
        }
      }
    } catch (_) {}
  }, 2000);
}

function big(msg, sub) {
  $('bigmsg').textContent = msg;
  $('submsg').textContent = sub || '';
}
function showPlayer() {
  $('stage').style.display = 'none';
  $('player').style.display = 'block';
  $('topbar').style.display = 'flex';
  $('playBtn').style.display = 'none';
}
function gatherComplete(p, ms = 9000) {
  if (p.iceGatheringState === 'complete') return Promise.resolve();
  return new Promise((res) => {
    const to = setTimeout(res, ms);
    p.addEventListener('icegatheringstatechange', () => {
      if (p.iceGatheringState === 'complete') { clearTimeout(to); res(); }
    });
  });
}

async function connectToOffer(offerB64) {
  if (busy) return;
  busy = true;
  stopAnswerRetransmit(); // старый ответ неактуален — строим новый
  try {
    if (pc) { try { pc.close(); } catch (_) {} pc = null; }
    $('spinner').style.display = 'block';
    big('Подключаемся к эфиру…', 'Обычно занимает 2–5 секунд.');
    window.__vdiag.stage = 'got offer, connecting pc';
    const offer = JSON.parse(b64d(offerB64));
    pc = new RTCPeerConnection({ iceServers: STUN_LIST });
    // Собираем входящий поток вручную: sendonly-трансиверы без msid могут
    // давать пустой ev.streams — опираемся на сами треки.
    inboundStream = new MediaStream();
    pc.ontrack = (ev) => {
      inboundStream.addTrack(ev.track);
      gotStream = true;
      $('player').srcObject = inboundStream;
      $('spinner').style.display = 'none';
      $('playBtn').style.display = 'inline-block';
      big('Эфир готов!', '');
    };
    pc.onconnectionstatechange = () => {
      const st = pc.connectionState;
      if (st === 'connected') {
        clearTimeout(discoTimer);
        stopAnswerRetransmit();
        big('Эфир готов!', '');
        showConnPath();
        startViewerStats();
      } else if (st === 'disconnected') {
        // короткие обрывы на мобильных сетях/VPN часто восстанавливаются сами
        big('Связь прерывается…', 'Восстанавливаем автоматически, ничего нажимать не нужно.');
        clearTimeout(discoTimer);
        discoTimer = setTimeout(() => teardownToWaiting(), 15000);
      } else if (st === 'failed' || st === 'closed') {
        clearTimeout(discoTimer);
        teardownToWaiting();
      }
    };
    await pc.setRemoteDescription(offer);
    window.__vdiag.stage = 'offer applied';
    await pc.setLocalDescription(await pc.createAnswer());
    window.__vdiag.stage = 'answer created';
    window.__answerB64 = b64e(JSON.stringify(pc.localDescription));
    await gatherComplete(pc);
    if (code && mc) {
      // Публикуем в любом состоянии: полный клиент поставит в очередь
      // (queueQoSzero), мини-клиент проигнорирует — но startAnswerRetransmit
      // и publishAnswer при reconnect всё равно довезут.
      pendingAnswer = b64e(JSON.stringify({ v: curOfferVer, s: JSON.stringify(pc.localDescription) }));
      // Не ждём: на полузамёрзшем сокете publishAsync может висеть вечно,
      // а busy держит весь цикл. Ретранслятор (каждые 8 с) довезёт сам.
      try { mc.publishAsync(`kl/${code}/a/${VIEWER_ID}`, pendingAnswer, { retain: true, qos: 1 }).catch(() => {}); } catch (_) {}
      window.__vdiag.stage = 'answer queued, connected=' + mc.connected;
      startAnswerRetransmit(); // пока не подключимся — периодически дублируем ответ
      big('Ответ отправлен.', 'Устанавливаем соединение…');
    } else {
      const ans = 'KLANS1.' + b64e(JSON.stringify(pc.localDescription));
      $('answerOut').value = ans;
      $('answerOut').style.display = 'block';
      $('copyAnswer').style.display = 'inline-block';
      $('playBtn').style.display = 'none';
      big('Эфир подготовлен.', 'Пришлите код ниже тому, кто дал вам ссылку — например, в Telegram или WhatsApp.');
      try { await navigator.clipboard.writeText(ans); $('copyAnswer').textContent = '✔ Скопировано'; } catch (_) {}
    }
  } catch (e) {
    window.__vdiag.err.push('connectToOffer: ' + String(e && e.message || e));
    big('Не удалось подключиться.', String(e.message || e));
    if (code) setTimeout(waitForOffer, 4000);
  } finally {
    busy = false;
  }
}

/* Ожидание объявления эфира и нашего личного предложения (автоматический режим) */
let waitingTimer = null;
let lastMqttActivity = 0;   // сторож: соединение может быть "полузамёрзшим"
function mqttTouch() { lastMqttActivity = Date.now(); }
function waitForOffer() {
  if (!code || busy) return;
  clearTimeout(waitingTimer);
  window.__vdiag.stage = 'waitForOffer';
  // Сторож: connected=true ещё не значит живое соединение (WebSocket может
  // умереть молча). Если 45 с никакого трафика — пересоздаём клиента.
  if (mc && mc.connected && lastMqttActivity && Date.now() - lastMqttActivity > 45000) {
    try { mc.end(true); } catch (_) {}
    mc = null;
    window.__vdiag.stage = 'mqtt zombie-killed';
  }
  if (!mc || !mc.connected) {
    big('Соединяемся…', '');
    window.__vdiag.stage = 'mqtt connecting, klMqttConnect=' + typeof klMqttConnect;
    mc = klMqttConnect(MQTT_URL, {
      clientId: 'klview_' + Math.random().toString(36).slice(2, 12),
      connectTimeout: 9000,
      reconnectPeriod: 2000
    });
    mqttTouch();
    mc.on('connect', () => {
      mqttTouch();
      try { mc.options.reconnectPeriod = 2000; } catch (_) {}
      window.__vdiag.stage = 'mqtt connected';
      mc.subscribe(`kl/${code}/live`);
      mc.subscribe(`kl/${code}/o/${VIEWER_ID}`);
      announceViewer(); // сразу объявляемся — отправитель построит нам эфир
      publishAnswer(); // если ответ ещё не дошёл — отправим снова
    });
    mc.on('message', (topic, payload) => {
      mqttTouch();
      const data = payload.toString();
      if (topic === `kl/${code}/live`) {
        live = data === '1';
        if (live) {
          big('Эфир начинается…', 'Подключаемся.');
        } else {
          $('player').style.display = 'none';
          $('topbar').style.display = 'none';
          $('stage').style.display = 'flex';
          $('playBtn').style.display = 'none';
          $('spinner').style.display = 'block';
          big('Эфир сейчас не идёт.', 'Оставьте страницу открытой — как только начнётся показ, видео появится само.');
        }
      }
      if (topic === `kl/${code}/o/${VIEWER_ID}` && data.length > 40) {
        // Формат: KL1.<версия>.<b64 SDP>. Повторный оффер той же версии при
        // живом соединении игнорируем — не рвём рабочий эфир из-за реконнекта брокера.
        let ver = 'legacy', payload = data;
        if (data.startsWith('KL1.')) {
          const parts = data.split('.');
          ver = parts[1] || 'x';
          payload = parts.slice(2).join('.');
        }
        const healthy = pc && pc.connectionState === 'connected';
        if (ver === curOfferVer && healthy) return;
        curOfferVer = ver;
        connectToOffer(payload);
      }
    });
    mc.on('error', (e) => {
      window.__vdiag.err.push('mqtt error: ' + String(e && e.message || e));
      big('Проблема с соединением.', 'Проверьте интернет и обновите страницу.');
    });
    mc.on('offline', () => {
      window.__vdiag.err.push('mqtt offline');
      // Бэкофф переподключений: не держать себя в бане у публичного брокера
      try { mc.options.reconnectPeriod = Math.min(30000, (mc.options.reconnectPeriod || 2000) * 2); } catch (_) {}
    });
    mc.on('message', (topic) => { window.__vdiag.lastTopic = topic; });
  }
  // Пока ждём и не подключены — периодически переподписываемся:
  // брокер заново присылает retained-оффер, так мы подхватим свежую версию
  // после автопересоздания эфира на стороне отправителя. Заодно повторяем
  // объявление зрителя — отправитель по нему узнаёт, что мы всё ещё тут.
  if (mc && mc.connected && !(pc && pc.connectionState === 'connected')) {
    mc.subscribe(`kl/${code}/o/${VIEWER_ID}`);
    mc.subscribe(`kl/${code}/live`);
    announceViewer();
  }
  waitingTimer = setTimeout(waitForOffer, 15000);
}

/* ---------- Старт ---------- */
if (manualOffer && manualOffer.length > 40) {
  $('codeBox').innerHTML = '';
  connectToOffer(manualOffer);
} else if (code) {
  $('codeBox').innerHTML = 'Код эфира: <b>' + code + '</b>';
  if (typeof window.KL_TEST_NO_MQTT === 'undefined' || !window.KL_TEST_NO_MQTT) {
    waitForOffer();
  } else {
    big('Тестовый режим — ждём предложения…', '');
  }
} else {
  $('spinner').style.display = 'none';
  big('Ссылка неполная.', 'Попросите ссылку у отправителя ещё раз.');
}

/* ---------- Управление воспроизведением ---------- */
async function tryPlay() {
  const v = $('player');
  try {
    await v.play(); // со звуком
    return true;
  } catch (e1) {
    try {
      v.muted = true;               // мобильные часто требуют жест для звука —
      await v.play();               // запускаем беззвучно, потом возвращаем звук
      try { v.muted = false; } catch (_) {}
      return true;
    } catch (e2) {
      return false;
    }
  }
}
$('playBtn').onclick = async () => {
  if (!(await tryPlay())) {
    big('Браузер блокирует воспроизведение.', 'Нажмите на чёрный экран ещё раз.');
    return;
  }
  showPlayer();
};
$('player').onclick = async () => {
  if ($('player').paused) { try { await $('player').play(); } catch (_) {} }
};
$('copyAnswer').onclick = async () => {
  try {
    await navigator.clipboard.writeText($('answerOut').value);
    $('copyAnswer').textContent = '✔ Скопировано';
  } catch (_) {
    $('answerOut').select();
    document.execCommand('copy');
  }
};
$('fsBtn').onclick = () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else document.documentElement.requestFullscreen().catch(() => {});
};
/* KinoLink — страница зрителя (режиссёр). Один файл, без установки. */
'use strict';

window.__vdiag = { stage: 'boot', err: [] };
window.onerror = (m, s, l, c) => { window.__vdiag.err.push(String(m) + ' @' + l + ':' + c); };

const $ = (id) => document.getElementById(id);
const MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
const STUN_LIST = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.yandex.net:3478' },
  { urls: 'stun:stun.3cx.com:3478' },
  { urls: 'stun:stun.voiparound.com:3478' },
  { urls: ['turn:openrelay.metered.ca:443?transport=tcp',
           'turn:openrelay.metered.ca:80?transport=tcp',
           'turn:openrelay.metered.ca:443',
           'turn:openrelay.metered.ca:80'],
    username: 'openrelayproject', credential: 'openrelayproject' },
  { urls: ['turn:freestun.net:3478?transport=tcp', 'turn:freestun.net:3478'],
    username: 'free', credential: 'free' }
];

/* MQTT-клиент: предпочитаем полную библиотеку mqtt.js (mqtt.min.js рядом со
   страницей) — она сама держит keepalive, переподписывается и, главное,
   очередит исходящие публикации (queueQoSzero) на время обрыва связи.
   Мини-клиент ниже — запасной вариант, если библиотека не загрузилась. */
function klMqttConnect(url, opts) {
  if (typeof window !== 'undefined' && window.mqtt && typeof window.mqtt.connect === 'function') {
    return window.mqtt.connect(url, {
      clientId: opts.clientId,
      connectTimeout: opts.connectTimeout || 10000,
      reconnectPeriod: opts.reconnectPeriod || 4000,
      keepalive: 25,
      clean: true,
      resubscribe: true,
      queueQoSzero: true   // ответ режиссёру не потеряется даже в окне обрыва
    });
  }
  return klMqttMini(url, opts);
}

/* Минимальный MQTT 3.1.1 поверх WebSocket (QoS 0) — запасной вариант.
   API совместим с используемым здесь подмножеством mqtt.js: connect/on/subscribe/publishAsync/connected. */
function klMqttMini(url, opts) {
  const enc = new TextEncoder(), dec = new TextDecoder();
  const handlers = {};
  let ws = null, connected = false, killed = false, pktId = 1, pingT = null, connT = null, recT = null;
  const api = {
    get connected() { return connected; },
    on(ev, fn) { (handlers[ev] = handlers[ev] || []).push(fn); return api; },
    subscribe(topic) {
      const t = enc.encode(topic);
      const body = new Uint8Array(2 + 2 + t.length + 1);
      body[0] = pktId >> 8; body[1] = pktId++ & 255;
      body[2] = t.length >> 8; body[3] = t.length & 255; body.set(t, 4); body[4 + t.length] = 0;
      send(packet(0x82, body));
    },
    async publishAsync(topic, payload) {
      const t = enc.encode(topic), d = enc.encode(String(payload));
      const body = new Uint8Array(2 + t.length + d.length);
      body[0] = t.length >> 8; body[1] = t.length & 255; body.set(t, 2); body.set(d, 2 + t.length);
      send(packet(0x30, body));
    },
    end() { killed = true; clearTimeout(recT); try { ws && ws.close(); } catch (_) {} }
  };
  function emit(ev, a, b) { (handlers[ev] || []).forEach((f) => { try { f(a, b); } catch (_) {} }); }
  function packet(typeFlags, body) {
    let n = body.length, len = [];
    do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 0x80; len.push(d); } while (n > 0);
    const out = new Uint8Array(1 + len.length + body.length);
    out[0] = typeFlags; out.set(len, 1); out.set(body, 1 + len.length);
    return out;
  }
  function send(u8) { if (ws && ws.readyState === 1) ws.send(u8); }
  function parseVarLen(d, p) { let m = 1, v = 0, b; do { b = d[p++]; v += (b & 127) * m; m *= 128; } while (b & 128); return [v, p]; }
  function onPacket(d) {
    const type = d[0] >> 4;
    const [rl, p0] = parseVarLen(d, 1);
    const p = p0;
    if (type === 2) { // CONNACK
      if (d[p + 1] === 0) {
        connected = true;
        clearTimeout(connT);
        pingT = setInterval(() => send(new Uint8Array([0xC0, 0x00])), 30000);
        emit('connect');
      } else {
        emit('error', new Error('MQTT refused, code ' + d[p + 1]));
      }
    } else if (type === 3) { // PUBLISH (QoS 0)
      const tl = (d[p] << 8) | d[p + 1];
      const topic = dec.decode(d.subarray(p + 2, p + 2 + tl));
      emit('message', topic, dec.decode(d.subarray(p + 2 + tl)));
    } else if (type === 13) { /* PINGRESP */ }
  }
  function open() {
    ws = new WebSocket(url, 'mqtt');
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      const c = enc.encode(opts.clientId || 'kl_' + Math.random().toString(36).slice(2, 10));
      const vh = enc.encode('MQTT');
      const body = new Uint8Array(2 + vh.length + 4 + 2 + c.length);
      let o = 0;
      body[o++] = vh.length >> 8; body[o++] = vh.length & 255; body.set(vh, o); o += vh.length;
      body[o++] = 4;        // уровень протокола MQTT 3.1.1
      body[o++] = 2;        // clean session
      body[o++] = 0; body[o++] = 60; // keepalive
      body[o++] = c.length >> 8; body[o++] = c.length & 255; body.set(c, o);
      send(packet(0x10, body));
      clearTimeout(connT);
      connT = setTimeout(() => { try { ws.close(); } catch (_) {} }, opts.connectTimeout || 9000);
    };
    ws.onmessage = (ev) => onPacket(new Uint8Array(ev.data));
    ws.onerror = () => emit('error', new Error('ошибка WebSocket'));
    ws.onclose = () => {
      clearInterval(pingT); clearTimeout(connT);
      if (connected) { connected = false; emit('offline'); }
      if (!killed) recT = setTimeout(open, opts.reconnectPeriod || 4000);
    };
  }
  open();
  return api;
}

const b64d = (s) => new TextDecoder().decode(Uint8Array.from(
  atob(s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - s.length % 4) % 4)),
  (c) => c.charCodeAt(0)));
const b64e = (s) => btoa(String.fromCharCode(...new TextEncoder().encode(s)))
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

const hash = new URLSearchParams(location.hash.slice(1));
const code = (hash.get('c') || '').replace(/[^A-Za-z0-9_-]/g, '');
const manualOffer = hash.get('o');

let pc = null;
let inboundStream = null;
let mc = null;
let live = null;
let busy = false;
let gotStream = false;
let curOfferVer = null;
let discoTimer = null;
let pendingAnswer = null;   // ответ, ожидающий доставки отправителю
let ansTimer = null;

function publishAnswer() {
  if (!pendingAnswer || !mc) return;
  // retain=true: отправитель подхватит ответ при ЛЮБОМ переподключении —
  // живые публикации на ненадёжном канале до него часто не доходят.
  mc.publishAsync(`kl/${code}/a`, pendingAnswer, { retain: true }).catch(() => {});
}
function startAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = setInterval(publishAnswer, 8000);
}
function stopAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = null;
  pendingAnswer = null;
}
// Разрыв эфира НЕ должен отменять доставку ответа: отправитель может
// ожить через минуту и подхватить retained-ответ — поэтому здесь только
// гасим таймер периодики, сам ответ и цикл публикации сохраняются.
function pauseAnswerRetransmit() {
  clearInterval(ansTimer);
  ansTimer = null;
}

function teardownToWaiting() {
  gotStream = false;
  stopViewerStats();
  pauseAnswerRetransmit();
  // ретрансляцию продолжаем: publishAnswer вызывается и из цикла waitForOffer
  startAnswerRetransmit();
  try { if (pc) pc.close(); } catch (_) {}
  pc = null;
  $('player').style.display = 'none';
  $('topbar').style.display = 'none';
  $('stage').style.display = 'flex';
  $('playBtn').style.display = 'none';
  if (code) {
    big('Связь прервана.', 'Не закрывайте страницу — подключимся снова, как только эфир возобновится.');
    waitForOffer();
  } else {
    big('Связь прервана.', 'Обновите страницу или попросите новую ссылку.');
  }
}

/* Показать, каким путём пошёл трафик (direct/relay) — для диагностики */
function showConnPath() {
  setTimeout(async () => {
    try {
      const st = await pc.getStats();
      st.forEach((r) => {
        if (r.type === 'candidate-pair' && r.nominated && r.state === 'succeeded') {
          const l = st.get(r.localCandidateId), rm = st.get(r.remoteCandidateId);
          const how = (l ? l.candidateType : '?') + ' → ' + (rm ? rm.candidateType : '?');
          window.__vdiag.path = how;
        }
      });
    } catch (_) {}
  }, 2500);
}

/* Живой индикатор в панели: сколько видео/звука реально доходит.
   Если цифры стоят на нуле — проблема в сети, а не в телефоне. */
let vStatsTimer = null, vLastVideo = 0, vLastAudio = 0, vZeroSince = 0;
function stopViewerStats() { clearInterval(vStatsTimer); vStatsTimer = null; }
function startViewerStats() {
  stopViewerStats();
  vLastVideo = 0; vLastAudio = 0; vZeroSince = Date.now();
  const tag = $('netTag');
  vStatsTimer = setInterval(async () => {
    if (!pc) { stopViewerStats(); return; }
    try {
      const st = await pc.getStats();
      let vb = 0, ab = 0;
      st.forEach((r) => {
        if (r.type === 'inbound-rtp' && r.kind === 'video') vb += r.bytesReceived || 0;
        if (r.type === 'inbound-rtp' && r.kind === 'audio') ab += r.bytesReceived || 0;
      });
      const vkbps = Math.round((vb - vLastVideo) * 8 / 1000 / 2);
      const akbps = Math.round((ab - vLastAudio) * 8 / 1000 / 2);
      vLastVideo = vb; vLastAudio = ab;
      const path = window.__vdiag.path || '';
      if (vb + ab > 0) vZeroSince = 0;
      if (tag) {
        if (vb + ab === 0 && vZeroSince && Date.now() - vZeroSince > 6000) {
          tag.textContent = '⚠ видео не доходит (' + (path || 'канал неизвестен') + ')';
          tag.style.color = '#ff8484';
        } else {
          tag.textContent = '▸ ' + vkbps + ' кбит/с видео · ' + akbps + ' кбит/с звук' + (path ? ' · ' + path : '');
          tag.style.color = '';
        }
      }
    } catch (_) {}
  }, 2000);
}

function big(msg, sub) {
  $('bigmsg').textContent = msg;
  $('submsg').textContent = sub || '';
}
function showPlayer() {
  $('stage').style.display = 'none';
  $('player').style.display = 'block';
  $('topbar').style.display = 'flex';
  $('playBtn').style.display = 'none';
}
function gatherComplete(p, ms = 9000) {
  if (p.iceGatheringState === 'complete') return Promise.resolve();
  return new Promise((res) => {
    const to = setTimeout(res, ms);
    p.addEventListener('icegatheringstatechange', () => {
      if (p.iceGatheringState === 'complete') { clearTimeout(to); res(); }
    });
  });
}

async function connectToOffer(offerB64) {
  if (busy) return;
  busy = true;
  stopAnswerRetransmit(); // старый ответ неактуален — строим новый
  try {
    if (pc) { try { pc.close(); } catch (_) {} pc = null; }
    $('spinner').style.display = 'block';
    big('Подключаемся к эфиру…', 'Обычно занимает 2–5 секунд.');
    window.__vdiag.stage = 'got offer, connecting pc';
    const offer = JSON.parse(b64d(offerB64));
    pc = new RTCPeerConnection({ iceServers: STUN_LIST });
    // Собираем входящий поток вручную: sendonly-трансиверы без msid могут
    // давать пустой ev.streams — опираемся на сами треки.
    inboundStream = new MediaStream();
    pc.ontrack = (ev) => {
      inboundStream.addTrack(ev.track);
      gotStream = true;
      $('player').srcObject = inboundStream;
      $('spinner').style.display = 'none';
      $('playBtn').style.display = 'inline-block';
      big('Эфир готов!', '');
    };
    pc.onconnectionstatechange = () => {
      const st = pc.connectionState;
      if (st === 'connected') {
        clearTimeout(discoTimer);
        stopAnswerRetransmit();
        big('Эфир готов!', '');
        showConnPath();
        startViewerStats();
      } else if (st === 'disconnected') {
        // короткие обрывы на мобильных сетях/VPN часто восстанавливаются сами
        big('Связь прерывается…', 'Восстанавливаем автоматически, ничего нажимать не нужно.');
        clearTimeout(discoTimer);
        discoTimer = setTimeout(() => teardownToWaiting(), 15000);
      } else if (st === 'failed' || st === 'closed') {
        clearTimeout(discoTimer);
        teardownToWaiting();
      }
    };
    await pc.setRemoteDescription(offer);
    window.__vdiag.stage = 'offer applied';
    await pc.setLocalDescription(await pc.createAnswer());
    window.__vdiag.stage = 'answer created';
    window.__answerB64 = b64e(JSON.stringify(pc.localDescription));
    await gatherComplete(pc);
    if (code && mc) {
      // Публикуем в любом состоянии: полный клиент поставит в очередь
      // (queueQoSzero), мини-клиент проигнорирует — но startAnswerRetransmit
      // и publishAnswer при reconnect всё равно довезут.
      pendingAnswer = b64e(JSON.stringify({ v: curOfferVer, s: JSON.stringify(pc.localDescription) }));
      // Не ждём: на полузамёрзшем сокете publishAsync может висеть вечно,
      // а busy держит весь цикл. Ретранслятор (каждые 8 с) довезёт сам.
      try { mc.publishAsync(`kl/${code}/a`, pendingAnswer, { retain: true }).catch(() => {}); } catch (_) {}
      window.__vdiag.stage = 'answer queued, connected=' + mc.connected;
      startAnswerRetransmit(); // пока не подключимся — периодически дублируем ответ
      big('Ответ отправлен.', 'Устанавливаем соединение…');
    } else {
      const ans = 'KLANS1.' + b64e(JSON.stringify(pc.localDescription));
      $('answerOut').value = ans;
      $('answerOut').style.display = 'block';
      $('copyAnswer').style.display = 'inline-block';
      $('playBtn').style.display = 'none';
      big('Эфир подготовлен.', 'Пришлите код ниже тому, кто дал вам ссылку — например, в Telegram или WhatsApp.');
      try { await navigator.clipboard.writeText(ans); $('copyAnswer').textContent = '✔ Скопировано'; } catch (_) {}
    }
  } catch (e) {
    window.__vdiag.err.push('connectToOffer: ' + String(e && e.message || e));
    big('Не удалось подключиться.', String(e.message || e));
    if (code) setTimeout(waitForOffer, 4000);
  } finally {
    busy = false;
  }
}

/* Ожидание объявления эфира и предложения (автоматический режим) */
let waitingTimer = null;
let lastMqttActivity = 0;   // сторож: соединение может быть "полузамёрзшим"
function mqttTouch() { lastMqttActivity = Date.now(); }
function waitForOffer() {
  if (!code || busy) return;
  clearTimeout(waitingTimer);
  window.__vdiag.stage = 'waitForOffer';
  // Сторож: connected=true ещё не значит живое соединение (WebSocket может
  // умереть молча). Если 45 с никакого трафика — пересоздаём клиента.
  if (mc && mc.connected && lastMqttActivity && Date.now() - lastMqttActivity > 45000) {
    try { mc.end(true); } catch (_) {}
    mc = null;
    window.__vdiag.stage = 'mqtt zombie-killed';
  }
  if (!mc || !mc.connected) {
    big('Соединяемся…', '');
    window.__vdiag.stage = 'mqtt connecting, klMqttConnect=' + typeof klMqttConnect;
    mc = klMqttConnect(MQTT_URL, {
      clientId: 'klview_' + Math.random().toString(36).slice(2, 12),
      connectTimeout: 9000,
      reconnectPeriod: 2000
    });
    mqttTouch();
    mc.on('connect', () => {
      mqttTouch();
      try { mc.options.reconnectPeriod = 2000; } catch (_) {}
      window.__vdiag.stage = 'mqtt connected';
      mc.subscribe(`kl/${code}/live`);
      mc.subscribe(`kl/${code}/o`);
      publishAnswer(); // если ответ ещё не дошёл — отправим снова
    });
    mc.on('message', (topic, payload) => {
      mqttTouch();
      const data = payload.toString();
      if (topic === `kl/${code}/live`) {
        live = data === '1';
        if (live) {
          big('Эфир начинается…', 'Подключаемся.');
        } else {
          $('player').style.display = 'none';
          $('topbar').style.display = 'none';
          $('stage').style.display = 'flex';
          $('playBtn').style.display = 'none';
          $('spinner').style.display = 'block';
          big('Эфир сейчас не идёт.', 'Оставьте страницу открытой — как только начнётся показ, видео появится само.');
        }
      }
      if (topic === `kl/${code}/o` && data.length > 40) {
        // Формат: KL1.<версия>.<b64 SDP>. Повторный оффер той же версии при
        // живом соединении игнорируем — не рвём рабочий эфир из-за реконнекта брокера.
        let ver = 'legacy', payload = data;
        if (data.startsWith('KL1.')) {
          const parts = data.split('.');
          ver = parts[1] || 'x';
          payload = parts.slice(2).join('.');
        }
        const healthy = pc && pc.connectionState === 'connected';
        if (ver === curOfferVer && healthy) return;
        curOfferVer = ver;
        connectToOffer(payload);
      }
    });
    mc.on('error', (e) => {
      window.__vdiag.err.push('mqtt error: ' + String(e && e.message || e));
      big('Проблема с соединением.', 'Проверьте интернет и обновите страницу.');
    });
    mc.on('offline', () => {
      window.__vdiag.err.push('mqtt offline');
      // Бэкофф переподключений: не держать себя в бане у публичного брокера
      try { mc.options.reconnectPeriod = Math.min(30000, (mc.options.reconnectPeriod || 2000) * 2); } catch (_) {}
    });
    mc.on('message', (topic) => { window.__vdiag.lastTopic = topic; });
  }
  // Пока ждём и не подключены — периодически переподписываемся:
  // брокер заново присылает retained-оффер, так мы подхватим свежую версию
  // после автопересоздания эфира на стороне отправителя.
  if (mc && mc.connected && !(pc && pc.connectionState === 'connected')) {
    mc.subscribe(`kl/${code}/o`);
    mc.subscribe(`kl/${code}/live`);
  }
  waitingTimer = setTimeout(waitForOffer, 15000);
}

/* ---------- Старт ---------- */
if (manualOffer && manualOffer.length > 40) {
  $('codeBox').innerHTML = '';
  connectToOffer(manualOffer);
} else if (code) {
  $('codeBox').innerHTML = 'Код эфира: <b>' + code + '</b>';
  if (typeof window.KL_TEST_NO_MQTT === 'undefined' || !window.KL_TEST_NO_MQTT) {
    waitForOffer();
  } else {
    big('Тестовый режим — ждём предложения…', '');
  }
} else {
  $('spinner').style.display = 'none';
  big('Ссылка неполная.', 'Попросите ссылку у отправителя ещё раз.');
}

/* ---------- Управление воспроизведением ---------- */
async function tryPlay() {
  const v = $('player');
  try {
    await v.play(); // со звуком
    return true;
  } catch (e1) {
    try {
      v.muted = true;               // мобильные часто требуют жест для звука —
      await v.play();               // запускаем беззвучно, потом возвращаем звук
      try { v.muted = false; } catch (_) {}
      return true;
    } catch (e2) {
      return false;
    }
  }
}
$('playBtn').onclick = async () => {
  if (!(await tryPlay())) {
    big('Браузер блокирует воспроизведение.', 'Нажмите на чёрный экран ещё раз.');
    return;
  }
  showPlayer();
};
$('player').onclick = async () => {
  if ($('player').paused) { try { await $('player').play(); } catch (_) {} }
};
$('copyAnswer').onclick = async () => {
  try {
    await navigator.clipboard.writeText($('answerOut').value);
    $('copyAnswer').textContent = '✔ Скопировано';
  } catch (_) {
    $('answerOut').select();
    document.execCommand('copy');
  }
};
$('fsBtn').onclick = () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else document.documentElement.requestFullscreen().catch(() => {});
};
/* KinoLink — страница зрителя (режиссёр). Один файл, без установки. */
'use strict';

window.__vdiag = { stage: 'boot', err: [] };
window.onerror = (m, s, l, c) => { window.__vdiag.err.push(String(m) + ' @' + l + ':' + c); };

const $ = (id) => document.getElementById(id);
const MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
const STUN_LIST = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun.yandex.net:3478' },
  { urls: 'stun:stun.3cx.com:3478' },
  { urls: 'stun:stun.voiparound.com:3478' },
  { urls: ['turn:openrelay.metered.ca:443?transport=tcp',
           'turn:openrelay.metered.ca:80?transport=tcp',
           'turn:openrelay.metered.ca:443',
           'turn:openrelay.metered.ca:80'],
    username: 'openrelayproject', credential: 'openrelayproject' },
  { urls: ['turn:freestun.net:3478?transport=tcp', 'turn:freestun.net:3478'],
    username: 'free', credential: 'free' }
];

/* Минимальный MQTT 3.1.1 поверх WebSocket (QoS 0) — вместо тяжёлой библиотеки.
   API совместим с используемым здесь подмножеством mqtt.js: connect/on/subscribe/publishAsync/connected. */
function klMqttConnect(url, opts) {
  const enc = new TextEncoder(), dec = new TextDecoder();
  const handlers = {};
  let ws = null, connected = false, killed = false, pktId = 1, pingT = null, connT = null, recT = null;
  const api = {
    get connected() { return connected; },
    on(ev, fn) { (handlers[ev] = handlers[ev] || []).push(fn); return api; },
    subscribe(topic) {
      const t = enc.encode(topic);
      const body = new Uint8Array(2 + 2 + t.length + 1);
      body[0] = pktId >> 8; body[1] = pktId++ & 255;
      body[2] = t.length >> 8; body[3] = t.length & 255; body.set(t, 4); body[4 + t.length] = 0;
      send(packet(0x82, body));
    },
    async publishAsync(topic, payload) {
      const t = enc.encode(topic), d = enc.encode(String(payload));
      const body = new Uint8Array(2 + t.length + d.length);
      body[0] = t.length >> 8; body[1] = t.length & 255; body.set(t, 2); body.set(d, 2 + t.length);
      send(packet(0x30, body));
    },
    end() { killed = true; clearTimeout(recT); try { ws && ws.close(); } catch (_) {} }
  };
  function emit(ev, a, b) { (handlers[ev] || []).forEach((f) => { try { f(a, b); } catch (_) {} }); }
  function packet(typeFlags, body) {
    let n = body.length, len = [];
    do { let d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 0x80; len.push(d); } while (n > 0);
    const out = new Uint8Array(1 + len.length + body.length);
    out[0] = typeFlags; out.set(len, 1); out.set(body, 1 + len.length);
    return out;
  }
  function send(u8) { if (ws && ws.readyState === 1) ws.send(u8); }
  function parseVarLen(d, p) { let m = 1, v = 0, b; do { b = d[p++]; v += (b & 127) * m; m *= 128; } while (b & 128); return [v, p]; }
  function onPacket(d) {
    const type = d[0] >> 4;
    const [rl, p0] = parseVarLen(d, 1);
    const p = p0;
    if (type === 2) { // CONNACK
      if (d[p + 1] === 0) {
        connected = true;
        clearTimeout(connT);
        pingT = setInterval(() => send(new Uint8Array([0xC0, 0x00])), 30000);
        emit('connect');
      } else {
        emit('error', new Error('MQTT refused, code ' + d[p + 1]));
      }
    } else if (type === 3) { // PUBLISH (QoS 0)
      const tl = (d[p] << 8) | d[p + 1];
      const topic = dec.decode(d.subarray(p + 2, p + 2 + tl));
      emit('message', topic, dec.decode(d.subarray(p + 2 + tl)));
    } else if (type === 13) { /* PINGRESP */ }
  }
  function open() {
    ws = new WebSocket(url, 'mqtt');
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      const c = enc.encode(opts.clientId || 'kl_' + Math.random().toString(36).slice(2, 10));
      const vh = enc.encode('MQTT');
      const body = new Uint8Array(2 + vh.length + 4 + 2 + c.length);
      let o = 0;
      body[o++] = vh.length >> 8; body[o++] = vh.length & 255; body.set(vh, o); o += vh.length;
      body[o++] = 4;        // уровень протокола MQTT 3.1.1
      body[o++] = 2;        // clean session
      body[o++] = 0; body[o++] = 60; // keepalive
      body[o++] = c.length >> 8; body[o++] = c.length & 255; body.set(c, o);
      send(packet(0x10, body));
      clearTimeout(connT);
      connT = setTimeout(() => { try { ws.close(); } catch (_) {} }, opts.connectTimeout || 9000);
    };
    ws.onmessage = (ev) => onPacket(new Uint8Array(ev.data));
    ws.onerror = () => emit('error', new Error('ошибка WebSocket'));
    ws.onclose = () => {
      clearInterval(pingT); clearTimeout(connT);
      if (connected) { connected = false; emit('offline'); }
      if (!killed) recT = setTimeout(open, opts.reconnectPeriod || 4000);
    };
  }
  open();
  return api;
}

const b64d = (s) => new TextDecoder().decode(Uint8Array.from(
  atob(s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - s.length % 4) % 4)),
  (c) => c.charCodeAt(0)));
const b64e = (s) => btoa(String.fromCharCode(...new TextEncoder().encode(s)))
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

const hash = new URLSearchParams(location.hash.slice(1));
const code = (hash.get('c') || '').replace(/[^A-Za-z0-9_-]/g, '');
const manualOffer = hash.get('o');

let pc = null;
let inboundStream = null;
let mc = null;
let live = null;
let busy = false;
let gotStream = false;
let curOfferVer = null;
let discoTimer = null;

function teardownToWaiting() {
  gotStream = false;
  stopViewerStats();
  try { if (pc) pc.close(); } catch (_) {}
  pc = null;
  $('player').style.display = 'none';
  $('topbar').style.display = 'none';
  $('stage').style.display = 'flex';
  $('playBtn').style.display = 'none';
  if (code) {
    big('Связь прервана.', 'Не закрывайте страницу — подключимся снова, как только эфир возобновится.');
    waitForOffer();
  } else {
    big('Связь прервана.', 'Обновите страницу или попросите новую ссылку.');
  }
}

/* Показать, каким путём пошёл трафик (direct/relay) — для диагностики */
function showConnPath() {
  setTimeout(async () => {
    try {
      const st = await pc.getStats();
      st.forEach((r) => {
        if (r.type === 'candidate-pair' && r.nominated && r.state === 'succeeded') {
          const l = st.get(r.localCandidateId), rm = st.get(r.remoteCandidateId);
          const how = (l ? l.candidateType : '?') + ' → ' + (rm ? rm.candidateType : '?');
          window.__vdiag.path = how;
        }
      });
    } catch (_) {}
  }, 2500);
}

/* Живой индикатор в панели: сколько видео/звука реально доходит.
   Если цифры стоят на нуле — проблема в сети, а не в телефоне. */
let vStatsTimer = null, vLastVideo = 0, vLastAudio = 0, vZeroSince = 0;
function stopViewerStats() { clearInterval(vStatsTimer); vStatsTimer = null; }
function startViewerStats() {
  stopViewerStats();
  vLastVideo = 0; vLastAudio = 0; vZeroSince = Date.now();
  const tag = $('netTag');
  vStatsTimer = setInterval(async () => {
    if (!pc) { stopViewerStats(); return; }
    try {
      const st = await pc.getStats();
      let vb = 0, ab = 0;
      st.forEach((r) => {
        if (r.type === 'inbound-rtp' && r.kind === 'video') vb += r.bytesReceived || 0;
        if (r.type === 'inbound-rtp' && r.kind === 'audio') ab += r.bytesReceived || 0;
      });
      const vkbps = Math.round((vb - vLastVideo) * 8 / 1000 / 2);
      const akbps = Math.round((ab - vLastAudio) * 8 / 1000 / 2);
      vLastVideo = vb; vLastAudio = ab;
      const path = window.__vdiag.path || '';
      if (vb + ab > 0) vZeroSince = 0;
      if (tag) {
        if (vb + ab === 0 && vZeroSince && Date.now() - vZeroSince > 6000) {
          tag.textContent = '⚠ видео не доходит (' + (path || 'канал неизвестен') + ')';
          tag.style.color = '#ff8484';
        } else {
          tag.textContent = '▸ ' + vkbps + ' кбит/с видео · ' + akbps + ' кбит/с звук' + (path ? ' · ' + path : '');
          tag.style.color = '';
        }
      }
    } catch (_) {}
  }, 2000);
}

function big(msg, sub) {
  $('bigmsg').textContent = msg;
  $('submsg').textContent = sub || '';
}
function showPlayer() {
  $('stage').style.display = 'none';
  $('player').style.display = 'block';
  $('topbar').style.display = 'flex';
  $('playBtn').style.display = 'none';
}
function gatherComplete(p, ms = 9000) {
  if (p.iceGatheringState === 'complete') return Promise.resolve();
  return new Promise((res) => {
    const to = setTimeout(res, ms);
    p.addEventListener('icegatheringstatechange', () => {
      if (p.iceGatheringState === 'complete') { clearTimeout(to); res(); }
    });
  });
}

async function connectToOffer(offerB64) {
  if (busy) return;
  busy = true;
  try {
    if (pc) { try { pc.close(); } catch (_) {} pc = null; }
    $('spinner').style.display = 'block';
    big('Подключаемся к эфиру…', 'Обычно занимает 2–5 секунд.');
    window.__vdiag.stage = 'got offer, connecting pc';
    const offer = JSON.parse(b64d(offerB64));
    pc = new RTCPeerConnection({ iceServers: STUN_LIST });
    // Собираем входящий поток вручную: sendonly-трансиверы без msid могут
    // давать пустой ev.streams — опираемся на сами треки.
    inboundStream = new MediaStream();
    pc.ontrack = (ev) => {
      inboundStream.addTrack(ev.track);
      gotStream = true;
      $('player').srcObject = inboundStream;
      $('spinner').style.display = 'none';
      $('playBtn').style.display = 'inline-block';
      big('Эфир готов!', '');
    };
    pc.onconnectionstatechange = () => {
      const st = pc.connectionState;
      if (st === 'connected') {
        clearTimeout(discoTimer);
        big('Эфир готов!', '');
        showConnPath();
        startViewerStats();
      } else if (st === 'disconnected') {
        // короткие обрывы на мобильных сетях/VPN часто восстанавливаются сами
        big('Связь прерывается…', 'Восстанавливаем автоматически, ничего нажимать не нужно.');
        clearTimeout(discoTimer);
        discoTimer = setTimeout(() => teardownToWaiting(), 7000);
      } else if (st === 'failed' || st === 'closed') {
        clearTimeout(discoTimer);
        teardownToWaiting();
      }
    };
    await pc.setRemoteDescription(offer);
    await pc.setLocalDescription(await pc.createAnswer());
    window.__answerB64 = b64e(JSON.stringify(pc.localDescription));
    await gatherComplete(pc);
    if (code && mc && mc.connected) {
      await mc.publishAsync(`kl/${code}/a`, b64e(JSON.stringify(pc.localDescription)));
      big('Ответ отправлен.', 'Устанавливаем соединение…');
    } else {
      const ans = 'KLANS1.' + b64e(JSON.stringify(pc.localDescription));
      $('answerOut').value = ans;
      $('answerOut').style.display = 'block';
      $('copyAnswer').style.display = 'inline-block';
      $('playBtn').style.display = 'none';
      big('Эфир подготовлен.', 'Пришлите код ниже тому, кто дал вам ссылку — например, в Telegram или WhatsApp.');
      try { await navigator.clipboard.writeText(ans); $('copyAnswer').textContent = '✔ Скопировано'; } catch (_) {}
    }
  } catch (e) {
    big('Не удалось подключиться.', String(e.message || e));
    if (code) setTimeout(waitForOffer, 4000);
  } finally {
    busy = false;
  }
}

/* Ожидание объявления эфира и предложения (автоматический режим) */
let waitingTimer = null;
function waitForOffer() {
  if (!code || busy) return;
  clearTimeout(waitingTimer);
  window.__vdiag.stage = 'waitForOffer';
  if (!mc || !mc.connected) {
    big('Соединяемся…', '');
    window.__vdiag.stage = 'mqtt connecting, klMqttConnect=' + typeof klMqttConnect;
    mc = klMqttConnect(MQTT_URL, {
      clientId: 'klview_' + Math.random().toString(36).slice(2, 12),
      connectTimeout: 9000,
      reconnectPeriod: 4000
    });
    mc.on('connect', () => {
      window.__vdiag.stage = 'mqtt connected';
      mc.subscribe(`kl/${code}/live`);
      mc.subscribe(`kl/${code}/o`);
    });
    mc.on('message', (topic, payload) => {
      const data = payload.toString();
      if (topic === `kl/${code}/live`) {
        live = data === '1';
        if (live) {
          big('Эфир начинается…', 'Подключаемся.');
        } else {
          $('player').style.display = 'none';
          $('topbar').style.display = 'none';
          $('stage').style.display = 'flex';
          $('playBtn').style.display = 'none';
          $('spinner').style.display = 'block';
          big('Эфир сейчас не идёт.', 'Оставьте страницу открытой — как только начнётся показ, видео появится само.');
        }
      }
      if (topic === `kl/${code}/o` && data.length > 40) {
        // Формат: KL1.<версия>.<b64 SDP>. Повторный оффер той же версии при
        // живом соединении игнорируем — не рвём рабочий эфир из-за реконнекта брокера.
        let ver = 'legacy', payload = data;
        if (data.startsWith('KL1.')) {
          const parts = data.split('.');
          ver = parts[1] || 'x';
          payload = parts.slice(2).join('.');
        }
        const healthy = pc && pc.connectionState === 'connected';
        if (ver === curOfferVer && healthy) return;
        curOfferVer = ver;
        connectToOffer(payload);
      }
    });
    mc.on('error', (e) => {
      window.__vdiag.err.push('mqtt error: ' + String(e && e.message || e));
      big('Проблема с соединением.', 'Проверьте интернет и обновите страницу.');
    });
    mc.on('offline', () => { window.__vdiag.err.push('mqtt offline'); });
    mc.on('message', (topic) => { window.__vdiag.lastTopic = topic; });
  }
  // Пока ждём и не подключены — периодически переподписываемся:
  // брокер заново присылает retained-оффер, так мы подхватим свежую версию
  // после автопересоздания эфира на стороне отправителя.
  if (mc && mc.connected && !(pc && pc.connectionState === 'connected')) {
    mc.subscribe(`kl/${code}/o`);
    mc.subscribe(`kl/${code}/live`);
  }
  waitingTimer = setTimeout(waitForOffer, 15000);
}

/* ---------- Старт ---------- */
if (manualOffer && manualOffer.length > 40) {
  $('codeBox').innerHTML = '';
  connectToOffer(manualOffer);
} else if (code) {
  $('codeBox').innerHTML = 'Код эфира: <b>' + code + '</b>';
  if (typeof window.KL_TEST_NO_MQTT === 'undefined' || !window.KL_TEST_NO_MQTT) {
    waitForOffer();
  } else {
    big('Тестовый режим — ждём предложения…', '');
  }
} else {
  $('spinner').style.display = 'none';
  big('Ссылка неполная.', 'Попросите ссылку у отправителя ещё раз.');
}

/* ---------- Управление воспроизведением ---------- */
async function tryPlay() {
  const v = $('player');
  try {
    await v.play(); // со звуком
    return true;
  } catch (e1) {
    try {
      v.muted = true;               // мобильные часто требуют жест для звука —
      await v.play();               // запускаем беззвучно, потом возвращаем звук
      try { v.muted = false; } catch (_) {}
      return true;
    } catch (e2) {
      return false;
    }
  }
}
$('playBtn').onclick = async () => {
  if (!(await tryPlay())) {
    big('Браузер блокирует воспроизведение.', 'Нажмите на чёрный экран ещё раз.');
    return;
  }
  showPlayer();
};
$('player').onclick = async () => {
  if ($('player').paused) { try { await $('player').play(); } catch (_) {} }
};
$('copyAnswer').onclick = async () => {
  try {
    await navigator.clipboard.writeText($('answerOut').value);
    $('copyAnswer').textContent = '✔ Скопировано';
  } catch (_) {
    $('answerOut').select();
    document.execCommand('copy');
  }
};
$('fsBtn').onclick = () => {
  if (document.fullscreenElement) document.exitFullscreen();
  else document.documentElement.requestFullscreen().catch(() => {});
};
