const REMOTE_USERS_URL = process.env.REMOTE_USERS_URL || "https://raw.githubusercontent.com/xcvisimilarity-latest/xcvidatabase/refs/heads/main/xcvifree.json";
const REMOTE_CREATE_URL = process.env.REMOTE_CREATE_URL || "https://tesfreegen2.vercel.app/api/connect/create-account";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8207201116:AAHyth3gbJInooesGUp3QoGvSlVVXYOy8Bg";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "6716435472";

const MIN_EXPIRE_DAYS = 31;
const MAX_EXPIRE_DAYS = 90;
const COOLDOWN_MS = 5 * 60 * 1000;
const BAN_THRESHOLD = 30;
const BAN_DURATION_MS = 60 * 60 * 1000;
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX = 20;

// In-memory stores for Vercel serverless
let cooldowns = new Map();
let banList = new Map();
let recentReqs = new Map();
let failCounts = new Map();
let userStats = { totalUsers: 0, premiumUsers: 0 };

// ===== Simple in-memory  =====
let __usersCache = { ts: 0, ttl: 3000, data: null };

function getUsersCache() {
  if (__usersCache.data && (Date.now() - __usersCache.ts) < __usersCache.ttl) {
    return __usersCache.data;
  }
  return null;
}

function setUsersCache(data) {
  __usersCache = { ts: Date.now(), ttl: 3000, data };
}

// Telegram notification function
async function sendTelegramNotification(message) {
    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        console.log('Sending Telegram notification...');
        
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                chat_id: TELEGRAM_CHAT_ID,
                text: message,
                parse_mode: 'HTML'
            })
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('Telegram API error:', response.status, errorText);
            return false;
        }
        
        console.log('Telegram notification sent successfully');
        return true;
    } catch (error) {
        console.error('Telegram notification failed:', error.message);
        return false;
    }
}

function now() { return Date.now(); }

function cleanOld(reqs) {
    const cutoff = now() - RATE_LIMIT_WINDOW_MS;
    return reqs.filter(t => t > cutoff);
}

function ipFromReq(req) {
    try {
        const xForwardedFor = req.headers['x-forwarded-for'];
        const xRealIp = req.headers['x-real-ip'];
        const cfConnectingIp = req.headers['cf-connecting-ip'];
        
        let ip = 'unknown';
        
        if (cfConnectingIp) {
            ip = cfConnectingIp;
        } else if (xRealIp) {
            ip = xRealIp;
        } else if (xForwardedFor) {
            ip = xForwardedFor.split(',')[0].trim();
        } else if (req.socket?.remoteAddress) {
            ip = req.socket.remoteAddress;
        }
        
        // Handle IPv6 format
        if (ip === '::1') ip = '127.0.0.1';
        if (ip.includes('::ffff:')) ip = ip.replace('::ffff:', '');
        
        return ip;
    } catch (error) {
        console.error('Error getting IP:', error);
        return 'unknown';
    }
}

function randStr(len = 8) {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let s = '';
    for (let i = 0; i < len; i++) s += chars.charAt(Math.floor(Math.random() * chars.length));
    return s;
}

function genPassword() {
    const part = randStr(6);
    const insertAt = Math.floor(Math.random() * (part.length + 1));
    return part.slice(0, insertAt) + 'xcvi' + part.slice(insertAt);
}

function genExpiryMs() {
    const days = Math.floor(Math.random() * (MAX_EXPIRE_DAYS - MIN_EXPIRE_DAYS + 1)) + MIN_EXPIRE_DAYS;
    return Date.now() + days * 24 * 3600 * 1000;
}

// === REPLACE fetchJson with robust AbortController + redirect follow ===
async function fetchJson(url, opts = {}) {
  const timeoutMs = typeof opts.timeout === 'number' ? opts.timeout : 10000;
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);

  try {
    console.log(`[sistem] fetchJson -> ${url} (timeout ${timeoutMs}ms)`);
    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': 'XCVI-Database-System/3.0.0',
        'Accept': 'application/json, text/plain, */*'
      },
      redirect: 'follow',
      signal: ac.signal
    });

    clearTimeout(timer);

    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      throw new Error(`HTTP ${resp.status}: ${txt || resp.statusText}`);
    }

    const text = await resp.text().catch(() => '');
    console.log('[sistem] fetchJson raw (truncated):', text ? text.substring(0, 300) : '<empty>');
    const normalized = normalizePossiblyUnquotedJson(text || '');
    try {
      const data = JSON.parse(normalized);
      return data;
    } catch (parseErr) {
      throw new Error('Failed to parse JSON from remote: ' + (parseErr.message || parseErr));
    }
  } catch (err) {
    if (err.name === 'AbortError') {
      throw new Error('Request timed out');
    }
    throw err;
  } finally {
    try { clearTimeout(timer); } catch (_) {}
  }
}

// ========== REPLACE getTotalUsers() ==========
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function getTotalUsers(options = {}) {
  const retries = (options.retries != null) ? options.retries : 3;
  const retryDelayMs = (options.retryDelayMs != null) ? options.retryDelayMs : 700;

  try {
    console.log('Getting user stats from:', REMOTE_USERS_URL);

    // cache-buster only for raw.githubusercontent to avoid stale CDN cache
    let url = REMOTE_USERS_URL;
    if (typeof url === 'string' && url.includes('raw.githubusercontent.com')) {
      url = url + (url.includes('?') ? '&' : '?') + 't=' + Date.now();
      console.log('Using cache-busted URL for GitHub raw:', url);
    }

    let lastErr = null;
    for (let i = 0; i < retries; i++) {
      try {
        const data = await fetchRemoteUsers({ retries: 3, retryDelayMs: 700, timeoutMs: 10000 });

        if (Array.isArray(data)) {
          userStats.totalUsers = data.length;
          userStats.premiumUsers = data.filter(u => String(u.role || '').toLowerCase() === 'premium').length;
          return userStats;
        } else {
          throw new Error('Remote users JSON not array');
        }
      } catch (err) {
        lastErr = err;
        console.warn(`getTotalUsers attempt ${i+1} failed:`, err.message || err);
        if (i < retries - 1) await sleep(retryDelayMs);
      }
    }

    console.error('getTotalUsers: all retries failed:', lastErr && lastErr.message);
    return { totalUsers: userStats.totalUsers || 15842, premiumUsers: userStats.premiumUsers || 14258 };
  } catch (err) {
    console.error('Failed to fetch user stats unexpected:', err.message);
    return { totalUsers: userStats.totalUsers || 15842, premiumUsers: userStats.premiumUsers || 14258 };
  }
}


function normalizePossiblyUnquotedJson(text) {
  if (!text || !String(text).trim()) return '[]';
  try {
    // If already valid JSON, return as-is
    JSON.parse(text);
    return text;
  } catch (e) {
    try {
      // add quotes to unquoted keys: { key: -> { "key":
      let fixed = String(text).replace(/([{,]\s*)([-A-Za-z0-9_$]+)\s*:/g, '$1"$2":');
      // convert single quotes to double quotes
      fixed = fixed.replace(/'/g, '"');
      JSON.parse(fixed);
      return fixed;
    } catch (e2) {
      // try extract first JSON array or object substring
      const arrMatch = String(text).match(/\[.*\]/s);
      if (arrMatch) return arrMatch[0];
      const objMatch = String(text).match(/\{.*\}/s);
      if (objMatch) return objMatch[0];
      // else return original (will likely fail upstream but we did our best)
      return text;
    }
  }
}


// === fetchRemoteUsers: robust + cache + GitHub API fallback (no recursion) ===
async function fetchRemoteUsers(opts = {}) {
  const cached = getUsersCache ? getUsersCache() : null;
  if (cached) {
    console.log('[sistem] Returning cached users -', cached.length);
    return cached;
  }

  const retries = typeof opts.retries === 'number' ? opts.retries : 3;
  const retryDelayMs = typeof opts.retryDelayMs === 'number' ? opts.retryDelayMs : 600;
  const timeoutMs = typeof opts.timeoutMs === 'number' ? opts.timeoutMs : 10000;

  const baseUrl = REMOTE_USERS_URL;
  const cacheBustedUrl = baseUrl + (baseUrl.includes('?') ? '&' : '?') + 't=' + Date.now();

  let lastErr = null;

  for (let i = 0; i < retries; i++) {
    try {
      // Use fetchJson (safe fetch + timeout) if available; else fallback to fetch
      if (typeof fetchJson === 'function') {
        const data = await fetchJson(cacheBustedUrl, { timeout: timeoutMs });
        if (Array.isArray(data)) {
          console.log(`[sistem] fetchRemoteUsers success (attempt ${i+1}) - ${data.length} users`);
          if (typeof setUsersCache === 'function') setUsersCache(data);
          return data;
        }
        throw new Error('Remote payload is not an array');
      } else {
        const resp = await fetch(cacheBustedUrl, { redirect: 'follow' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const txt = await resp.text();
        const norm = normalizePossiblyUnquotedJson(txt || '');
        const parsed = JSON.parse(norm);
        if (Array.isArray(parsed)) {
          if (typeof setUsersCache === 'function') setUsersCache(parsed);
          return parsed;
        }
        throw new Error('Remote payload not array');
      }
    } catch (e) {
      lastErr = e;
      console.warn(`[sistem] fetchRemoteUsers attempt ${i+1} failed:`, String(e?.message || e));
      if (i < retries - 1) await new Promise(r => setTimeout(r, retryDelayMs));
    }
  }

  // Fallback: GitHub API /contents (authoritative)
  try {
    if (typeof REMOTE_USERS_URL === 'string' && REMOTE_USERS_URL.includes('raw.githubusercontent.com')) {
      const m = REMOTE_USERS_URL.match(/raw\.githubusercontent\.com\/([^/]+)\/([^/]+)\/(.+)/);
      if (m) {
        const owner = m[1], repo = m[2];
        let rest = m[3];
        if (rest.startsWith('refs/heads/')) rest = rest.slice('refs/heads/'.length);
        const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${rest}`;
        console.log('[sistem] Trying GitHub API fallback:', apiUrl);

        const token = process.env.GITHUB_TOKEN || global.GITHUB_TOKEN;
        const headers = { 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'XCVI-Database-System/3.0.0' };
        if (token) headers.Authorization = `token ${token}`;

        const ac = new AbortController();
        const timer = setTimeout(() => ac.abort(), timeoutMs);
        try {
          const resp = await fetch(apiUrl, { headers, redirect: 'follow', signal: ac.signal });
          clearTimeout(timer);
          if (!resp.ok) {
            const txt = await resp.text().catch(()=>'');
            console.warn('[sistem] GitHub API fallback non-OK:', resp.status, txt.substring(0,200));
          } else {
            const body = await resp.json().catch(()=>null);
            if (body && body.content) {
              const content = Buffer.from(body.content || '', 'base64').toString('utf8');
              const normalized = normalizePossiblyUnquotedJson(content);
              const parsed = JSON.parse(normalized);
              if (Array.isArray(parsed)) {
                console.log('[sistem] GitHub API fallback success - users:', parsed.length);
                if (typeof setUsersCache === 'function') setUsersCache(parsed);
                return parsed;
              }
            }
          }
        } catch (e) {
          if (e.name === 'AbortError') console.warn('[sistem] GitHub API fallback timed out');
          else console.warn('[sistem] GitHub API fallback error:', String(e?.message || e));
        } finally {
          try{ clearTimeout(timer); }catch(_){}
        }
      }
    }
  } catch (e) {
    console.warn('[sistem] GitHub API fallback outer error:', String(e?.message || e));
  }

  throw lastErr || new Error('Failed to fetch remote users after retries');
}
// ==============================================

module.exports = async (req, res) => {
    console.log('=== XCVI API Request ===');
    console.log('Method:', req.method);
    console.log('URL:', req.url);
    console.log('Headers:', req.headers);
    
    try {
        // Set CORS headers
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
        res.setHeader('Access-Control-Max-Age', '86400');

        if (req.method === 'OPTIONS') {
            console.log('Handling OPTIONS request');
            return res.status(200).end();
        }

        if (req.method === 'GET') {
            console.log('Handling GET request');
            const stats = await getTotalUsers();
            return res.status(200).json({
                ok: true,
                message: 'XCVI Database System API',
                version: '3.0.0',
                stats: stats,
                timestamp: now(),
                endpoints: {
                    'POST /': 'Create new account',
                    'GET /': 'Get system stats'
                }
            });
        }

        if (req.method !== 'POST') {
            console.log('Method not allowed:', req.method);
            return res.status(405).json({ 
                ok: false, 
                error: 'Method not allowed', 
                hint: 'Use POST for creating accounts', 
                allowed: ['POST', 'GET'] 
            });
        }

        const ip = ipFromReq(req);
        const nowTime = now();
        
        console.log('Client IP:', ip);
        console.log('User Agent:', req.headers['user-agent']);

        // Send activity notification to Telegram
        await sendTelegramNotification(
            `🔔 <b>New Activity Detected</b>\n` +
            `📱 <b>IP:</b> <code>${ip}</code>\n` +
            `⏰ <b>Time:</b> ${new Date(nowTime).toLocaleString('id-ID')}\n` +
            `🌐 <b>Path:</b> ${req.url}`
        );

        // Ban check
        const banUntil = banList.get(ip) || 0;
        if (banUntil && nowTime < banUntil) {
            console.log('IP banned until:', new Date(banUntil).toLocaleString());
            await sendTelegramNotification(
                `🚫 <b>Blocked Banned IP</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `⏰ <b>Ban Until:</b> ${new Date(banUntil).toLocaleString('id-ID')}`
            );
            return res.status(403).json({ 
                ok: false, 
                error: 'Your IP is temporarily banned', 
                banUntil 
            });
        }

        // Rate limit sliding window
        let arr = recentReqs.get(ip) || [];
        arr = cleanOld(arr);
        arr.push(nowTime);
        recentReqs.set(ip, arr);
        
        if (arr.length > RATE_LIMIT_MAX) {
            const until = nowTime + BAN_DURATION_MS;
            banList.set(ip, until);
            console.log(`Rate limit exceeded for IP ${ip}, banning until ${new Date(until)}`);
            
            await sendTelegramNotification(
                `⚠️ <b>Auto-Ban Triggered</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `📊 <b>Requests:</b> ${arr.length}/${RATE_LIMIT_MAX}\n` +
                `⏰ <b>Ban Until:</b> ${new Date(until).toLocaleString('id-ID')}`
            );
            
            return res.status(429).json({ 
                ok: false, 
                error: 'Too many requests - temporarily banned', 
                banUntil: until 
            });
        }

        // Parse body
        let body;
        try {
            if (typeof req.body === 'string') {
                body = JSON.parse(req.body);
            } else if (Buffer.isBuffer(req.body)) {
                body = JSON.parse(req.body.toString());
            } else {
                body = req.body || {};
            }
            console.log('Parsed body:', body);
        } catch (e) {
            console.error('Body parse error:', e.message);
            body = {};
        }
        
        const usernameRaw = String(body.username || '').trim();
        console.log('Username raw:', usernameRaw);
        
        if (!usernameRaw) {
            const failCount = (failCounts.get(ip) || 0) + 1;
            failCounts.set(ip, failCount);
            
            console.log(`Empty username attempt from IP ${ip}, fail count: ${failCount}`);
            
            if (failCount >= BAN_THRESHOLD) {
                const banTime = nowTime + BAN_DURATION_MS;
                banList.set(ip, banTime);
                
                await sendTelegramNotification(
                    `🔨 <b>Permanent Ban Applied</b>\n` +
                    `📱 <b>IP:</b> <code>${ip}</code>\n` +
                    `❌ <b>Fail Count:</b> ${failCount}\n` +
                    `⏰ <b>Ban Duration:</b> 1 hour`
                );
                
                return res.status(403).json({ 
                    ok: false, 
                    error: 'Too many invalid requests - banned' 
                });
            }
            
            await sendTelegramNotification(
                `❌ <b>Invalid Request</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `🚫 <b>Error:</b> Empty username\n` +
                `📊 <b>Fail Count:</b> ${failCount}`
            );
            
            return res.status(400).json({ 
                ok: false, 
                error: 'Username wajib diisi' 
            });
        }

        const username = usernameRaw.replace(/\s+/g, '');
        console.log('Username cleaned:', username);
        
        // Cooldown check per IP
        const cd = cooldowns.get(ip) || 0;
        if (cd && nowTime < cd) {
            const remaining = Math.ceil((cd - nowTime) / 1000 / 60);
            console.log(`Cooldown active for IP ${ip}, ${remaining} minutes remaining`);
            
            return res.status(429).json({ 
                ok: false, 
                error: `Cooldown aktif. Tunggu ${remaining} menit lagi.`, 
                cooldownUntil: cd 
            });
        }


let remoteList = [];
try {
    console.log('Checking duplicate username (robust fetch)...');
    remoteList = await fetchRemoteUsers({ retries: 3, retryDelayMs: 600 });
    console.log(`Fetched ${remoteList.length} users from remote (robust)`);
} catch (e) {
    console.error('Remote fetch error (duplicate-check):', String(e?.message || e));
    await sendTelegramNotification(
        `🔴 <b>Database Connection Failed</b>\n` +
        `📱 <b>IP:</b> <code>${ip}</code>\n` +
        `👤 <b>Username:</b> ${username}\n` +
        `❌ <b>Error:</b> ${String(e?.message || e)}`
    );
    return res.status(502).json({
        ok: false,
        error: 'Gagal memeriksa database remote',
        details: String(e?.message || e)
    });
}


const exists = remoteList.some(u => {
  const existingUsername = String(u.username || '').toLowerCase();
  return existingUsername === username.toLowerCase();
});

if (exists) {

  try {
    console.log('[sistem] Duplicate detected locally — performing authoritative re-check');
    const fresh = await fetchRemoteUsers({ retries: 2, retryDelayMs: 300, timeoutMs: 8000 });
    const freshExists = Array.isArray(fresh) && fresh.some(u => String(u.username || '').toLowerCase() === username.toLowerCase());
    if (freshExists) {
      console.log('[sistem] Authoritative check: username exists -> returning 409');
      await sendTelegramNotification(`⚠️ Duplicate Confirmed: ${username} (IP ${ip})`);
      return res.status(409).json({ ok: false, error: 'Username sudah ada. Pilih username lain.' });
    } else {

      console.warn('[sistem] Transient: local cache said exists but authoritative source did not. Proceeding to attempt create.');
    }
  } catch (e) {
    console.warn('[sistem] Authoritative re-check failed:', String(e?.message || e));

    return res.status(409).json({ ok: false, error: 'Username sudah ada. Pilih username lain.' });
  }
}

        // Generate credentials
        const password = genPassword();
        const role = 'premium';
        const createdAt = Date.now();
        const expired = genExpiryMs();

        const account = { 
            username, 
            password, 
            role, 
            createdAt, 
            expired,
            createdBy: ip
        };

        console.log('Generated account:', { username, password, role, expired: new Date(expired) });

        // Forward to remote create-account endpoint
        let forwardResp;
       
        let forwardData = {};
        try {
            console.log('Forwarding to remote create endpoint:', REMOTE_CREATE_URL);

            const forwardBody = { username, password, role, expires: expired };
            const forwardPayload = JSON.stringify(forwardBody);
            console.log('Forward body (truncated):', JSON.stringify({ username, role, expires: new Date(expired).toISOString() }));

            // small retry attempts for transient network problems
            const FORWARD_RETRIES = 2;
            const FORWARD_TIMEOUT_MS = 10000;
            let forwardRespLocal = null;
            for (let fi = 0; fi < FORWARD_RETRIES; fi++) {
                const ac = new AbortController();
                const tid = setTimeout(() => ac.abort(), FORWARD_TIMEOUT_MS);
                try {
                    forwardRespLocal = await fetch(REMOTE_CREATE_URL, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'User-Agent': 'XCVI-Database-System/3.0.0'
                        },
                        body: forwardPayload,
                        signal: ac.signal,
                        redirect: 'follow'
                    });
                    clearTimeout(tid);
                    console.log(`Remote response status (attempt ${fi+1}):`, forwardRespLocal.status);
                    // break out on any response (we will handle non-ok below)
                    break;
                } catch (err) {
                    clearTimeout(tid);
                    console.warn(`Forward attempt ${fi+1} failed:`, String(err?.message || err));
                    if (fi === FORWARD_RETRIES - 1) throw err;
                    await new Promise(r => setTimeout(r, 500 + fi * 200));
                }
            }

            if (!forwardRespLocal) throw new Error('No response from remote create endpoint');

            // try parse JSON safely
            const contentType = (forwardRespLocal.headers && forwardRespLocal.headers.get && forwardRespLocal.headers.get('content-type')) || '';
            const forwardText = await forwardRespLocal.text().catch(() => '');
            console.log('Remote response text (truncated):', forwardText ? forwardText.substring(0,400) : '<empty>');

            try {
                if (contentType.includes('application/json') || forwardText.trim().startsWith('{') || forwardText.trim().startsWith('[')) {
                    forwardData = JSON.parse(forwardText);
                } else {
                    forwardData = { raw: forwardText };
                }
            } catch (parseErr) {
                console.warn('Could not parse remote response as JSON:', parseErr.message);
                forwardData = { raw: forwardText };
            }

            forwardResp = forwardRespLocal;

        } catch (e) {
            console.error('Remote create error:', String(e?.message || e));
            await sendTelegramNotification(
                `🔴 <b>Create Account Failed</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> ${username}\n` +
                `❌ <b>Error:</b> ${String(e?.message || e)}`
            );
            return res.status(502).json({
                ok: false,
                error: 'Gagal memanggil server create-account',
                details: String(e?.message || e)
            });
        }

        if (!forwardResp.ok) {
            const failCount = (failCounts.get(ip) || 0) + 1;
            failCounts.set(ip, failCount);
            
            console.log('Remote create failed:', forwardResp.status, forwardData);
            
            await sendTelegramNotification(
                `🔴 <b>Account Creation Failed</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> ${username}\n` +
                `❌ <b>Error:</b> ${forwardData.error || 'Remote create failed'}\n` +
                `📊 <b>Status:</b> ${forwardResp.status}`
            );
            
            return res.status(forwardResp.status).json({ 
                ok: false, 
                error: forwardData.error || 'Remote create failed', 
                details: forwardData 
            });
        }

        // Success: set cooldown for IP
        const until = nowTime + COOLDOWN_MS;
        cooldowns.set(ip, until);
        failCounts.set(ip, 0);

        // --- Optimistic update: segera reflect perubahan di memori supaya UI cepat responsif
        userStats.totalUsers = (userStats.totalUsers || 0) + 1;

        // --- Try to re-fetch fresh stats (with retries). If fetch fails, fall back to optimistic stats.
        let stats;
        try {
            // request a fresh fetch (getTotalUsers has built-in retries + cache-buster)
            stats = await getTotalUsers({ retries: 3, retryDelayMs: 700 });
        } catch (e) {
            console.warn('Warning: getTotalUsers failed after create, using in-memory optimistic stats:', e && e.message);
            stats = { totalUsers: userStats.totalUsers || 0, premiumUsers: userStats.premiumUsers || 0 };
        }

        // --- Send success notification to Telegram (will include stats)
        await sendTelegramNotification(
            `✅ <b>New Account Created Successfully</b>\n` +
            `📱 <b>IP:</b> <code>${ip}</code>\n` +
            `👤 <b>Username:</b> <code>${username}</code>\n` +
            `🔑 <b>Password:</b> <code>${password}</code>\n` +
            `👑 <b>Role:</b> ${role}\n` +
            `📅 <b>Expires:</b> ${new Date(expired).toLocaleString('id-ID')}\n` +
            `👥 <b>Total Users:</b> ${stats.totalUsers}\n` +
            `⏰ <b>Cooldown Until:</b> ${new Date(until).toLocaleString('id-ID')}`
        );

        console.log('Account created successfully for user:', username);

        
        return res.status(200).json({
            ok: true,
            message: 'Akun berhasil dibuat',
            data: account,
            stats: stats,
            cooldownUntil: until,
            remoteResponse: forwardData
        });

    } catch (err) {
        console.error('[api/sistem] Unhandled error:', err);
        console.error('Error stack:', err.stack);
        
        // Send error notification to Telegram
        await sendTelegramNotification(
            `🔴 <b>System Error</b>\n` +
            `❌ <b>Error:</b> ${err.message || 'Unknown error'}\n` +
            `⏰ <b>Time:</b> ${new Date().toLocaleString('id-ID')}\n` +
            `📁 <b>Stack:</b> ${err.stack ? err.stack.substring(0, 100) : 'No stack'}`
        );
        
        return res.status(500).json({ 
            ok: false, 
            error: 'Internal server error: ' + (err.message || 'Unknown error'),
            timestamp: now()
        });
    }
};
