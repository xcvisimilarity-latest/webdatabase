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

// In-memory stores
const cooldowns = global.__xcvi_cooldowns || new Map();
const banList = global.__xcvi_banlist || new Map();
const recentReqs = global.__xcvi_reqs || new Map();
const failCounts = global.__xcvi_fails || new Map();
const userStats = global.__xcvi_stats || { totalUsers: 0, premiumUsers: 0 };

global.__xcvi_cooldowns = cooldowns;
global.__xcvi_banlist = banList;
global.__xcvi_reqs = recentReqs;
global.__xcvi_fails = failCounts;
global.__xcvi_stats = userStats;

// Telegram notification function
async function sendTelegramNotification(message) {
    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
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
            console.error('Telegram API error:', await response.text());
        }
        return response.ok;
    } catch (error) {
        console.error('Telegram notification failed:', error);
        return false;
    }
}

function now() { return Date.now(); }

function cleanOld(reqs) {
    const cutoff = now() - RATE_LIMIT_WINDOW_MS;
    return reqs.filter(t => t > cutoff);
}

function ipFromReq(req) {
    return (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown').split(',')[0].trim();
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

async function fetchJson(url) {
    const resp = await fetch(url, { 
        method: 'GET', 
        headers: { 
            'User-Agent': 'xcvi-client/2.0',
            'Accept': 'application/json'
        } 
    });
    if (!resp.ok) throw new Error(`Fetch remote failed ${resp.status}`);
    const data = await resp.json();
    return data;
}

async function getTotalUsers() {
    try {
        const data = await fetchJson(REMOTE_USERS_URL);
        if (Array.isArray(data)) {
            userStats.totalUsers = data.length;
            userStats.premiumUsers = data.filter(u => u.role === 'premium').length;
        }
        return userStats;
    } catch (e) {
        console.error('Failed to fetch user stats:', e.message);
        return userStats;
    }
}

module.exports = async (req, res) => {
    try {
        // Set CORS headers
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

        if (req.method === 'OPTIONS') {
            return res.status(200).end();
        }

        if (req.method === 'GET') {
            const stats = await getTotalUsers();
            return res.status(200).json({
                ok: true,
                message: 'XCVI Database System API',
                version: '2.0.0',
                stats: stats,
                timestamp: now()
            });
        }

        if (req.method !== 'POST') {
            return res.status(405).json({ 
                ok: false, 
                error: 'Method not allowed', 
                hint: 'Use POST', 
                allowed: 'POST, GET' 
            });
        }

        const ip = ipFromReq(req);
        const nowTime = now();

        // Send activity notification to Telegram
        await sendTelegramNotification(
            `🔔 <b>New Activity Detected</b>\n` +
            `📱 <b>IP:</b> <code>${ip}</code>\n` +
            `⏰ <b>Time:</b> ${new Date(nowTime).toLocaleString('id-ID')}\n` +
            `🌐 <b>User Agent:</b> ${req.headers['user-agent'] || 'Unknown'}`
        );

        // Ban check
        const banUntil = banList.get(ip) || 0;
        if (banUntil && nowTime < banUntil) {
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
            body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
        } catch (e) {
            body = {};
        }
        
        const usernameRaw = String((body.username || '') || '').trim();
        if (!usernameRaw) {
            const failCount = (failCounts.get(ip) || 0) + 1;
            failCounts.set(ip, failCount);
            
            if (failCount > BAN_THRESHOLD) {
                banList.set(ip, nowTime + BAN_DURATION_MS);
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
        
        // Cooldown check per IP
        const cd = cooldowns.get(ip) || 0;
        if (cd && nowTime < cd) {
            return res.status(429).json({ 
                ok: false, 
                error: 'Cooldown aktif. Tunggu beberapa menit', 
                cooldownUntil: cd 
            });
        }

        // Check duplicate username by fetching remote JSON
        let remoteList = [];
        try {
            const data = await fetchJson(REMOTE_USERS_URL);
            if (Array.isArray(data)) remoteList = data;
        } catch (e) {
            await sendTelegramNotification(
                `🔴 <b>Database Connection Failed</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `❌ <b>Error:</b> ${e.message}\n` +
                `🌐 <b>URL:</b> ${REMOTE_USERS_URL}`
            );
            return res.status(502).json({ 
                ok: false, 
                error: 'Gagal memeriksa database remote', 
                details: e.message 
            });
        }

        const exists = remoteList.some(u => String(u.username || '').toLowerCase() === username.toLowerCase());
        if (exists) {
            failCounts.set(ip, (failCounts.get(ip) || 0) + 1);
            
            await sendTelegramNotification(
                `⚠️ <b>Duplicate Username Attempt</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> ${username}\n` +
                `❌ <b>Status:</b> Already exists in database`
            );
            
            return res.status(409).json({ 
                ok: false, 
                error: 'Username sudah ada. Pilih username lain.' 
            });
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

        // Forward to remote create-account endpoint
        let forwardResp;
        try {
            forwardResp = await fetch(REMOTE_CREATE_URL, {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'User-Agent': 'xcvi-system/2.0'
                },
                body: JSON.stringify({ 
                    username, 
                    password, 
                    role, 
                    expires: expired 
                })
            });
        } catch (e) {
            await sendTelegramNotification(
                `🔴 <b>Create Account Failed</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> ${username}\n` +
                `❌ <b>Error:</b> ${e.message}`
            );
            return res.status(502).json({ 
                ok: false, 
                error: 'Gagal memanggil server create-account', 
                details: e.message 
            });
        }

        let forwardData = {};
        try { 
            forwardData = await forwardResp.json(); 
        } catch (e) { 
            forwardData = {}; 
        }

        if (!forwardResp.ok || !forwardData.ok) {
            failCounts.set(ip, (failCounts.get(ip) || 0) + 1);
            
            await sendTelegramNotification(
                `🔴 <b>Account Creation Failed</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> ${username}\n` +
                `❌ <b>Error:</b> ${forwardData.error || 'Remote create failed'}`
            );
            
            return res.status(forwardResp.status || 500).json({ 
                ok: false, 
                error: forwardData.error || 'Remote create failed', 
                raw: forwardData 
            });
        }

        // Success: set cooldown for IP
        const until = nowTime + COOLDOWN_MS;
        cooldowns.set(ip, until);
        failCounts.set(ip, 0);

        // Update user stats
        await getTotalUsers();

        // Send success notification to Telegram
        await sendTelegramNotification(
            `✅ <b>New Account Created Successfully</b>\n` +
            `📱 <b>IP:</b> <code>${ip}</code>\n` +
            `👤 <b>Username:</b> <code>${username}</code>\n` +
            `🔑 <b>Password:</b> <code>${password}</code>\n` +
            `👑 <b>Role:</b> ${role}\n` +
            `📅 <b>Expires:</b> ${new Date(expired).toLocaleString('id-ID')}\n` +
            `👥 <b>Total Users:</b> ${userStats.totalUsers}\n` +
            `⏰ <b>Cooldown Until:</b> ${new Date(until).toLocaleString('id-ID')}`
        );

        // Respond to client with account data
        return res.status(200).json({
            ok: true,
            message: 'Akun berhasil dibuat',
            data: account,
            stats: userStats,
            createdAt,
            cooldownUntil: until,
            remoteResponse: forwardData
        });

    } catch (err) {
        console.error('[api/sistem] error:', err && err.message);
        
        // Send error notification to Telegram
        await sendTelegramNotification(
            `🔴 <b>System Error</b>\n` +
            `❌ <b>Error:</b> ${err.message || 'Unknown error'}\n` +
            `⏰ <b>Time:</b> ${new Date().toLocaleString('id-ID')}`
        );
        
        return res.status(500).json({ 
            ok: false, 
            error: err.message || 'Internal server error'
        });
    }
};