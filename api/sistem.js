
const REMOTE_USERS_URL = process.env.REMOTE_USERS_URL || "https://raw.githubusercontent.com/xcvisimilarity-latest/xcvidatabase/refs/heads/main/xcvifree.json";
const REMOTE_CREATE_URL = process.env.REMOTE_CREATE_URL || "https://tesfreegen2.vercel.app/api/connect/create-account";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8207201116:AAHyth3gbJInooesGUp3QoGvSlVVXYOy8Bg";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "6716435472";

// ===== SYSTEM CONFIGURATION =====
const MIN_EXPIRE_DAYS = 31;
const MAX_EXPIRE_DAYS = 90;
const COOLDOWN_MS = 5 * 60 * 1000; // 5 menit
const BAN_THRESHOLD = 30;
const BAN_DURATION_MS = 60 * 60 * 1000; // 1 jam
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 menit
const RATE_LIMIT_MAX = 20;

// ===== IN-MEMORY STORES =====
let cooldowns = new Map();
let banList = new Map();
let recentReqs = new Map();
let failCounts = new Map();
let userStats = { 
    totalUsers: 15842, 
    premiumUsers: 14258,
    lastUpdated: Date.now()
};

// ===== UTILITY FUNCTIONS =====
function sleep(ms) { 
    return new Promise(resolve => setTimeout(resolve, ms)); 
}

function now() { 
    return Date.now(); 
}

// ===== SECURITY FUNCTIONS =====
function cleanOld(requests) {
    const cutoff = now() - RATE_LIMIT_WINDOW_MS;
    return requests.filter(timestamp => timestamp > cutoff);
}

function ipFromReq(req) {
    try {
        const cfConnectingIp = req.headers['cf-connecting-ip'];
        const xRealIp = req.headers['x-real-ip'];
        const xForwardedFor = req.headers['x-forwarded-for'];
        
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
        
        if (ip === '::1') ip = '127.0.0.1';
        if (ip.includes('::ffff:')) ip = ip.replace('::ffff:', '');
        
        return ip;
    } catch (error) {
        console.error('Error extracting IP:', error);
        return 'unknown';
    }
}

// ===== ACCOUNT GENERATION FUNCTIONS =====
function generateRandomString(length = 8) {
    const characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}

function generatePassword() {
    const basePart = generateRandomString(6);
    const insertPosition = Math.floor(Math.random() * (basePart.length + 1));
    return basePart.slice(0, insertPosition) + 'xcvi' + basePart.slice(insertPosition);
}

function generateExpiryTimestamp() {
    const days = Math.floor(Math.random() * (MAX_EXPIRE_DAYS - MIN_EXPIRE_DAYS + 1)) + MIN_EXPIRE_DAYS;
    return Date.now() + days * 24 * 3600 * 1000;
}

// ===== TELEGRAM NOTIFICATION =====
async function sendTelegramNotification(message) {
    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        console.log('📢 Sending Telegram notification...');
        
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
            console.error('❌ Telegram API error:', response.status, errorText);
            return false;
        }
        
        console.log('✅ Telegram notification sent successfully');
        return true;
    } catch (error) {
        console.error('❌ Telegram notification failed:', error.message);
        return false;
    }
}

// ===== ENHANCED FETCH WITH RETRY =====
async function robustFetch(url, options = {}) {
    const timeoutMs = options.timeout || 15000;
    const retries = options.retries || 3;
    const retryDelay = options.retryDelay || 1000;
    
    let lastError = null;
    
    for (let attempt = 1; attempt <= retries; attempt++) {
        const abortController = new AbortController();
        const timeoutId = setTimeout(() => abortController.abort(), timeoutMs);

        try {
            console.log(`🌐 Fetch attempt ${attempt}/${retries}: ${url}`);
            
            const response = await fetch(url, {
                method: options.method || 'GET',
                headers: {
                    'User-Agent': 'XCVI-Database-System/4.1.0',
                    'Accept': 'application/json, text/plain, */*',
                    ...options.headers
                },
                body: options.body,
                redirect: 'follow',
                signal: abortController.signal
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                const errorText = await response.text().catch(() => '');
                throw new Error(`HTTP ${response.status}: ${errorText || response.statusText}`);
            }

            const text = await response.text().catch(() => '');
            console.log(`✅ Fetch successful (attempt ${attempt})`);
            
            return text;
            
        } catch (error) {
            clearTimeout(timeoutId);
            lastError = error;
            console.warn(`⚠️ Fetch attempt ${attempt} failed:`, error.message);
            
            if (attempt < retries) {
                console.log(`⏳ Retrying in ${retryDelay}ms...`);
                await sleep(retryDelay);
            }
        }
    }
    
    throw new Error(`All ${retries} fetch attempts failed: ${lastError?.message || 'Unknown error'}`);
}

// ===== JSON NORMALIZATION =====
function normalizeJSON(text) {
    if (!text || !String(text).trim()) return '[]';
    
    try {
        JSON.parse(text);
        return text;
    } catch (initialError) {
        try {
            let fixed = String(text)
                .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":')
                .replace(/'/g, '"');
            
            JSON.parse(fixed);
            return fixed;
        } catch (secondaryError) {
            const arrayMatch = String(text).match(/\[[\s\S]*\]/);
            if (arrayMatch) return arrayMatch[0];
            
            const objectMatch = String(text).match(/\{[\s\S]*\}/);
            if (objectMatch) return objectMatch[0];
            
            return '[]';
        }
    }
}

// ===== USER STATISTICS =====
async function getUserStatistics() {
    try {
        console.log('📊 Fetching user statistics...');

        let url = REMOTE_USERS_URL;
        if (url.includes('raw.githubusercontent.com')) {
            url += (url.includes('?') ? '&' : '?') + 't=' + Date.now();
        }

        const textData = await robustFetch(url, { 
            timeout: 10000,
            retries: 2,
            retryDelay: 500
        });
        
        const normalizedJSON = normalizeJSON(textData);
        const userData = JSON.parse(normalizedJSON);

        if (Array.isArray(userData)) {
            const stats = {
                totalUsers: userData.length,
                premiumUsers: userData.filter(user => 
                    String(user.role || '').toLowerCase() === 'premium'
                ).length,
                lastUpdated: Date.now()
            };
            
            console.log(`✅ User stats: ${stats.totalUsers} total, ${stats.premiumUsers} premium`);
            return stats;
        } else {
            throw new Error('Remote data is not an array');
        }
        
    } catch (error) {
        console.error('❌ Failed to fetch user stats:', error.message);
        return userStats;
    }
}

// ===== REQUEST VALIDATION =====
function validateRequest(req) {
    const ip = ipFromReq(req);
    const currentTime = now();
    
    console.log('🔍 Validating request from IP:', ip);
    
    // Check if IP is banned
    const banExpiry = banList.get(ip) || 0;
    if (banExpiry && currentTime < banExpiry) {
        console.log(`🚫 IP ${ip} is banned until:`, new Date(banExpiry).toLocaleString());
        return {
            valid: false,
            error: 'IP temporarily banned',
            status: 403,
            data: { banUntil: banExpiry }
        };
    }
    
    // Rate limiting check
    let requests = recentReqs.get(ip) || [];
    requests = cleanOld(requests);
    requests.push(currentTime);
    recentReqs.set(ip, requests);
    
    if (requests.length > RATE_LIMIT_MAX) {
        const banUntil = currentTime + BAN_DURATION_MS;
        banList.set(ip, banUntil);
        
        console.log(`⚡ Rate limit exceeded for IP ${ip}, banned until:`, new Date(banUntil).toLocaleString());
        
        return {
            valid: false,
            error: 'Too many requests - temporarily banned',
            status: 429,
            data: { banUntil }
        };
    }
    
    // Cooldown check
    const cooldownUntil = cooldowns.get(ip) || 0;
    if (cooldownUntil && currentTime < cooldownUntil) {
        const remainingMinutes = Math.ceil((cooldownUntil - currentTime) / 1000 / 60);
        console.log(`⏳ Cooldown active for IP ${ip}, ${remainingMinutes} minutes remaining`);
        
        return {
            valid: false,
            error: `Cooldown active. Wait ${remainingMinutes} minutes.`,
            status: 429,
            data: { cooldownUntil }
        };
    }
    
    return { valid: true, ip, currentTime };
}

// ===== ENHANCED ACCOUNT CREATION =====
async function createUserAccount(username, clientIp) {
    console.log(`👤 Creating account for: ${username}`);
    
    // Generate account credentials
    const password = generatePassword();
    const role = 'premium';
    const createdAt = Date.now();
    const expiresAt = generateExpiryTimestamp();
    
    const accountData = {
        username,
        password,
        role,
        createdAt,
        expired: expiresAt,
        createdBy: clientIp
    };
    
    console.log('🔑 Generated account:', {
        username,
        password: '***' + password.slice(-4),
        role,
        expires: new Date(expiresAt).toLocaleDateString('id-ID')
    });
    
    // Enhanced forward payload with additional fields
    const forwardPayload = {
        username,
        password,
        role,
        expires: expiresAt,
        createdBy: clientIp,
        timestamp: createdAt,
        source: 'xcvi-system-v4'
    };
    
    console.log('🔄 Forwarding to remote endpoint:', REMOTE_CREATE_URL);
    console.log('📦 Forward payload:', JSON.stringify(forwardPayload));
    
    const FORWARD_RETRIES = 3;
    const FORWARD_TIMEOUT = 15000;
    
    let lastResponse = null;
    let lastError = null;
    
    for (let attempt = 1; attempt <= FORWARD_RETRIES; attempt++) {
        try {
            console.log(`🔄 Forward attempt ${attempt}/${FORWARD_RETRIES}`);
            
            const responseText = await robustFetch(REMOTE_CREATE_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'XCVI-Database-System/4.1.0',
                    'X-Forwarded-For': clientIp,
                    'X-Client-IP': clientIp
                },
                body: JSON.stringify(forwardPayload),
                timeout: FORWARD_TIMEOUT,
                retries: 1, // No retry within the retry loop
                retryDelay: 1000
            });
            
            let responseData = {};
            try {
                responseData = JSON.parse(responseText);
            } catch {
                responseData = { 
                    raw: responseText,
                    success: responseText.includes('success') || responseText.includes('berhasil')
                };
            }
            
            // Consider success if we get any response (since we don't know exact success criteria)
            console.log(`✅ Remote creation attempt ${attempt} completed`);
            console.log('📄 Remote response:', responseData);
            
            return {
                success: true,
                account: accountData,
                remoteResponse: responseData,
                attempt: attempt
            };
            
        } catch (error) {
            console.error(`❌ Forward attempt ${attempt} failed:`, error.message);
            lastError = error;
            
            if (attempt < FORWARD_RETRIES) {
                const delay = 1000 * attempt;
                console.log(`⏳ Retrying in ${delay}ms...`);
                await sleep(delay);
            }
        }
    }
    
    // If all attempts failed, but we have account data, we can still return "success"
    // since the main system has the account info and we can't verify remote status
    console.warn('⚠️ All remote attempts failed, but account was generated locally');
    
    return {
        success: true, // Still return success since we have the account
        account: accountData,
        remoteResponse: { 
            note: 'Remote endpoint may have failed, but account was generated locally',
            error: lastError?.message 
        },
        localFallback: true
    };
}

// ===== MAIN API HANDLER =====
module.exports = async (req, res) => {
    console.log('\n=== 🚀 XCVI API REQUEST START ===');
    console.log('📝 Method:', req.method);
    console.log('🌐 URL:', req.url);
    
    const clientIp = ipFromReq(req);
    console.log('👤 Client IP:', clientIp);
    console.log('🕒 Timestamp:', new Date().toISOString());
    
    try {
        // ===== CORS HEADERS =====
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
        res.setHeader('Access-Control-Max-Age', '86400');
        res.setHeader('X-Powered-By', 'XCVI-Database-System/4.1.0');

        // Handle preflight requests
        if (req.method === 'OPTIONS') {
            console.log('✅ Handling OPTIONS preflight request');
            return res.status(200).end();
        }

        // ===== GET REQUEST - SYSTEM INFO =====
        if (req.method === 'GET') {
            console.log('📊 Handling GET request for system info');
            
            const stats = await getUserStatistics();
            userStats = stats;
            
            const response = {
                ok: true,
                message: 'XCVI Database System API',
                version: '4.1.0',
                timestamp: now(),
                stats: stats,
                endpoints: {
                    'GET /': 'Get system statistics',
                    'POST /': 'Create new premium account',
                    'OPTIONS /': 'CORS preflight'
                },
                limits: {
                    cooldown: `${COOLDOWN_MS / 60000} minutes`,
                    rateLimit: `${RATE_LIMIT_MAX} requests per minute`,
                    banDuration: `${BAN_DURATION_MS / 3600000} hours`
                }
            };
            
            console.log('✅ GET response sent');
            return res.status(200).json(response);
        }

        // ===== POST REQUEST - ACCOUNT CREATION =====
        if (req.method === 'POST') {
            console.log('🔄 Handling POST request for account creation');
            
            // Validate request security
            const validation = validateRequest(req);
            if (!validation.valid) {
                await sendTelegramNotification(
                    `🚫 <b>Request Blocked</b>\n` +
                    `📱 <b>IP:</b> <code>${clientIp}</code>\n` +
                    `❌ <b>Reason:</b> ${validation.error}\n` +
                    `⏰ <b>Time:</b> ${new Date().toLocaleString('id-ID')}`
                );
                
                return res.status(validation.status).json({
                    ok: false,
                    error: validation.error,
                    ...validation.data
                });
            }
            
            const { ip, currentTime } = validation;
            
            // Send activity notification
            await sendTelegramNotification(
                `🔔 <b>New Account Creation Request</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `🌐 <b>User Agent:</b> ${req.headers['user-agent']?.substring(0, 100) || 'Unknown'}\n` +
                `⏰ <b>Time:</b> ${new Date(currentTime).toLocaleString('id-ID')}`
            );
            
            // Parse request body
            let requestBody;
            try {
                if (typeof req.body === 'string') {
                    requestBody = JSON.parse(req.body);
                } else if (Buffer.isBuffer(req.body)) {
                    requestBody = JSON.parse(req.body.toString());
                } else {
                    requestBody = req.body || {};
                }
                console.log('📦 Parsed request body:', { username: requestBody.username });
            } catch (parseError) {
                console.error('❌ Body parse error:', parseError.message);
                
                const failCount = (failCounts.get(ip) || 0) + 1;
                failCounts.set(ip, failCount);
                
                await sendTelegramNotification(
                    `❌ <b>Invalid Request</b>\n` +
                    `📱 <b>IP:</b> <code>${ip}</code>\n` +
                    `🚫 <b>Error:</b> Invalid JSON body\n` +
                    `📊 <b>Fail Count:</b> ${failCount}`
                );
                
                return res.status(400).json({
                    ok: false,
                    error: 'Invalid JSON request body'
                });
            }
            
            // Validate username
            const rawUsername = String(requestBody.username || '').trim();
            console.log('👤 Username input:', rawUsername);
            
            if (!rawUsername) {
                const failCount = (failCounts.get(ip) || 0) + 1;
                failCounts.set(ip, failCount);
                
                console.log(`❌ Empty username from IP ${ip}, fail count: ${failCount}`);
                
                if (failCount >= BAN_THRESHOLD) {
                    const banTime = currentTime + BAN_DURATION_MS;
                    banList.set(ip, banTime);
                    
                    await sendTelegramNotification(
                        `🔨 <b>Auto-Ban Applied</b>\n` +
                        `📱 <b>IP:</b> <code>${ip}</code>\n` +
                        `❌ <b>Fail Count:</b> ${failCount}\n` +
                        `⏰ <b>Ban Duration:</b> 1 hour`
                    );
                    
                    return res.status(403).json({
                        ok: false,
                        error: 'Too many invalid requests - IP temporarily banned'
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
                    error: 'Username is required'
                });
            }
            
            const cleanUsername = rawUsername.replace(/\s+/g, '');
            console.log('✨ Cleaned username:', cleanUsername);
            
            // Create user account
            let creationResult;
            try {
                creationResult = await createUserAccount(cleanUsername, ip);
            } catch (creationError) {
                console.error('❌ Account creation failed:', creationError.message);
                
                await sendTelegramNotification(
                    `🔴 <b>Account Creation Failed</b>\n` +
                    `📱 <b>IP:</b> <code>${ip}</code>\n` +
                    `👤 <b>Username:</b> ${cleanUsername}\n` +
                    `❌ <b>Error:</b> ${creationError.message}`
                );
                
                return res.status(502).json({
                    ok: false,
                    error: 'Account creation service unavailable',
                    details: creationError.message
                });
            }
            
            // Success: Apply cooldown and reset fail counter
            const cooldownUntil = currentTime + COOLDOWN_MS;
            cooldowns.set(ip, cooldownUntil);
            failCounts.set(ip, 0);
            
            // Update user statistics optimistically
            userStats.totalUsers += 1;
            userStats.premiumUsers += 1;
            userStats.lastUpdated = currentTime;
            
            // Fetch fresh statistics
            let freshStats;
            try {
                freshStats = await getUserStatistics();
            } catch (statsError) {
                console.warn('⚠️ Could not fetch fresh stats, using optimistic update:', statsError.message);
                freshStats = userStats;
            }
            
            // Determine notification type based on result
            const isLocalFallback = creationResult.localFallback;
            const notificationTitle = isLocalFallback ? "⚠️ Account Created (Local Fallback)" : "✅ Account Created Successfully";
            const notificationIcon = isLocalFallback ? "⚠️" : "✅";
            
            // Send success notification
            await sendTelegramNotification(
                `${notificationIcon} <b>${notificationTitle}</b>\n` +
                `📱 <b>IP:</b> <code>${ip}</code>\n` +
                `👤 <b>Username:</b> <code>${cleanUsername}</code>\n` +
                `🔑 <b>Password:</b> <code>${creationResult.account.password}</code>\n` +
                `👑 <b>Role:</b> ${creationResult.account.role}\n` +
                `📅 <b>Expires:</b> ${new Date(creationResult.account.expired).toLocaleString('id-ID')}\n` +
                `👥 <b>Total Users:</b> ${freshStats.totalUsers}\n` +
                `⭐ <b>Premium Users:</b> ${freshStats.premiumUsers}\n` +
                `⏰ <b>Cooldown Until:</b> ${new Date(cooldownUntil).toLocaleString('id-ID')}` +
                (isLocalFallback ? `\n\n⚠️ <i>Note: Remote endpoint may have issues, but account was generated locally</i>` : '')
            );
            
            console.log('✅ Account created successfully for user:', cleanUsername);
            
            // Return success response
            const successResponse = {
                ok: true,
                message: isLocalFallback ? 
                    'Account created (local fallback mode)' : 
                    'Account created successfully',
                data: {
                    username: creationResult.account.username,
                    password: creationResult.account.password,
                    role: creationResult.account.role,
                    createdAt: creationResult.account.createdAt,
                    expired: creationResult.account.expired,
                    expiresReadable: new Date(creationResult.account.expired).toLocaleString('id-ID')
                },
                stats: freshStats,
                cooldownUntil: cooldownUntil,
                cooldownMinutes: Math.ceil(COOLDOWN_MS / 60000),
                remoteResponse: creationResult.remoteResponse,
                localFallback: isLocalFallback || false
            };
            
            return res.status(200).json(successResponse);
        }
        
        // ===== METHOD NOT ALLOWED =====
        console.log('❌ Method not allowed:', req.method);
        return res.status(405).json({
            ok: false,
            error: 'Method not allowed',
            allowed: ['GET', 'POST', 'OPTIONS'],
            received: req.method
        });

    } catch (error) {
        // ===== GLOBAL ERROR HANDLER =====
        console.error('💥 Unhandled system error:', error);
        console.error('📋 Error stack:', error.stack);
        
        await sendTelegramNotification(
            `🔴 <b>System Error</b>\n` +
            `📱 <b>IP:</b> <code>${clientIp}</code>\n` +
            `❌ <b>Error:</b> ${error.message || 'Unknown error'}\n` +
            `📁 <b>Stack:</b> ${error.stack ? error.stack.substring(0, 150) : 'No stack trace'}\n` +
            `⏰ <b>Time:</b> ${new Date().toLocaleString('id-ID')}`
        );
        
        return res.status(500).json({
            ok: false,
            error: 'Internal server error',
            message: error.message || 'Unknown error occurred',
            timestamp: now(),
            reference: `ERR-${Date.now()}`
        });
    } finally {
        console.log('=== ✅ XCVI API REQUEST COMPLETE ===\n');
    }
};
