'use strict';

const express = require('express');
const http    = require('http');
const { WebSocketServer } = require('ws');
const path    = require('path');
const fs      = require('fs');

// =========================================================
// FIREBASE ADMIN SDK — compatible firebase-admin@14.x
//
// Dans firebase-admin >= 11, admin.credential N'EXISTE PLUS.
// La fonction cert() est directement sur admin :
//   admin.cert(serviceAccount)  ← CORRECT
//   admin.credential.cert(...)  ← ERREUR en v14
// =========================================================

const admin = require('firebase-admin');
const { getMessaging } = require('firebase-admin/messaging');

let firebaseInitialized = false;
let messagingInstance   = null;

(function initFirebase() {

    const serviceAccountPath = path.join(__dirname, 'serviceAccountKey.json');

    if (!fs.existsSync(serviceAccountPath)) {
        console.error('❌ serviceAccountKey.json introuvable :', serviceAccountPath);
        console.error('⚠️  Firebase désactivé — le serveur WebSocket reste opérationnel.');
        return;
    }

    let serviceAccount;
    try {
        const raw = fs.readFileSync(serviceAccountPath, 'utf8');
        serviceAccount = JSON.parse(raw);
    } catch (parseErr) {
        console.error('❌ Impossible de parser serviceAccountKey.json :', parseErr.message);
        console.error('⚠️  Firebase désactivé — le serveur WebSocket reste opérationnel.');
        return;
    }

    if (!serviceAccount.project_id || !serviceAccount.private_key || !serviceAccount.client_email) {
        console.error('❌ serviceAccountKey.json invalide (champs manquants : project_id / private_key / client_email).');
        console.error('⚠️  Firebase désactivé — le serveur WebSocket reste opérationnel.');
        return;
    }

    try {
        admin.initializeApp({
            credential: admin.cert(serviceAccount)
        });

        firebaseInitialized = true;
        messagingInstance   = getMessaging();

        console.log('🔥 Firebase Admin SDK initialisé avec succès');
        console.log('📲 Firebase Cloud Messaging prêt');

    } catch (initErr) {
        console.error('❌ Échec admin.initializeApp :', initErr.message);
        console.error('⚠️  Firebase désactivé — le serveur WebSocket reste opérationnel.');
    }

})();

// =========================================================
// SERVEUR HTTP
// =========================================================

const app    = express();
const server = http.createServer(app);

app.use(express.json());

// Configuration CORS pour Render/Localhost
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// =========================================================
// WEBSOCKET
// =========================================================

const wss = new WebSocketServer({ server });

// =========================================================
// STOCKAGE EN MÉMOIRE
// =========================================================

// Connexions WebSocket actives : userId -> WebSocket
const users = new Map();

// Tokens FCM persistants : userId -> fcmToken
// IMPORTANT : le token n'est JAMAIS supprimé lorsque le WebSocket se ferme.
const fcmTokens = new Map();

// Présence / occupation : userId -> AVAILABLE | RINGING | IN_CALL | OFFLINE
const USER_STATE = Object.freeze({
    AVAILABLE: 'AVAILABLE',
    RINGING: 'RINGING',
    IN_CALL: 'IN_CALL',
    OFFLINE: 'OFFLINE'
});
const userStates = new Map();

// Appels en attente (utilisateur hors ligne) : targetId -> { callId, callerId, offer, timestamp, expiryTimer }
const pendingCalls = new Map();

// Appels actifs : callId -> { callId, callerId, calleeId, targetId, status, createdAt }
// status: 'ringing' | 'connecting' | 'active' | 'ended'
const activeCalls = new Map();

// =========================================================
// STRUCTURES POUR APPELS DE GROUPE
// =========================================================

// Appels de groupe : callId -> { callId, participants: Set, createdBy, createdAt, isGroup }
const groupCalls = new Map();

// État des appels de groupe : callId -> Set of userId (participants actifs)
const groupCallParticipants = new Map();

// Durée max avant expiration d'un appel en attente (45 secondes)
const PENDING_CALL_TTL_MS = 45000;

// =========================================================
// UTILITAIRES
// =========================================================

/**
 * Génère un callId unique de type CALL-xxxxxxxx
 */
function generateCallId() {
    return 'CALL-' + Math.random().toString(36).substr(2, 8).toUpperCase();
}

/**
 * Valide qu'une chaîne est non vide et n'est pas trop longue (anti-injection).
 */
function isValidString(value, maxLen = 256) {
    return typeof value === 'string' && value.trim().length > 0 && value.length <= maxLen;
}

function getUserState(userId) {
    if (!userId) return USER_STATE.OFFLINE;
    if (userStates.has(userId)) return userStates.get(userId);
    const ws = users.get(userId);
    return (ws && ws.readyState === 1) ? USER_STATE.AVAILABLE : USER_STATE.OFFLINE;
}

function setUserState(userId, state) {
    if (!userId || !state) return;
    userStates.set(userId, state);
    console.log(`👤 État ${userId} → ${state}`);
}

function isUserBusy(userId) {
    const state = getUserState(userId);
    return state === USER_STATE.IN_CALL || state === USER_STATE.RINGING;
}

// =========================================================
// UTILITAIRES APPELS DE GROUPE
// =========================================================

/**
 * Vérifie si un utilisateur participe à un appel de groupe.
 * 
 * @param {string} callId - Identifiant de l'appel
 * @param {string} userId - Identifiant de l'utilisateur
 * @returns {boolean}
 */
function isParticipantInGroupCall(callId, userId) {
    const participants = groupCallParticipants.get(callId);
    return participants && participants.has(userId);
}

/**
 * Vérifie si un appel de groupe existe.
 * 
 * @param {string} callId - Identifiant de l'appel
 * @returns {boolean}
 */
function groupCallExists(callId) {
    return groupCalls.has(callId);
}

/**
 * Crée un appel de groupe.
 * 
 * @param {string} callId - Identifiant de l'appel
 * @param {string} createdBy - Utilisateur créateur
 * @returns {Object}
 */
function createGroupCall(callId, createdBy) {
    const participants = new Set();
    participants.add(createdBy);
    
    groupCalls.set(callId, {
        callId,
        participants,
        createdBy,
        createdAt: Date.now(),
        isGroup: true
    });
    
    groupCallParticipants.set(callId, participants);
    
    console.log(`[GROUP CALL] Création : ${callId} par ${createdBy}`);
    return groupCalls.get(callId);
}

/**
 * Ajoute un participant à un appel de groupe.
 * 
 * @param {string} callId - Identifiant de l'appel
 * @param {string} userId - Utilisateur à ajouter
 * @returns {boolean}
 */
function addParticipantToGroupCall(callId, userId) {
    const groupCall = groupCalls.get(callId);
    if (!groupCall) {
        console.warn(`[GROUP CALL ERROR] Tentative d'ajout à un appel inexistant : ${callId}`);
        return false;
    }
    
    const participants = groupCallParticipants.get(callId);
    if (!participants) {
        console.warn(`[GROUP CALL ERROR] Participants Set inexistant pour ${callId}`);
        return false;
    }
    
    if (participants.has(userId)) {
        console.warn(`[GROUP CALL] ${userId} est déjà participant de ${callId}`);
        return false;
    }
    
    participants.add(userId);
    groupCall.participants = participants;
    
    console.log(`[GROUP CALL] Participant ajouté : ${userId} → ${callId}`);
    return true;
}

/**
 * Retire un participant d'un appel de groupe.
 * 
 * @param {string} callId - Identifiant de l'appel
 * @param {string} userId - Utilisateur à retirer
 * @returns {boolean}
 */
function removeParticipantFromGroupCall(callId, userId) {
    const participants = groupCallParticipants.get(callId);
    if (!participants) {
        console.warn(`[GROUP CALL ERROR] Participants Set inexistant pour ${callId}`);
        return false;
    }
    
    if (!participants.has(userId)) {
        console.warn(`[GROUP CALL] ${userId} n'est pas participant de ${callId}`);
        return false;
    }
    
    participants.delete(userId);
    
    const groupCall = groupCalls.get(callId);
    if (groupCall) {
        groupCall.participants = participants;
    }
    
    console.log(`[GROUP CALL] Participant retiré : ${userId} ← ${callId}`);
    
    // Si moins de 2 participants, supprimer l'appel
    if (participants.size < 2) {
        console.log(`[GROUP CALL] Suppression de ${callId} (moins de 2 participants)`);
        groupCalls.delete(callId);
        groupCallParticipants.delete(callId);
    }
    
    return true;
}

/**
 * Termine complètement un appel de groupe.
 * 
 * @param {string} callId - Identifiant de l'appel
 */
function endGroupCall(callId) {
    console.log(`[GROUP CALL] Terminaison : ${callId}`);
    groupCalls.delete(callId);
    groupCallParticipants.delete(callId);
}

function findCallForUser(userId) {
    for (const call of activeCalls.values()) {
        if (call.callerId === userId || call.calleeId === userId || call.targetId === userId) {
            return call;
        }
    }
    return null;
}

function sendToUser(userId, payload) {
    const targetWs = users.get(userId);
    if (targetWs && targetWs.readyState === 1) {
        targetWs.send(JSON.stringify(payload));
        return true;
    }
    return false;
}

function endCallSession(callId, reason) {
    const call = callId ? activeCalls.get(callId) : null;
    if (!call) return null;

    clearTimeout(call.expiryTimer);
    activeCalls.delete(callId);
    removePendingCall(call.calleeId || call.targetId);
    removePendingCall(call.callerId);

    if (getUserState(call.callerId) !== USER_STATE.OFFLINE) {
        setUserState(call.callerId, USER_STATE.AVAILABLE);
    }
    const calleeId = call.calleeId || call.targetId;
    if (getUserState(calleeId) !== USER_STATE.OFFLINE) {
        setUserState(calleeId, USER_STATE.AVAILABLE);
    }

    console.log(`📴 Appel ${callId} terminé (${reason || 'ended'})`);
    return call;
}

/**
 * Annule et supprime un appel en attente, en nettoyant son timer d'expiration.
 */
function removePendingCall(targetId) {
    const pending = pendingCalls.get(targetId);
    if (pending) {
        clearTimeout(pending.expiryTimer);
        pendingCalls.delete(targetId);
        console.log(`🗑️  Appel en attente supprimé pour ${targetId}`);
    }
}

/**
 * Planifie l'expiration automatique d'un appel en attente.
 */
function schedulePendingCallExpiry(targetId, callId) {
    return setTimeout(() => {
        const pending = pendingCalls.get(targetId);
        if (pending && pending.callId === callId) {
            console.log(`⏰ Appel ${callId} expiré pour ${targetId} (aucune réponse en ${PENDING_CALL_TTL_MS / 1000}s)`);
            pendingCalls.delete(targetId);

            const callerId = pending.callerId;
            endCallSession(callId, 'no-answer');

            sendToUser(callerId, {
                type: 'call-timeout',
                callId: callId,
                targetId: targetId,
                message: 'Aucune réponse du destinataire.'
            });
            sendToUser(targetId, {
                type: 'cancel-call',
                callId: callId,
                from: callerId,
                reason: 'no-answer'
            });
        }
    }, PENDING_CALL_TTL_MS);
}

// =========================================================
// ROUTE PRINCIPALE
// =========================================================

app.get('/', (req, res) => {
    res.send('Serveur WebSocket d\'Appel WebRTC + FCM actif');
});

// =========================================================
// ROUTE USERS (debug)
// =========================================================

app.get('/users', (req, res) => {

    const list = [];

    users.forEach((ws, userId) => {
        list.push({
            userId,
            connected: ws.readyState === 1,
            state: getUserState(userId),
            hasFcmToken: fcmTokens.has(userId)
        });
    });

    fcmTokens.forEach((token, userId) => {
        if (!list.some((u) => u.userId === userId)) {
            list.push({ userId, connected: false, state: getUserState(userId), hasFcmToken: true });
        }
    });

    const pendingList = [];
    pendingCalls.forEach((call, targetId) => {
        pendingList.push({
            targetId,
            callId: call.callId,
            callerId: call.callerId,
            age: Date.now() - call.timestamp
        });
    });

    const groupCallsList = [];
    groupCalls.forEach((call, callId) => {
        const participantsArray = Array.from(call.participants || []);
        groupCallsList.push({
            callId,
            createdBy: call.createdBy,
            participantCount: participantsArray.length,
            participants: participantsArray,
            createdAt: call.createdAt
        });
    });

    res.json({
        activeConnections: users.size,
        storedTokens: fcmTokens.size,
        pendingCallsCount: pendingCalls.size,
        activeCallsCount: activeCalls.size,
        groupCallsCount: groupCalls.size,
        firebaseInitialized,
        userStates: Object.fromEntries(userStates),
        users: list,
        pendingCalls: pendingList,
        activeCalls: Array.from(activeCalls.values()).map((c) => ({
            callId: c.callId,
            callerId: c.callerId,
            calleeId: c.calleeId,
            status: c.status,
            createdAt: c.createdAt
        })),
        groupCalls: groupCallsList
    });

});

// =========================================================
// FONCTION : ENVOYER UNE NOTIFICATION FCM D'APPEL ENTRANT
// =========================================================

async function sendIncomingCallNotification(targetId, callerId, callId) {

    console.log('----------------------------------------------------');
    console.log(`📲 Préparation notification FCM`);
    console.log(`   👤 Appelant  : ${callerId}`);
    console.log(`   🎯 Destinataire : ${targetId}`);
    console.log(`   🆔 Call ID   : ${callId}`);

    if (!firebaseInitialized || !messagingInstance) {
        console.error('❌ Firebase Cloud Messaging n\'est pas initialisé');
        return false;
    }

    const token = fcmTokens.get(targetId);

    if (!token) {
        console.log(`❌ Aucun token FCM pour ce destinataire`);
        return false;
    }

    console.log(`✅ Token FCM trouvé`);
    console.log(`📲 Envoi notification FCM...`);

    const message = {
        token: token,
        // Style WhatsApp : nom de l'appelant en titre, type d'appel en sous-titre
        notification: {
            title: String(callerId),
            body: 'Appel vocal entrant'
        },
        // data pour que l'app puisse récupérer l'appel à l'ouverture
        data: {
            type: 'incoming-call',
            callId: String(callId),
            callerId: String(callerId),
            targetId: String(targetId)
        },
        android: {
            priority: 'high',
            ttl: PENDING_CALL_TTL_MS,
            notification: {
                channelId: 'incoming_calls',
                icon: 'notification_icon',
                color: '#25D366',
                sound: 'default',
                defaultSound: true,
                defaultVibrateTimings: false,
                vibrateTimingsMillis: [0, 1000, 500, 1000, 500, 1000],
                priority: 'max',
                visibility: 'public',
                tag: String(callId),
                sticky: true,
                ticker: `Appel vocal de ${callerId}`,
                notificationCount: 1
            }
        }
    };

    try {

        const response = await messagingInstance.send(message);
        console.log(`✅ Notification FCM envoyée`);
        console.log(`📨 Firebase Message ID : ${response}`);
        console.log('----------------------------------------------------');
        return true;

    } catch (error) {

        console.error(`❌ ERREUR FCM`);
        console.error(`Code : ${error.code}`);
        console.error(`Message : ${error.message}`);

        // Supprimer les tokens définitivement invalides
        if (
            error.code === 'messaging/registration-token-not-registered' ||
            error.code === 'messaging/invalid-registration-token'
        ) {
            console.warn(`🗑️  Token FCM invalide → suppression pour ${targetId}`);
            fcmTokens.delete(targetId);
        }

        return false;
    }
}

// =========================================================
// FONCTION : ENVOYER UNE NOTIFICATION FCM D'APPEL DE GROUPE
// =========================================================

/**
 * Envoie une notification FCM pour inviter un utilisateur à rejoindre un appel de groupe.
 * 
 * @param {string} targetId - ID de l'utilisateur cible
 * @param {string} callId - ID de l'appel de groupe
 * @param {string} callerId - ID de l'utilisateur qui a créé l'appel
 * @param {string} addedBy - ID de l'utilisateur qui ajoute le participant
 * @returns {Promise<boolean>}
 */
async function sendGroupCallInvitationNotification(targetId, callId, callerId, addedBy) {
    console.log('----------------------------------------------------');
    console.log(`[GROUP FCM] Préparation notification d'invitation`);
    console.log(`   👤 Ajouté par : ${addedBy}`);
    console.log(`   🎯 Destinataire : ${targetId}`);
    console.log(`   🆔 Call ID : ${callId}`);
    console.log(`   👤 Créateur : ${callerId}`);

    if (!firebaseInitialized || !messagingInstance) {
        console.error('[GROUP FCM ERROR] Firebase Cloud Messaging n\'est pas initialisé');
        return false;
    }

    const token = fcmTokens.get(targetId);

    if (!token) {
        console.log(`[GROUP FCM] Aucun token FCM pour ${targetId}`);
        return false;
    }

    console.log(`[GROUP FCM] Token FCM trouvé`);
    console.log(`[GROUP FCM] Envoi notification...`);

    const message = {
        token: token,
        notification: {
            title: 'Appel audio de groupe',
            body: `${addedBy} vous invite à rejoindre un appel`
        },
        data: {
            type: 'group-incoming-call',
            callId: String(callId),
            callerId: String(callerId),
            addedBy: String(addedBy),
            targetId: String(targetId),
            callType: 'audio'
        },
        android: {
            priority: 'high',
            ttl: PENDING_CALL_TTL_MS,
            notification: {
                channelId: 'incoming_calls',
                icon: 'notification_icon',
                color: '#25D366',
                sound: 'default',
                defaultSound: true,
                defaultVibrateTimings: false,
                vibrateTimingsMillis: [0, 1000, 500, 1000, 500, 1000],
                priority: 'max',
                visibility: 'public',
                tag: String(callId),
                sticky: true,
                ticker: `Invitation d'appel de groupe de ${addedBy}`,
                notificationCount: 1
            }
        }
    };

    try {
        const response = await messagingInstance.send(message);
        console.log(`[GROUP FCM] Notification envoyée avec succès`);
        console.log(`[GROUP FCM] Firebase Message ID : ${response}`);
        console.log('----------------------------------------------------');
        return true;
    } catch (error) {
        console.error(`[GROUP FCM ERROR] Erreur d'envoi`);
        console.error(`Code : ${error.code}`);
        console.error(`Message : ${error.message}`);

        if (
            error.code === 'messaging/registration-token-not-registered' ||
            error.code === 'messaging/invalid-registration-token'
        ) {
            console.warn(`[GROUP FCM] Token FCM invalide → suppression pour ${targetId}`);
            fcmTokens.delete(targetId);
        }

        return false;
    }
}

// =========================================================
// GESTION DES CONNEXIONS WEBSOCKET
// =========================================================

wss.on('connection', (ws) => {

    console.log('🔌 Nouvelle connexion WebSocket entrante...');
    ws.isAlive = true;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    // ---------------------------------------------------------
    // Réception des messages
    // ---------------------------------------------------------
    ws.on('message', async (message) => {

        let data;
        try {
            data = JSON.parse(message);
        } catch (parseErr) {
            console.error('❌ Message JSON invalide reçu, ignoré.');
            return;
        }

        const { type } = data;

        if (!isValidString(type, 64)) {
            console.warn('⚠️  Message reçu sans type valide, ignoré.');
            return;
        }

        try {

            // =================================================
            // 1. REGISTER USER
            // =================================================
            if (type === 'register-user') {

                const userId = data.userId;

                if (!isValidString(userId, 64)) {
                    console.warn('⚠️  register-user reçu avec userId invalide');
                    return;
                }

                // Fermer l'ancienne connexion si elle existe
                const existingWs = users.get(userId);
                if (existingWs && existingWs !== ws) {
                    console.log(`⚠️  Ancienne connexion trouvée pour ${userId}, fermeture...`);
                    try { existingWs.close(); } catch (e) { /* ignore */ }
                }

                users.set(userId, ws);
                ws.userId = userId;

                if (!isUserBusy(userId)) {
                    setUserState(userId, USER_STATE.AVAILABLE);
                }

                // Enregistrer le token FCM s'il est fourni
                if (isValidString(data.fcmToken, 4096)) {
                    fcmTokens.set(userId, data.fcmToken.trim());
                    console.log(`🔥 FCM TOKEN ENREGISTRÉ`);
                    console.log(`   👤 User  : ${userId}`);
                    console.log(`   📡 Token : ${data.fcmToken.substring(0, 20)}...`);
                } else {
                    const existingToken = fcmTokens.get(userId);
                    if (existingToken) {
                        console.log(`ℹ️  Token FCM existant conservé pour ${userId}`);
                    } else {
                        console.log(`⚠️  Aucun token FCM fourni pour ${userId}`);
                    }
                }

                console.log(`👤 Utilisateur enregistré : ${userId}`);

                // Réponse de confirmation au client
                ws.send(JSON.stringify({
                    type: 'registered',
                    userId: userId
                }));

                // Vérifier s'il y a un appel en attente pour cet utilisateur
                const pending = pendingCalls.get(userId);
                if (pending) {
                    console.log(`📞 Appel en attente trouvé pour ${userId} → envoi immédiat`);
                    ws.send(JSON.stringify({
                        type: 'pending-call',
                        callId: pending.callId,
                        callerId: pending.callerId,
                        offer: pending.offer
                    }));
                }

                return;
            }

            // =================================================
            // 2. UPDATE FCM TOKEN
            // =================================================
            if (type === 'update-fcm-token') {

                const targetUserId = isValidString(data.userId, 64) ? data.userId : ws.userId;

                if (!targetUserId) {
                    console.warn('⚠️  update-fcm-token sans userId identifiable');
                    return;
                }

                // Sécurité : seul le propriétaire de la connexion peut mettre à jour son token
                if (ws.userId && ws.userId !== targetUserId) {
                    console.warn(`⚠️  Tentative de mise à jour du token d'un autre utilisateur refusée`);
                    return;
                }

                if (!isValidString(data.fcmToken, 4096)) {
                    console.warn('⚠️  update-fcm-token : token invalide ou vide');
                    return;
                }

                fcmTokens.set(targetUserId, data.fcmToken.trim());

                console.log(`🔥 FCM TOKEN MIS À JOUR`);
                console.log(`   👤 User  : ${targetUserId}`);
                console.log(`   📡 Token : ${data.fcmToken.substring(0, 20)}...`);

                return;
            }

            // =================================================
            // 3. APPEL (call-user)
            // =================================================
            if (type === 'call-user') {

                const callerId = ws.userId || data.from;
                const targetId = data.targetId;

                // Validation
                if (!isValidString(callerId, 64) || !isValidString(targetId, 64)) {
                    console.warn('⚠️  call-user : callerId ou targetId invalide');
                    return;
                }

                if (callerId === targetId) {
                    console.warn('⚠️  call-user : appel vers soi-même refusé');
                    return;
                }

                if (isUserBusy(callerId) && findCallForUser(callerId)) {
                    ws.send(JSON.stringify({
                        type: 'user-busy',
                        callId: data.callId || null,
                        targetId: callerId,
                        message: 'Vous êtes déjà en appel.'
                    }));
                    return;
                }

                if (isUserBusy(targetId)) {
                    console.log(`🚫 ${targetId} occupé (${getUserState(targetId)})`);
                    ws.send(JSON.stringify({
                        type: 'user-busy',
                        callId: data.callId || null,
                        targetId: targetId,
                        message: 'Le correspondant est déjà en communication.'
                    }));
                    return;
                }

                // Générer ou réutiliser le callId (le client peut en envoyer un)
                const callId = isValidString(data.callId, 64) ? data.callId : generateCallId();

                console.log('====================================================');
                console.log('📞 NOUVEL APPEL');
                console.log('====================================================');
                console.log(`📤 Appelant : ${callerId}`);
                console.log(`📥 Destinataire : ${targetId}`);
                console.log(`🆔 Call ID : ${callId}`);

                removePendingCall(targetId);
                const expiryTimer = schedulePendingCallExpiry(targetId, callId);
                pendingCalls.set(targetId, {
                    callId,
                    callerId,
                    offer: data.offer,
                    timestamp: Date.now(),
                    expiryTimer
                });

                activeCalls.set(callId, {
                    callId,
                    callerId,
                    calleeId: targetId,
                    targetId,
                    status: 'ringing',
                    createdAt: Date.now(),
                    expiryTimer
                });

                setUserState(callerId, USER_STATE.RINGING);
                setUserState(targetId, USER_STATE.RINGING);

                const targetWs = users.get(targetId);
                const targetOnline = !!(targetWs && targetWs.readyState === 1);

                if (targetOnline) {
                    console.log(`🟢 WebSocket ${targetId} EN LIGNE`);

                    // B est connecté via WebSocket → transmission directe
                    targetWs.send(JSON.stringify({
                        type: 'incoming-call',
                        callId: callId,
                        from: callerId,
                        offer: data.offer
                    }));

                    ws.send(JSON.stringify({
                        type: 'call-ringing',
                        callId: callId,
                        targetId: targetId,
                        message: 'Sonnerie...'
                    }));

                    console.log(`   📤 Appel transmis par WebSocket à ${targetId}`);
                    console.log('====================================================');

                } else {
                    console.log(`🔴 Destinataire WebSocket hors ligne`);
                    console.log(`🔥 Recherche token FCM...`);

                    // B est hors ligne → tenter via FCM
                    const targetToken = fcmTokens.get(targetId);

                    if (targetToken && firebaseInitialized) {

                        const notificationSent = await sendIncomingCallNotification(targetId, callerId, callId);

                        if (notificationSent) {
                            // Informer l'appelant que la notification a été envoyée
                            ws.send(JSON.stringify({
                                type: 'call-ringing-fcm',
                                callId: callId,
                                targetId: targetId,
                                message: 'Notification envoyée au destinataire.'
                            }));
                        } else {
                            console.warn(`   ⚠️  Échec notification FCM vers ${targetId}`);
                            ws.send(JSON.stringify({
                                type: 'user-offline',
                                callId: callId,
                                targetId: targetId,
                                message: 'Le destinataire est hors ligne et la notification FCM a échoué.'
                            }));
                            endCallSession(callId, 'fcm-failed');
                        }

                    } else {

                        console.warn(`   ⚠️  Aucune notification possible pour ${targetId}`);

                        ws.send(JSON.stringify({
                            type: 'user-offline',
                            callId: callId,
                            targetId: targetId,
                            message: 'Le correspondant est hors ligne et ne possède aucun token FCM.'
                        }));

                        endCallSession(callId, 'offline');
                    }

                    console.log('====================================================');
                }

                return;
            }

            // =================================================
            // 4. GET PENDING CALL (demande au démarrage après notification FCM)
            // =================================================
            if (type === 'get-pending-call') {

                const userId = data.userId || ws.userId;

                if (!isValidString(userId, 64)) {
                    ws.send(JSON.stringify({ type: 'no-pending-call' }));
                    return;
                }

                const pending = pendingCalls.get(userId);

                if (pending) {
                    console.log(`📞 get-pending-call → appel trouvé pour ${userId}`);
                    console.log(`   🆔 Call ID  : ${pending.callId}`);
                    console.log(`   👤 Appelant : ${pending.callerId}`);

                    ws.send(JSON.stringify({
                        type: 'pending-call',
                        callId: pending.callId,
                        callerId: pending.callerId,
                        offer: pending.offer
                    }));
                } else {
                    console.log(`ℹ️  get-pending-call → aucun appel en attente pour ${userId}`);
                    ws.send(JSON.stringify({ type: 'no-pending-call' }));
                }

                return;
            }

            // =================================================
            // 5. ANSWER CALL (réponse de B vers A)
            // =================================================
            if (type === 'answer-call') {

                const targetId  = data.targetId;
                const responderId = ws.userId || data.from;
                const callId    = data.callId;

                if (!isValidString(targetId, 64) || !isValidString(responderId, 64)) {
                    console.warn('⚠️  answer-call : données invalides');
                    return;
                }

                // Nettoyer l'appel en attente car il a été répondu
                removePendingCall(responderId);

                if (callId && activeCalls.has(callId)) {
                    const call = activeCalls.get(callId);
                    call.status = 'connecting';
                    setUserState(call.callerId, USER_STATE.IN_CALL);
                    setUserState(call.calleeId || call.targetId, USER_STATE.IN_CALL);
                } else {
                    setUserState(responderId, USER_STATE.IN_CALL);
                    setUserState(targetId, USER_STATE.IN_CALL);
                }

                const targetWs = users.get(targetId);

                if (targetWs && targetWs.readyState === 1) {

                    console.log(`✅ APPEL ACCEPTÉ`);
                    console.log(`   👤 ${responderId} → ${targetId}`);
                    if (callId) console.log(`   🆔 Call ID : ${callId}`);

                    targetWs.send(JSON.stringify({
                        type: 'call-answered',
                        callId: callId || null,
                        from: responderId,
                        answer: data.answer
                    }));

                } else {
                    console.warn(`⚠️  Impossible d'envoyer la réponse : ${targetId} hors ligne`);
                }

                return;
            }

            // =================================================
            // 6. ICE CANDIDATE
            // =================================================
            if (type === 'ice-candidate' || type === 'ice-restart-offer' || type === 'ice-restart-answer') {

                const targetId = data.targetId;
                const senderId = ws.userId || data.from;
                const callId   = data.callId;

                if (!isValidString(targetId, 64)) return;

                sendToUser(targetId, {
                    type: type,
                    callId: callId || null,
                    candidate: data.candidate || null,
                    offer: data.offer || null,
                    answer: data.answer || null,
                    from: senderId
                });

                return;
            }

            // =================================================
            // 6.5 CALL CONNECTED (confirmation client WebRTC connecté)
            // =================================================
            if (type === 'call-connected') {
                const callerId = data.from || ws.userId;
                const callId = data.callId;

                if (callId && activeCalls.has(callId)) {
                    const call = activeCalls.get(callId);
                    call.status = 'active';
                    console.log(`✅ Appel ${callId} marqué comme connecté par ${callerId}`);
                }

                return;
            }

            // =================================================
            // 7. REFUS D'APPEL
            // =================================================
            if (type === 'call-refused') {

                const targetId = data.targetId;
                const refuserId = ws.userId || data.from;
                const callId   = data.callId;

                if (!isValidString(targetId, 64)) return;

                // Sécurité légère : vérifier que l'appelant existe
                const targetWs = users.get(targetId);

                console.log(`📵 REFUS D'APPEL`);
                console.log(`   👤 ${refuserId} → ${targetId}`);
                if (callId) console.log(`   🆔 Call ID : ${callId}`);

                // Nettoyer l'appel
                removePendingCall(refuserId);
                endCallSession(callId, 'refused');
                if (!callId) {
                    setUserState(refuserId, USER_STATE.AVAILABLE);
                    setUserState(targetId, USER_STATE.AVAILABLE);
                }

                if (targetWs && targetWs.readyState === 1) {
                    targetWs.send(JSON.stringify({
                        type: 'call-refused',
                        callId: callId || null,
                        from: refuserId
                    }));
                }

                return;
            }

            // =================================================
            // 8. ANNULATION D'APPEL (avant réponse, par l'appelant)
            // =================================================
            if (type === 'cancel-call') {

                const targetId = data.targetId;
                const cancellerId = ws.userId || data.from;
                const callId   = data.callId;

                if (!isValidString(targetId, 64)) return;

                console.log(`🚫 ANNULATION D'APPEL`);
                console.log(`   👤 ${cancellerId} → ${targetId}`);
                if (callId) console.log(`   🆔 Call ID : ${callId}`);

                removePendingCall(targetId);
                endCallSession(callId, 'cancelled');
                if (!callId) {
                    setUserState(cancellerId, USER_STATE.AVAILABLE);
                    setUserState(targetId, USER_STATE.AVAILABLE);
                }

                const targetWs = users.get(targetId);
                if (targetWs && targetWs.readyState === 1) {
                    targetWs.send(JSON.stringify({
                        type: 'cancel-call',
                        callId: callId || null,
                        from: cancellerId
                    }));
                }

                return;
            }

            // =================================================
            // 9. RACCROCHAGE / FIN D'APPEL
            // =================================================
            if (type === 'hang-up' || type === 'call-ended' || type === 'end-call' || type === 'call-interrupted') {

                const targetId = data.targetId;
                const hangupId = ws.userId || data.from;
                const callId   = data.callId;
                const eventType = type === 'call-interrupted' ? 'call-interrupted' : 'hang-up';

                if (!isValidString(targetId, 64)) return;

                console.log(`📴 FIN D'APPEL (${eventType})`);
                console.log(`   👤 ${hangupId} → ${targetId}`);
                if (callId) console.log(`   🆔 Call ID : ${callId}`);

                if (callId && activeCalls.has(callId)) {
                    const call = activeCalls.get(callId);
                    if (call.callerId !== hangupId && call.targetId !== hangupId && call.calleeId !== hangupId) {
                        console.warn(`⚠️  Tentative de raccrochage par ${hangupId} sur un appel qui ne lui appartient pas`);
                        return;
                    }
                }

                endCallSession(callId, eventType);
                if (!callId) {
                    setUserState(hangupId, USER_STATE.AVAILABLE);
                    setUserState(targetId, USER_STATE.AVAILABLE);
                    removePendingCall(targetId);
                    removePendingCall(hangupId);
                }

                sendToUser(targetId, {
                    type: eventType,
                    callId: callId || null,
                    from: hangupId
                });

                return;
            }

            // =================================================
            // 10. APPELS DE GROUPE - AJOUT PARTICIPANT
            // =================================================
            if (type === 'group-call-add-participant') {
                const senderId = ws.userId || data.from;
                const callId = data.callId;
                const targetId = data.targetId;
                const addedBy = data.addedBy || senderId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(targetId, 64) || !isValidString(senderId, 64)) {
                    console.warn('[GROUP CALL ERROR] group-call-add-participant : données invalides');
                    return;
                }

                if (senderId === targetId) {
                    console.warn('[GROUP CALL ERROR] Tentative d\'ajout soi-même');
                    return;
                }

                // Vérifier que l'appel existe
                if (!groupCallExists(callId)) {
                    // Si l'appel n'existe pas encore, le créer (transition 1-à-1 vers groupe)
                    console.log(`[GROUP CALL] Création automatique de groupe pour ${callId}`);
                    createGroupCall(callId, senderId);
                }

                // Vérifier que l'expéditeur est participant
                if (!isParticipantInGroupCall(callId, senderId)) {
                    console.warn(`[GROUP CALL ERROR] ${senderId} n'est pas participant de ${callId}`);
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Vous n\'êtes pas participant de cet appel'
                    }));
                    return;
                }

                // Vérifier que la cible n'est pas déjà participant
                if (isParticipantInGroupCall(callId, targetId)) {
                    console.warn(`[GROUP CALL] ${targetId} est déjà participant de ${callId}`);
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Cet utilisateur est déjà dans l\'appel'
                    }));
                    return;
                }

                // Vérifier si la cible est occupée (dans un autre appel 1-à-1)
                if (isUserBusy(targetId) && !isParticipantInGroupCall(callId, targetId)) {
                    console.log(`[GROUP CALL] ${targetId} est occupé dans un autre appel`);
                    ws.send(JSON.stringify({
                        type: 'user-busy',
                        callId: callId,
                        targetId: targetId,
                        message: 'Le correspondant est déjà en communication.'
                    }));
                    return;
                }

                console.log(`[GROUP CALL] Ajout participant demandé : ${targetId} → ${callId} par ${addedBy}`);

                // Essayer d'envoyer via WebSocket si connecté
                const targetWs = users.get(targetId);
                const targetOnline = !!(targetWs && targetWs.readyState === 1);

                if (targetOnline) {
                    // Utilisateur en ligne - envoi direct WebSocket
                    targetWs.send(JSON.stringify({
                        type: 'group-incoming-call',
                        callId: callId,
                        callerId: senderId,
                        addedBy: addedBy,
                        targetId: targetId,
                        callType: 'audio'
                    }));

                    ws.send(JSON.stringify({
                        type: 'group-call-participant-invited',
                        callId: callId,
                        targetId: targetId,
                        message: 'Invitation envoyée'
                    }));

                    console.log(`[GROUP CALL] Invitation WebSocket envoyée à ${targetId}`);
                } else {
                    // Utilisateur hors ligne - envoi FCM
                    const notificationSent = await sendGroupCallInvitationNotification(
                        targetId,
                        callId,
                        senderId,
                        addedBy
                    );

                    if (notificationSent) {
                        ws.send(JSON.stringify({
                            type: 'group-call-participant-invited',
                            callId: callId,
                            targetId: targetId,
                            message: 'Notification envoyée'
                        }));
                        console.log(`[GROUP CALL] Notification FCM envoyée à ${targetId}`);
                    } else {
                        ws.send(JSON.stringify({
                            type: 'user-offline',
                            callId: callId,
                            targetId: targetId,
                            message: 'Le correspondant est hors ligne et la notification a échoué.'
                        }));
                        console.warn(`[GROUP CALL] Échec notification FCM pour ${targetId}`);
                    }
                }

                return;
            }

            // =================================================
            // 11. APPELS DE GROUPE - REJOINDRE
            // =================================================
            if (type === 'group-call-join') {
                const userId = ws.userId || data.from;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(userId, 64)) {
                    console.warn('[GROUP CALL ERROR] group-call-join : données invalides');
                    return;
                }

                // Si l'appel n'existe pas comme appel de groupe, vérifier s'il existe comme appel 1-à-1
                // et le transformer en appel de groupe
                if (!groupCallExists(callId)) {
                    if (activeCalls.has(callId)) {
                        // Transformer l'appel 1-à-1 en appel de groupe
                        console.log(`[GROUP CALL] Transformation 1-à-1 → groupe : ${callId}`);
                        
                        const callData = activeCalls.get(callId);
                        const participantsList = [callData.callerId, callData.targetId];
                        
                        // Créer l'appel de groupe avec les participants existants
                        createGroupCall(callId, callData.callerId);
                        
                        // Ajouter les deux participants
                        addParticipantToGroupCall(callId, callData.callerId);
                        addParticipantToGroupCall(callId, callData.targetId);
                        
                        // Supprimer l'appel 1-à-1
                        activeCalls.delete(callId);
                        
                        // Informer l'autre participant que l'appel est devenu un groupe
                        const otherParticipant = userId === callData.callerId ? callData.targetId : callData.callerId;
                        const otherWs = users.get(otherParticipant);
                        if (otherWs && otherWs.readyState === WebSocket.OPEN) {
                            otherWs.send(JSON.stringify({
                                type: 'group-call-transformed',
                                callId: callId,
                                transformedBy: userId
                            }));
                            console.log(`[GROUP CALL] ${otherParticipant} informé de la transformation`);
                        }
                        
                        console.log(`[GROUP CALL] Appel transformé en groupe avec ${participantsList.length} participants`);
                    } else {
                        console.warn(`[GROUP CALL ERROR] Tentative de rejoindre un appel inexistant : ${callId}`);
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Cet appel n\'existe pas'
                        }));
                        return;
                    }
                }

                // Ajouter le participant
                const added = addParticipantToGroupCall(callId, userId);
                if (!added) {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Impossible de rejoindre cet appel'
                    }));
                    return;
                }

                setUserState(userId, USER_STATE.IN_CALL);

                // Récupérer la liste des participants
                const participants = groupCallParticipants.get(callId);
                const participantsArray = Array.from(participants);

                console.log(`[GROUP CALL] Participant rejoint : ${userId} → ${callId}`);

                // Envoyer l'état au nouveau participant
                ws.send(JSON.stringify({
                    type: 'group-call-state',
                    callId: callId,
                    participants: participantsArray
                }));

                // Notifier tous les autres participants
                participants.forEach(participantId => {
                    if (participantId !== userId) {
                        sendToUser(participantId, {
                            type: 'group-call-participant-joined',
                            callId: callId,
                            participantId: userId
                        });
                    }
                });

                return;
            }

            // =================================================
            // 12. APPELS DE GROUPE - PARTICIPANT QUITTE
            // =================================================
            if (type === 'group-call-participant-left') {
                const userId = ws.userId || data.from;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(userId, 64)) {
                    console.warn('[GROUP CALL ERROR] group-call-participant-left : données invalides');
                    return;
                }

                // Vérifier que l'appel existe et que l'utilisateur est participant
                if (!isParticipantInGroupCall(callId, userId)) {
                    console.warn(`[GROUP CALL ERROR] ${userId} n'est pas participant de ${callId}`);
                    return;
                }

                console.log(`[GROUP CALL] Participant quitte : ${userId} ← ${callId}`);

                // Retirer le participant
                removeParticipantFromGroupCall(callId, userId);

                // Mettre à jour l'état de l'utilisateur
                setUserState(userId, USER_STATE.AVAILABLE);

                // Notifier les autres participants
                const participants = groupCallParticipants.get(callId);
                if (participants) {
                    participants.forEach(participantId => {
                        if (participantId !== userId) {
                            sendToUser(participantId, {
                                type: 'group-call-participant-left',
                                callId: callId,
                                participantId: userId
                            });
                        }
                    });
                }

                // Si l'appel n'existe plus (moins de 2 participants), notifier le dernier
                if (!groupCallExists(callId)) {
                    ws.send(JSON.stringify({
                        type: 'group-call-ended',
                        callId: callId,
                        message: 'L\'appel a terminé'
                    }));
                }

                return;
            }

            // =================================================
            // 13. APPELS DE GROUPE - OFFER WEBRTC
            // =================================================
            if (type === 'group-call-offer') {
                const senderId = ws.userId || data.from;
                const targetId = data.targetId;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(targetId, 64) || !isValidString(senderId, 64)) {
                    console.warn('[GROUP WEBRTC ERROR] group-call-offer : données invalides');
                    return;
                }

                // Vérifier que les deux sont participants du même appel
                if (!isParticipantInGroupCall(callId, senderId) || !isParticipantInGroupCall(callId, targetId)) {
                    console.warn('[GROUP WEBRTC ERROR] Offre entre utilisateurs non participants du même appel');
                    return;
                }

                console.log(`[GROUP WEBRTC] Offer envoyé : ${senderId} → ${targetId} (${callId})`);

                sendToUser(targetId, {
                    type: 'group-call-offer',
                    callId: callId,
                    from: senderId,
                    offer: data.offer
                });

                return;
            }

            // =================================================
            // 14. APPELS DE GROUPE - ANSWER WEBRTC
            // =================================================
            if (type === 'group-call-answer') {
                const senderId = ws.userId || data.from;
                const targetId = data.targetId;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(targetId, 64) || !isValidString(senderId, 64)) {
                    console.warn('[GROUP WEBRTC ERROR] group-call-answer : données invalides');
                    return;
                }

                // Vérifier que les deux sont participants du même appel
                if (!isParticipantInGroupCall(callId, senderId) || !isParticipantInGroupCall(callId, targetId)) {
                    console.warn('[GROUP WEBRTC ERROR] Answer entre utilisateurs non participants du même appel');
                    return;
                }

                console.log(`[GROUP WEBRTC] Answer reçu : ${senderId} → ${targetId} (${callId})`);

                sendToUser(targetId, {
                    type: 'group-call-answer',
                    callId: callId,
                    from: senderId,
                    answer: data.answer
                });

                return;
            }

            // =================================================
            // 15. APPELS DE GROUPE - ICE CANDIDATE
            // =================================================
            if (type === 'group-call-ice-candidate') {
                const senderId = ws.userId || data.from;
                const targetId = data.targetId;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(targetId, 64) || !isValidString(senderId, 64)) {
                    console.warn('[GROUP WEBRTC ERROR] group-call-ice-candidate : données invalides');
                    return;
                }

                // Vérifier que les deux sont participants du même appel
                if (!isParticipantInGroupCall(callId, senderId) || !isParticipantInGroupCall(callId, targetId)) {
                    console.warn('[GROUP WEBRTC ERROR] ICE candidate entre utilisateurs non participants du même appel');
                    return;
                }

                console.log(`[GROUP WEBRTC] ICE candidate : ${senderId} → ${targetId} (${callId})`);

                sendToUser(targetId, {
                    type: 'group-call-ice-candidate',
                    callId: callId,
                    from: senderId,
                    candidate: data.candidate
                });

                return;
            }

            // =================================================
            // 16. APPELS DE GROUPE - TERMINER
            // =================================================
            if (type === 'group-call-end') {
                const senderId = ws.userId || data.from;
                const callId = data.callId;

                // Validation
                if (!isValidString(callId, 64) || !isValidString(senderId, 64)) {
                    console.warn('[GROUP CALL ERROR] group-call-end : données invalides');
                    return;
                }

                // Vérifier que l'appel existe
                if (!groupCallExists(callId)) {
                    console.warn(`[GROUP CALL ERROR] Tentative de terminer un appel inexistant : ${callId}`);
                    return;
                }

                console.log(`[GROUP CALL] Terminaison demandée par ${senderId} : ${callId}`);

                // Notifier tous les participants
                const participants = groupCallParticipants.get(callId);
                if (participants) {
                    participants.forEach(participantId => {
                        sendToUser(participantId, {
                            type: 'group-call-ended',
                            callId: callId,
                            endedBy: senderId
                        });
                        setUserState(participantId, USER_STATE.AVAILABLE);
                    });
                }

                // Supprimer l'appel
                endGroupCall(callId);

                return;
            }

            if (type === 'call-connected') {
                const callId = data.callId;
                if (callId && activeCalls.has(callId)) {
                    activeCalls.get(callId).status = 'active';
                }
                if (ws.userId) setUserState(ws.userId, USER_STATE.IN_CALL);
                return;
            }

            console.log(`ℹ️  Type de message non géré : ${type}`);

        } catch (error) {
            console.error('❌ Erreur de traitement du message :', error);
        }
    });

    // ---------------------------------------------------------
    // Déconnexion
    // ---------------------------------------------------------
    ws.on('close', () => {

        if (ws.userId) {
            if (users.get(ws.userId) === ws) {
                users.delete(ws.userId);
                const existingCall = findCallForUser(ws.userId);
                setUserState(ws.userId, USER_STATE.OFFLINE);
                console.log(`🔌 Utilisateur déconnecté : ${ws.userId} (Token FCM conservé)`);

                if (existingCall) {
                    const otherId = existingCall.callerId === ws.userId
                        ? (existingCall.calleeId || existingCall.targetId)
                        : existingCall.callerId;
                    sendToUser(otherId, {
                        type: 'call-interrupted',
                        callId: existingCall.callId,
                        from: ws.userId,
                        message: 'Le correspondant a perdu la connexion.'
                    });
                    endCallSession(existingCall.callId, 'ws-disconnect');
                }
            } else {
                console.log(`🔌 Ancienne connexion fermée pour ${ws.userId} (nouvelle connexion active)`);
            }
        }

    });

    // ---------------------------------------------------------
    // Erreur WebSocket
    // ---------------------------------------------------------
    ws.on('error', (error) => {
        console.error(`❌ Erreur WebSocket pour ${ws.userId || 'inconnu'} :`, error.message);
    });

});

// =========================================================
// HEARTBEAT (PING/PONG) POUR DÉTECTER LES CONNEXIONS MORTES
// =========================================================
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`💔 Connexion inactive → terminaison pour ${ws.userId || 'inconnu'}`);
            if (ws.userId && users.get(ws.userId) === ws) {
                const existingCall = findCallForUser(ws.userId);
                users.delete(ws.userId);
                setUserState(ws.userId, USER_STATE.OFFLINE);
                if (existingCall) {
                    const otherId = existingCall.callerId === ws.userId
                        ? (existingCall.calleeId || existingCall.targetId)
                        : existingCall.callerId;
                    sendToUser(otherId, {
                        type: 'call-interrupted',
                        callId: existingCall.callId,
                        from: ws.userId
                    });
                    endCallSession(existingCall.callId, 'heartbeat');
                }
            }
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

// =========================================================
// DÉMARRAGE SERVEUR
// =========================================================

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {

    console.log('====================================================');
    console.log(`🚀 Serveur WebSocket & WebRTC & FCM actif sur le port ${PORT}`);
    console.log('====================================================');
    console.log('📞 Appels WebSocket     : ACTIVÉS');
    console.log('🧊 Signalisation WebRTC : ACTIVÉE');
    console.log('🔥 Firebase FCM         : ' + (firebaseInitialized ? '🟢 ACTIVÉ' : '🔴 NON ACTIVÉ'));
    console.log('⏰ Expiration appels     : ' + (PENDING_CALL_TTL_MS / 1000) + 's');
    console.log('====================================================');

});