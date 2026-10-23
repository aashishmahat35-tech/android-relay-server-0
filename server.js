const http = require('http');
const WebSocket = require('ws');

// Configuration
const PORT = process.env.PORT || 8080;

// Create standard HTTP server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WebSocket Relay Server is running.\n');
});

// Initialize WebSocket server
const wss = new WebSocket.Server({ server });

// In-memory room storage
// Structure: Map<room_id, { controller: WebSocket, target: WebSocket }>
const rooms = new Map();

// Helper to send formatted JSON messages
const sendMessage = (ws, action, payload = {}, status = 'success') => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action, payload, status }));
    }
};

// Helper to clean up disconnected sockets
const cleanupRoom = (roomId, role) => {
    const room = rooms.get(roomId);
    if (!room) return;

    if (role === 'controller') {
        room.controller = null;
        if (room.target) {
            sendMessage(room.target, 'partner_disconnected', { message: 'Controller disconnected' });
        }
    } else if (role === 'target') {
        room.target = null;
        if (room.controller) {
            sendMessage(room.controller, 'partner_disconnected', { message: 'Target disconnected' });
        }
    }

    // If both are gone, delete the room
    if (!room.controller && !room.target) {
        rooms.delete(roomId);
        console.log(`Room [${roomId}] deleted.`);
    }
};

wss.on('connection', (ws) => {
    console.log('New client connected');
    
    // Attach room tracking variables to the socket instance
    ws.roomId = null;
    ws.role = null;
    ws.isAlive = true;

    // Heartbeat (Pong)
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const { action, payload } = data;

            // 1. Handle Registration
            if (action === 'register') {
                const { role, room_id } = payload;

                if (!room_id || !['controller', 'target'].includes(role)) {
                    return sendMessage(ws, 'register', { message: 'Invalid role or room_id' }, 'error');
                }

                ws.roomId = room_id;
                ws.role = role;

                if (!rooms.has(room_id)) {
                    rooms.set(room_id, { controller: null, target: null });
                }

                const room = rooms.get(room_id);

                // Check if role is already taken in this room
                if (room[role]) {
                    return sendMessage(ws, 'register', { message: `Role '${role}' already occupied in room ${room_id}` }, 'error');
                }

                // Register the device
                room[role] = ws;
                console.log(`Device registered as [${role}] in room [${room_id}]`);
                sendMessage(ws, 'register', { message: `Registered successfully as ${role}` });

                // Notify both if pairing is complete
                if (room.controller && room.target) {
                    sendMessage(room.controller, 'paired', { message: 'Target is connected' });
                    sendMessage(room.target, 'paired', { message: 'Controller is connected' });
                    console.log(`Room [${room_id}] is now fully paired.`);
                }
                return;
            }

            // 2. Handle Relay (Only if registered)
            if (!ws.roomId || !ws.role) {
                return sendMessage(ws, 'error', { message: 'Device not registered. Send "register" action first.' }, 'error');
            }

            const room = rooms.get(ws.roomId);
            if (!room) {
                return sendMessage(ws, 'error', { message: 'Room not found' }, 'error');
            }

            // Route message to the opposite device
            if (ws.role === 'controller') {
                if (room.target) {
                    // Forward exact message from controller to target
                    sendMessage(room.target, action, payload, data.status || 'success');
                } else {
                    sendMessage(ws, 'error', { message: 'Target offline' }, 'error');
                }
            } else if (ws.role === 'target') {
                if (room.controller) {
                    // Forward exact response from target back to controller
                    sendMessage(room.controller, action, payload, data.status || 'success');
                } else {
                    sendMessage(ws, 'error', { message: 'Controller offline' }, 'error');
                }
            }

        } catch (e) {
            console.error('Error processing message:', e.message);
            sendMessage(ws, 'error', { message: 'Invalid JSON format' }, 'error');
        }
    });

    // Handle Disconnection
    ws.on('close', () => {
        console.log(`Client disconnected: Role [${ws.role}], Room [${ws.roomId}]`);
        if (ws.roomId && ws.role) {
            cleanupRoom(ws.roomId, ws.role);
        }
    });
});

// Heartbeat interval to detect ghost connections (every 30s)
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log('Terminating dead connection');
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

// Start Server
server.listen(PORT, () => {
    console.log(`Server is listening on port ${PORT}`);
});