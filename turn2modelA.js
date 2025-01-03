const WebSocket = require('ws');
const JSONStream = require('JSONStream');
const through2 = require('through2');

// WebSocket server setup
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', (ws) => {
    console.log('Client connected');

    const transformStream = through2.obj(function (data, _, callback) {
        // Example validation: Ensure 'id' and 'value' are present
        if (typeof data.id !== 'string' || typeof data.value !== 'number') {
            console.error('Invalid data received:', data);
            return callback(); // skip invalid entries
        }

        // Example transformation
        const transformedData = {
            uniqueId: data.id,
            numericValue: data.value * 2 // example transformation
        };

        callback(null, transformedData);
    });

    // Handling incoming messages
    ws.on('message', (message) => {
        // Parse incoming message as a JSON array
        const dataStream = JSONStream.parse('*');

        // Create a stream for the incoming message
        const readStream = require('stream').Readable.from([message]);

        // Pipe streams together
        readStream
            .pipe(dataStream)
            .pipe(transformStream)
            .on('data', (data) => {
                // Here you forward the transformed data
                console.log('Processed and transformed data:', data);
                // You can also send it back to a client or forward it to proper microservices
                // ws.send(JSON.stringify(data)); // Uncomment to send back to client
            })
            .on('error', (err) => {
                console.error('Error processing data from client:', err);
            });
    });

    ws.on('close', () => {
        console.log('Client disconnected');
    });
});

console.log('WebSocket server started at ws://localhost:8080');
