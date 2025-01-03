const fs = require('fs');
const JSONStream = require('JSONStream');
const through2 = require('through2');
const WebSocket = require('ws');

// Create a WebSocket server
const wss = new WebSocket.Server({ port: 8081 });

wss.on('connection', (ws) => {
    console.log('Client connected');

    // A transform stream for validation and transformation
    const transformStream = through2.obj(function (data, _, callback) {
        // Example validation: Ensure 'id' and 'value' are present
        if (typeof data.id !== 'string' || typeof data.value !== 'number') {
            return callback(new Error('Invalid data')); // Skip invalid entries
        }
        const transformedData = {
            uniqueId: data.id,
            numericValue: data.value * 2 // Some arbitrary transformation
        };
        callback(null, transformedData);
    });

    // Transform and handle JSON data streams
    ws.on('message', (rawData) => {
        const stream = new Stream.PassThrough();

        // This is important to fix encoding issue for json stream
        // replace utf8_decode
        if (Buffer.isBuffer(rawData)) {
            stream.push(rawData.toString('utf-8'));
        } else {
            stream.push(rawData);
        }

        stream.end();

        stream
            .pipe(JSONStream.parse('*'))
            .pipe(transformStream)
            .on('data', (processedData) => {
                console.log('Processed data from socket:', processedData);
                // Here, you can forward the processed data to other microservices.
            })
            .on('error', (err) => {
                console.error('Error processing data:', err.message);
            });
    });

    ws.on('error', (error) => {
        console.error('WebSocket Error:', error);
    });

    ws.on('close', () => {
        console.log('Client disconnected');
    });
});

console.log('WebSocket server is listening on port 8081');
