const fs = require('fs'); // Let's say we read from a file for demo purpose
const JSONStream = require('JSONStream');
const through2 = require('through2');

// Example of incoming stream (simulate by reading from a file)
const stream = fs.createReadStream('largeData.json');

// Create a transform stream to validate and transform data
const transformStream = through2.obj(function (data, _, callback) {
    // Example validation: Ensure 'id' and 'value' are present
    if (typeof data.id !== 'string' || typeof data.value !== 'number') {
        return callback(new Error('Invalid data')); // skip invalid entries
    }

    // Example transformation: Convert to a new format
    const transformedData = {
        uniqueId: data.id,
        numericValue: data.value * 2 // some arbitrary transformation
    };

    callback(null, transformedData);
});

// Listen for errors on transform stream
transformStream.on('error', (err) => {
    console.error('Error processing data:', err.message);
});

// Process JSON as it arrives
stream
    .pipe(JSONStream.parse('*')) // Adjust the parsing for your structure
    .pipe(transformStream)
    .on('data', (data) => {
        // Here you would forward the transformed data to your microservices
        console.log('Processed data:', data);
    })
    .on('end', () => {
        console.log('Stream processing complete.');
    })
    .on('error', (err) => {
        console.error('Stream error:', err);
    });
