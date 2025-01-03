const { Transform } = require('stream');

class JSONProcessor extends Transform {
    constructor(options = {}) {
        super(options);
        this.currentBuffer = '';
    }

    _transform(chunk, encoding, callback) {
        // Concatenate the current chunk with the existing buffer
        this.currentBuffer += chunk.toString(encoding);

        // Check if the buffer contains a complete JSON object
        while (true) {
            const index = this.currentBuffer.indexOf('}');
            if (index === -1) {
                // No complete JSON object found, wait for the next chunk
                break;
            }

            const jsonString = this.currentBuffer.slice(0, index + 1);
            this.currentBuffer = this.currentBuffer.slice(index + 1);

            try {
                // Parse the JSON string
                const data = JSON.parse(jsonString);

                // Validate and transform the data
                const validatedData = this.validateAndTransform(data);

                // Push the transformed data to the next stream
                this.push(JSON.stringify(validatedData) + '\n');
            } catch (error) {
                // Handle parsing errors
                console.error('Error parsing JSON:', error.message);
                this.emit('error', error);
                return callback();
            }
        }

        callback();
    }

    validateAndTransform(data) {
        // Add your validation and transformation logic here
        // For example:
        if (!data.hasOwnProperty('id') || !data.hasOwnProperty('name')) {
            throw new Error('Invalid data: Missing "id" or "name" property');
        }

        return {
            ...data,
            processedAt: new Date().toISOString(),
        };
    }

    _flush(callback) {
        // Handle any remaining data in the buffer
        if (this.currentBuffer.trim().length > 0) {
            try {
                const data = JSON.parse(this.currentBuffer);
                const validatedData = this.validateAndTransform(data);
                this.push(JSON.stringify(validatedData) + '\n');
            } catch (error) {
                console.error('Error parsing JSON:', error.message);
                this.emit('error', error);
            }
        }
        callback();
    }
}

// Example usage:
const readStream = require('fs').createReadStream('largeData.json');
const writeStream = require('fs').createWriteStream('processed_data.json');

const processor = new JSONProcessor();

readStream
    .pipe(processor)
    .pipe(writeStream)
    .on('error', (error) => {
        console.error('Error:', error.message);
    })
    .on('finish', () => {
        console.log('Data processing completed.');
    });
