const fs = require('fs');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');
const JSZip = require('jszip');
var zlib = require('zlib');
var stream = require('stream').pipeline;


async function createList() {
    const processedJson = [];
    const csvToJsonParsing = new Promise(function (resolve, reject) {
        fs.createReadStream('test.csv')
            .pipe(csv({ separator: ',' }))
            .on('data', (data) => {
                processedJson.push(data);
            })
            .on('end', () => {
                resolve();
            });
    });

    await csvToJsonParsing;
    return processedJson;
}

async function fileSplitter(processedJson) {
    const zip = new JSZip();
    console.log('Splitting original file...');
    let startingPoint = 0;
    let linesWritten = 0;
    const chunkSize = parseInt(processedJson.length / 10);
    console.log(chunkSize)
    console.log(processedJson.length);

    // this reprenents the number of files the original file will be broken into
    numChunks = Math.ceil(processedJson.length / chunkSize);

    for (let i = 0; i < numChunks; i++) {
        if (linesWritten >= processedJson.length) {
            break;
        }

        // the data that will get written into the current smaller file
        const jsonChunk = [];

        for (let j = startingPoint; j < startingPoint + chunkSize; j++) {
            jsonChunk.push(processedJson[j]);
            if (j < processedJson.length) {
                linesWritten++;
                // if we've reached the chunk increment, increase the starting point to the next increment
                if (j == startingPoint + chunkSize - 1) {
                    startingPoint = j + 1;

                    // file chunk to be written
                    var gz = zlib.createGzip();
                    const writeStream = await fs.createWriteStream('./output/file-' + i + '.txt.gz');
                    gz.pipe(writeStream);

                    gz.on('error', function (err) {
                        console.log("wtf error", err);
                    });

                    gz.on('finish', function () {
                        console.log("write stream is done");
                    });
                    gz.write(jsonChunk.toString())
                    gz.end()

                    break;
                }
            }
        }
    }
    console.log('File split complete ...');
}

async function driver() {
    console.log('**** FILE SPLITTER ****');

    // get JSON Array of all lines in original file
    const fileLines = await createList();

    // split into multiple smaller files with original list of lines
    await fileSplitter(fileLines);

    console.log('**** FILE SPLITTER COMPLETE ****');
}

driver();