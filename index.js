console.log("Loading function");
const fs = require("fs");
const aws = require("aws-sdk");
const csv = require("csv-parser");

const fastcsv = require("fast-csv");
var zlib = require("zlib");
var pipeline = require("stream").pipeline;
var Readable = require("stream").Readable;

const s3 = new aws.S3({ apiVersion: "2006-03-01" });

const s3Stream = require("s3-upload-stream")(s3);

exports.handler = async (event, context) => {
  //console.log('Received event:', JSON.stringify(event, null, 2));

  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );
  const params = {
    Bucket: bucket,
    Key: key,
  };
  try {
    // logic here
    const { Body } = await s3.getObject(params).promise();
    // driver does all the job
    await driver(Body,key);

    return 1;
  } catch (err) {
    console.log(err);
    const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
    console.log(message);
    throw new Error(message);
  }
};

async function createList(buffer) {
  const processedJson = [];
  const csvToJsonParsing = new Promise(function (resolve, reject) {
    const bufferInstance = new Buffer.from(buffer, "utf-8");
    const readable = new Readable();
    readable._read = () => {}; // _read is required but you can noop it
    readable.push(buffer);
    readable.push(null);
    readable // this is the buffer coming from the s3 event
      .pipe(csv({ separator: "," }))
      .on("data", (data) => {
        processedJson.push(data);
      })
      .on("end", () => {
        resolve();
      });
  });

  await csvToJsonParsing;
  return processedJson;
}

async function fileSplitter(processedJson,initialFileName) {
  console.log("Splitting original file...");
  let startingPoint = 0;
  let linesWritten = 0;
  const chunkSize = parseInt(processedJson.length / 10);
  console.log(chunkSize);
  console.log(processedJson.length);

  // this reprenents the number of files the original file will be broken into
  const numChunks = Math.ceil(processedJson.length / chunkSize);

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
          // initiating zlib
          const gzip = zlib.createGzip();
          // file chunk to be written
          //const writeStream = await fs.createWriteStream('./output/file-' + i + '.gz');
          const writeStream = s3Stream.upload({
            Bucket: "large-file-split-output",
            Key: initialFileName+"/file-" + i + ".csv.gz",
          });
          // Handle errors.
          writeStream.on("error", function (error) {
            console.log(error);
          });

          /* Handle progress. Example details object:
                    { ETag: '"f9ef956c83756a80ad62f54ae5e7d34b"',
                    PartNumber: 5,
                    receivedSize: 29671068,
                    uploadedSize: 29671068 }
                */
          writeStream.on("part", function (details) {
            console.log("part");
          });

          /* Handle upload completion. Example details object:
                    { Location: 'https://bucketName.s3.amazonaws.com/filename.ext',
                    Bucket: 'bucketName',
                    Key: 'filename.ext',
                    ETag: '"bf2acbedf84207d696c8da7dbb205b9f-5"' }
                */
          writeStream.on("uploaded", function (details) {
            console.log("part uploaded");
          });
          const options = { headers: true };
          const generateCsv = fastcsv.write(jsonChunk, options);
          //generateCsv.pipe(writeStream);
          pipeline(generateCsv, gzip, writeStream, (err) => {
            if (err) {
              console.error(
                "An Error occured in the Compression Processor:",
                err
              );
            }
          });
          const jsonToCsv = new Promise(function (resolve, reject) {
            generateCsv
              .on("error", function (err) {
                reject(err);
              })
              .on("end", function () {
                resolve();
              });
          });
          await jsonToCsv;

          break;
        }
      }
    }
  }
  console.log("File split complete ...");
}

async function driver(buffer,key) {
  console.log("**** FILE SPLITTER ****");

  // get JSON Array of all lines in original file
  const fileLines = await createList(buffer);
  // split into multiple smaller files with original list of lines
  await fileSplitter(fileLines,key);

  console.log("**** FILE SPLITTER COMPLETE ****");
}
