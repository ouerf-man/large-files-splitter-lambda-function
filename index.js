console.log("Loading function");
const aws = require("aws-sdk");
const csv = require("csv-parser");
const fastcsv = require("fast-csv");
const archiver = require("archiver");
var Readable = require("stream").Readable;
const s3 = new aws.S3({ apiVersion: "2006-03-01" });
const stream = require("stream");

exports.handler = async (event, context) => {
  //console.log('Received event:', JSON.stringify(event, null, 2));

  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );
  console.log(key);
  const params = {
    Bucket: bucket,
    Key: key,
  };
  try {
    // logic here
    const { Body } = await s3.getObject(params).promise();
    // driver does all the job
    await driver(Body, key);

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

async function fileSplitter(processedJson, initialFileName) {
  const archive = archiver("zip", { zlib: { level: 9 } });
  console.log("Splitting original file...");
  const promises = [];
  let startingPoint = 0;
  let linesWritten = 0;
  const chunkSize = parseInt(processedJson.length / 10);

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
          promises.push(fastcsv.writeToBuffer(jsonChunk, { headers: true }));
          break;
        }
      }
    }
    if (i == numChunks - 1) {
      const used = process.memoryUsage().heapUsed / 1024 / 1024;
      console.log(
        `The script uses approximately ${Math.round(used * 100) / 100} MB`
      );
    }
  }
  archive.pipe(uploadFromStream(s3, initialFileName));
  return Promise.all(promises).then(async (data) => {
    data.map((thisFile, index) => {
      archive.append(thisFile, { name: `file${index}.csv` });
    });
    await archive.finalize();
  });
}

function uploadFromStream(s3,initialFileName) {
  var pass = new stream.PassThrough();

  var params = {Bucket: "large-file-split-output", Key: initialFileName + ".zip", Body: pass};
  s3.upload(params, function(err, data) {
    console.log(err, data);
  });

  return pass;
}

async function driver(buffer, key) {
  console.log("**** FILE SPLITTER ****");

  // get JSON Array of all lines in original file
  const fileLines = await createList(buffer);
  // split into multiple smaller files with original list of lines
  await fileSplitter(fileLines, key);

  console.log("**** FILE SPLITTER COMPLETE ****");
}
