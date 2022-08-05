const fs = require("fs");
const csv = require("csv-parser");
const fastcsv = require("fast-csv");
var JSZip = require("jszip");
var glob = require("glob");
const TEMP_PATH = "./temp/";

async function createList() {
  const processedJson = [];
  const csvToJsonParsing = new Promise(function (resolve, reject) {
    fs.createReadStream("test.csv")
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

async function fileSplitter(processedJson) {
  console.log("Splitting original file...");
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
          // write stream for writing files to disk
          const writeStream = await fs.createWriteStream(
            TEMP_PATH + "file-" + i + ".csv"
          );
          // file chunk to be written
          const generateCsv = fastcsv.write(jsonChunk, { headers: true });
          generateCsv.pipe(writeStream);
          const jsonToCsv = new Promise(function (resolve, reject) {
            generateCsv
              .on("error", function (err) {
                console.log(err);
                reject(err);
              })
              .on("end", function () {
                resolve();
              });
          });
          await jsonToCsv;

          writeStream
            .on("close", async (data) => {
              if (processedJson.length - j < chunkSize) {
                // zip and delete files and upload to s3
                await zipFile();
              }
            })
            .on("error", (err) => {
              console.log(err);
            });
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
  console.log("File split complete ...");
}

function cleanUpTemp() {
  let regex = /^file/;
  fs.readdirSync(TEMP_PATH)
    .filter((f) => regex.test(f))
    .map((f) => fs.unlinkSync(TEMP_PATH + f));
  console.log('CLEANUP SUCCESSFUL');
}

async function zipFile() {
  const zip = new JSZip();
  
  glob(TEMP_PATH + "file*", async function (err, files) {
    if (err) {
      console.log(err);
    }


    for (const file of files) {
        const fileData = fs.readFileSync(file);
        zip.file(file.split('/')[file.split('/').length -1], fileData);
    }
    var ws = await fs.createWriteStream('./output/final.zip')
    zip.generateNodeStream({ type: 'nodebuffer', streamFiles: true })
        .pipe(ws)
        .on('finish', function () {
            console.log("final.zip written.");
            cleanUpTemp()
        });

  });
}

async function driver() {
  try {
    console.log("**** FILE SPLITTER ****");

    // get JSON Array of all lines in original file
    const fileLines = await createList();

    // split into multiple smaller files with original list of lines
    await fileSplitter(fileLines);

    console.log("**** FILE SPLITTER COMPLETE ****");
  } catch (e) {
    console.log(e);
  }
}

driver();
