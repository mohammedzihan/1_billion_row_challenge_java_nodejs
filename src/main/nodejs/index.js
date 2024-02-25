import * as os from "node:os";
import * as fs from "node:fs";
import * as workerThreads from "worker_threads";

// const MAX_LINE_LENGTH = 100 + 1 + 4 + 1;
const CHAR_SEMICOLON = ";".charCodeAt(0);
const CHAR_NEWLINE = "\n".charCodeAt(0);
const TOKEN_STATION_NAME = 0;
const TOKEN_TEMPERATURE = 1;

const CHAR_MINUS = "-".charCodeAt(0);

const debug = process.env.DEBUG ? console.error : () => {};

const asyncIteratorFromReadable = (readable) => ({
  [Symbol.asyncIterator]: async function* () {
    for await (const chunk of readable) {
      yield chunk;
    }
  },
});

if (workerThreads.isMainThread) {
  const fileName = process.argv[2];
  // const file = fs.createReadStream(fileName, {
  //   highWaterMark: MAX_LINE_LENGTH,
  // });

  const threadsCount = os.cpus().length;

  const chunkSize = Math.ceil(fs.statSync(fileName).size / threadsCount);

  const chunkOffsets = Array.from(
    { length: threadsCount },
    (_, i) => i * chunkSize
  );

  const compiledResults = new Map();

  let stoppedWorkers = 0;

  for (let i = 0; i < chunkOffsets.length; i++) {
    const start = chunkOffsets[i];
    const end = i === chunkOffsets.length - 1 ? undefined : chunkOffsets[i + 1];
    const worker = new workerThreads.Worker("./src/main/nodejs/index.js", {
      workerData: {
        fileName,
        start,
        end,
      },
    });

    worker.on("message", (message) => {
      for (let [key, value] of message.entries()) {
        const existing = compiledResults.get(key);
        if (existing) {
          existing.min = Math.min(existing.min, value.min);
          existing.max = Math.max(existing.max, value.max);
          existing.sum += value.sum;
          existing.count += value.count;
        } else {
          compiledResults.set(key, value);
        }
      }
    });

    worker.on("error", (err) => console.error(err));

    worker.on("exit", (code) => {
      if (code !== 0) {
        console.error(`Worker stopped with exit code ${code}`);
      } else {
        debug("Worker stopped");
      }

      stoppedWorkers++;

      if (stoppedWorkers === chunkOffsets.length) {
        printCompiledResults(compiledResults);
      }
    });
  }
} else {
  const { fileName, start, end } = workerThreads.workerData;

  if (start >= end) {
    workerThreads.parentPort.postMessage(new Map());
  } else {
    const readStream = fs.createReadStream(fileName, { start, end });
    parseStream(readStream).then((map) => {
      workerThreads.parentPort.postMessage(map);
    });
  }
}

// Function to decode UTF-8 bytes into a string
function utf8Decode(bytes, length) {
  let result = "";
  let i = 0;
  while (i < length) {
    const byte = bytes[i++];
    if (byte < 0x80) {
      result += String.fromCharCode(byte);
    } else if (byte >= 0xc0 && byte < 0xe0) {
      const byte2 = bytes[i++];
      result += String.fromCharCode(((byte & 0x1f) << 6) | (byte2 & 0x3f));
    } else if (byte >= 0xe0 && byte < 0xf0) {
      const byte2 = bytes[i++];
      const byte3 = bytes[i++];
      result += String.fromCharCode(
        ((byte & 0x0f) << 12) | ((byte2 & 0x3f) << 6) | (byte3 & 0x3f)
      );
    } else if (byte >= 0xf0 && byte < 0xf8) {
      const byte2 = bytes[i++];
      const byte3 = bytes[i++];
      const byte4 = bytes[i++];
      result += String.fromCharCode(
        ((byte & 0x07) << 18) |
          ((byte2 & 0x3f) << 12) |
          ((byte3 & 0x3f) << 6) |
          (byte4 & 0x3f)
      );
    }
  }
  return result;
}

let readingToken = TOKEN_STATION_NAME;

const stationName = new Uint8Array(100);
let stationNameLen = 0;

const temperature = new Uint8Array(5);
let temperatureLen = 0;

async function parseStream(readStream) {
  const map = new Map();

  for await (const chunk of asyncIteratorFromReadable(readStream)) {
    for (let i = 0; i < chunk.length; i++) {
      const char = chunk[i];

      if (char === CHAR_SEMICOLON) {
        readingToken = TOKEN_TEMPERATURE;
      } else if (char === CHAR_NEWLINE) {
        const stationNameStr = utf8Decode(stationName, stationNameLen);
        const temperatureFloat = parseFloatBufferIntoInt(
          temperature,
          temperatureLen
        );

        let existing = map.get(stationNameStr);

        if (!existing) {
          existing = {
            min: temperatureFloat,
            max: temperatureFloat,
            sum: temperatureFloat,
            count: 1,
          };
          map.set(stationNameStr, existing);
        } else {
          existing.min = Math.min(existing.min, temperatureFloat);
          existing.max = Math.max(existing.max, temperatureFloat);
          existing.sum += temperatureFloat;
          existing.count++;
        }

        readingToken = TOKEN_STATION_NAME;
        stationNameLen = 0;
        temperatureLen = 0;
      } else if (readingToken === TOKEN_STATION_NAME) {
        stationName[stationNameLen++] = char;
      } else {
        temperature[temperatureLen++] = char;
      }
    }
  }

  return map;
}

function printCompiledResults(compiledResults) {
  const sortedStations = Array.from(compiledResults.keys()).sort();

  process.stdout.write("{");
  for (let i = 0; i < sortedStations.length; i++) {
    if (i > 0) {
      process.stdout.write(", ");
    }
    const data = compiledResults.get(sortedStations[i]);
    process.stdout.write(sortedStations[i]);
    process.stdout.write("=");
    process.stdout.write(
      `${round(data.min / 10)}/${round(data.sum / 10 / data.count)}/${round(
        data.max / 10
      )}`
    );
  }
  process.stdout.write("}\n");
  // console.log(sortedStations.length, "sortedStations LENGTH");
}

function round(num) {
  const fixed = Math.round(10 * num) / 10;
  return fixed.toFixed(1);
}

function parseFloatBufferIntoInt(b, length) {
  if (b[0] === CHAR_MINUS) {
    switch (length) {
      case 4:
        return -(parseOneDigit(b[1]) * 10 + parseOneDigit(b[3]));
      case 5:
        return -(
          parseOneDigit(b[1]) * 100 +
          parseOneDigit(b[2]) * 10 +
          parseOneDigit(b[4])
        );
    }
  } else {
    switch (length) {
      case 3:
        return parseOneDigit(b[0]) * 10 + parseOneDigit(b[2]);
      case 4:
        return (
          parseOneDigit(b[0]) * 100 +
          parseOneDigit(b[1]) * 10 +
          parseOneDigit(b[3])
        );
    }
  }
}

function parseOneDigit(char) {
  return char - 0x30;
}
