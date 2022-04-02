const fs = require("fs");
const path = require("path");

// const CACHE_DIR = "cache";
const DATA_DIR = "data";
const textract = require("textract");
const natural = require("natural");
const md5File = require("md5-file");
// const tokenizer = new natural.WordTokenizer();
const tokenizer = new natural.AggressiveTokenizerPt();
const Datastore = require("nedb");

const SUPPORTED_EXTENSIONS = [
    "TXT", "HTML", "HTM", "ATOM", "RSS", "MD", "EPUB", "XML", "XSL", "DOCX", "ODT", "OTT", "XLS", "XLSX", "XLSB", "XLSM", "XLTX", "CSV", "ODS", "OTS", "PPTX", "POTX", "ODP", "OTP", "ODG", "OTG",
    // https://www.npmjs.com/package/textract#extraction-requirements
    // "PDF", "DOC", "RTF", "PNG", "JPG", "GIF", "DXF" 
];

async function loadDatabase(db) {
    return new Promise((resolve, reject) => {
        db.loadDatabase((err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function isFileSupported(filename) {
    return !!SUPPORTED_EXTENSIONS.find(ext => filename.toUpperCase().endsWith(`.${ext}`));
}

function getAllFiles(dirPath, arrayOfFiles) {
    const files = fs.readdirSync(dirPath);

    arrayOfFiles = arrayOfFiles || [];

    files.forEach((file) => {
        if (fs.statSync(path.join(dirPath, file)).isDirectory()) {
            arrayOfFiles = getAllFiles(path.join(dirPath, file), arrayOfFiles);
        } else {
            if (isFileSupported(file)) {
                arrayOfFiles.push(path.join(dirPath, file));
            }
        }
    });

    return arrayOfFiles;
}

async function textractFromFileWithPath(filePath) {
    return new Promise((resolve, reject) => {
        textract.fromFileWithPath(filePath, function (error, text) {
            if (error) {
                reject(error);
            } else {
                resolve(text);
            }
        });
    });
}

async function dbInsert(db, doc) {
    return new Promise((resolve, reject) => {
        db.insert(doc, (err, newDoc) => {
            if (err) {
                reject(err);
            } else {
                resolve(newDoc);
            }
        });
    });
}

async function dbUpdate(db, query, update, options = {}) {
    return new Promise((resolve, reject) => {
        db.update(query, update, options, (err, numReplaced) => {
            if (err) {
                reject(err);
            } else {
                if (numReplaced !== 1) {
                    throw new Error("!== 1");
                }
                resolve(numReplaced);
            }
        });
    });
}

async function dbFind(db, query) {
    return new Promise((resolve, reject) => {
        db.find(query, (err, docs) => {
            if (err) {
                reject(err);
            } else {
                resolve(docs);
            }
        });
    });
}

async function dbRemove(db, query, options = {}) {
    return new Promise((resolve, reject) => {
        db.remove(query, options, (err, numRemoved) => {
            if (err) {
                reject(err);
            } else {
                resolve(numRemoved);
            }
        });
    });
}




async function run() {
    const dirWithPath = process.argv[2];
    let terms = process.argv.slice(3);

    // validate
    if (terms.length === 0) {
        console.error("No terms to find...");
        return;
    }

    // dedup
    terms = [...new Set(terms)];

    // uppercase
    terms = terms.map(t => t.toUpperCase());

    console.log(`Processing: ${dirWithPath}`);

    const md5db = new Datastore(path.join(DATA_DIR, "md5.db"));
    const tokendb = new Datastore(path.join(DATA_DIR, "token.db"));

    console.log("Loading databases...");
    await loadDatabase(md5db);
    await loadDatabase(tokendb);

    // Removing md5 of deleted files
    console.log("Cleaning md5 data...");
    const currentMD5data = await dbFind(md5db, {});
    currentMD5data.forEach(async (cmd) => {
        const fileName = cmd.file;
        if (!fs.existsSync(fileName)) {
            await dbRemove(md5db, { _id: cmd._id });
        }
    });

    // Create datadir if not exists
    if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR);
    }

    // Check if directory was informed
    if (!dirWithPath) {
        console.error("Path must be informed.");
        return;
    }

    // Check if exists
    if (!fs.existsSync(dirWithPath)) {
        console.error("Directory doesn't exists.");
        return;
    }

    // Check if is directory
    if (!fs.lstatSync(dirWithPath).isDirectory()) {
        console.error("Path is not a directory.");
        return;
    }

    console.log("Getting all files...");
    const allFiles = getAllFiles(dirWithPath);

    for (let idxFile = 0; idxFile < allFiles.length; idxFile++) {
        const file = allFiles[idxFile];

        console.log(`Processing:  ${file}`);

        const hash = md5File.sync(file);

        const foundMD5Data = await dbFind(md5db, { file });

        if (foundMD5Data.length > 1) {
            throw new Error(`Duplicate md5 data: ${file}`);
        }

        if (foundMD5Data.length === 1 && foundMD5Data[0].md5 === hash) {
            console.log(`File ${file} is up to date.`);
        }

        if (foundMD5Data.length === 0 || (foundMD5Data.length === 1 && foundMD5Data[0].md5 !== hash)) {
            if (foundMD5Data.length === 0) {
                console.log(`Indexing file ${file}.`);
            }

            if (foundMD5Data.length === 1) {
                console.log(`Updating index of file ${file}.`);
            }

            await updateTokensFromFile(file, tokendb);

            // Atualiza ou insere hash md5
            await dbUpdate(md5db, { file }, { $set: { md5: hash } }, { upsert: true });
        }
    }

    let listFiles;

    for (let idxTerm = 0; idxTerm < terms.length; idxTerm++) {
        const term = terms[idxTerm];
        const resp = await dbFind(tokendb, { token: term });
        if (resp.length > 1) {
            throw new Error(`Duplicate term: ${term}`);
        } else if (resp.length === 1) {
            if (listFiles) {
                listFiles = intersect(listFiles, resp[0].files);
            } else {
                listFiles = resp[0].files;
            }
        }
    }

    console.log("=====================================");
    console.log(`Terms: ${terms}`);
    listFiles.forEach(f => {
        console.log(f);
    });
    console.log("=====================================");
}

function intersect(a, b) {
    const setA = new Set(a);
    const setB = new Set(b);
    const intersection = new Set([...setA].filter(x => setB.has(x)));
    return Array.from(intersection);
}

async function updateTokensFromFile(file, tokendb) {
    try {
        const text = await textractFromFileWithPath(file);

        // Extract tokens
        let tokens = tokenizer.tokenize(text.toUpperCase());

        // Remove single letter tokens
        tokens = tokens.filter(t => t.length > 1);

        // Remove tokens que só tem números
        tokens = tokens.filter(t => !t.match(/^[0-9]+$/));

        // Ordena
        tokens.sort();

        // Dedup tokens
        const uniqTokens = [...new Set(tokens)];

        for (let idxToken = 0; idxToken < uniqTokens.length; idxToken++) {
            const token = uniqTokens[idxToken];

            let foundTokenData = await dbFind(tokendb, { token });

            if (foundTokenData.length > 1) {
                throw new Error(`Duplicate token data: ${token}`);
            }

            if (foundTokenData.length === 1) {
                const tokenData = foundTokenData[0];
                let files = tokenData.files;
                files.push(file);
                const uniqFiles = [...new Set(files)];
                await dbUpdate(tokendb, { token }, { $set: { files: uniqFiles } });
            } else {
                await dbInsert(tokendb, { token, files: [file] });
            }
        }
    } catch (err) {
        console.error(JSON.stringify(err));
    }
}

if (require.main === module) {
    run();
}