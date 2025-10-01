const fs = require('fs');
const path = require('path');
const { Liquid } = require('liquidjs');
const { faker } = require('@faker-js/faker');
const { DateTime } = require('luxon');
const Client = require('ssh2-sftp-client');

const {
  sftpConfig,
  sftpRemoteDir,
  outputDir,
  mergedOutputDir
} = require('./config/connectivity');
const locationCodes = require('./data/locationCodes');
const usStates      = require('./data/usStates');

const sftp = new Client();

// --- Sequence Helpers ---
const seqFile = path.join(__dirname, 'sequence.json');

function prefixToNumber(prefix) {
  let num = 0;
  for (let i = 0; i < prefix.length; i++) {
    num = num * 26 + (prefix.charCodeAt(i) - 65);
  }
  return num;
}

function numberToPrefix(num) {
  let chars = [];
  for (let i = 0; i < 3; i++) {
    chars.unshift(String.fromCharCode((num % 26) + 65));
    num = Math.floor(num / 26);
  }
  return chars.join('');
}

// Load last prefix or default to ADT
let lastPrefix = 'ADT';
if (fs.existsSync(seqFile)) {
  try {
    const data = JSON.parse(fs.readFileSync(seqFile, 'utf-8'));
    if (data.lastPrefix) lastPrefix = data.lastPrefix;
  } catch (err) {
    console.warn('Could not read sequence file, using default ADT');
  }
}

// Increment sequence
const currentNum = prefixToNumber(lastPrefix);
const nextNum = currentNum + 1;
const dailyPrefix = numberToPrefix(nextNum);

// Save for next run
fs.writeFileSync(seqFile, JSON.stringify({ lastPrefix: dailyPrefix }), 'utf-8');

// --- ID Generation using daily prefix ---
function generateOdyVehicleId(i) {
  return `ODY-${dailyPrefix}${String(i).padStart(4, '0')}`;
}

function generateEracVehicleId(i) {
  return `ERAC-${dailyPrefix}${String(i).padStart(4, '0')}`;
}

function generateRandomLicensePlate(i) {
  return `${dailyPrefix}${String(i).padStart(4, '0')}`;
}


// Index mapping for both defleet and update JSONs
const defleetIdx = {
  license_plate_number: 2,
  license_plate_state:  3,
  year:                 4,
  make:                 5,
  model:                6,
  color:                7,
  vin:                  8
};

// Column mapping for error records only
const errorColumnMapping = {
  brand: 0,
  ody_vehicle_id_number: 1,
  license_plate_number: 2,
  license_plate_state: 3,
  year: 4,
  make: 5,
  model: 6,
  color: 7,
  vin: 8,
  location_group: 9,
  location_code: 10,
  location_name: 11,
  address_1: 12,
  address_2: 13,
  city: 14,
  state: 15,
  zip: 16,
  phone_number: 17,
  vehicle_erac: 18
};

// Column mapping for s_fleet
const s_fleetColumnMapping = {
  brand: 0,
  ody_vehicle_id_number: 1,
  license_plate_number: 2,
  license_plate_state: 3,
  year: 4,
  make: 5,
  model: 6,
  color: 7,
  vin: 8,
  vehicle_erac: 18
};


// Ten fixed words used to overwrite the `color` field in the update JSON
const commonWords = ['delta'];

// Output directories for JSON files
const jsonBaseDir = path.join(__dirname, '../json');  

const sFleetOutputDir   = path.join(jsonBaseDir, 's_fleet');
const defleetOutputDir  = path.join(jsonBaseDir, 'defleet');
const updateOutputDir   = path.join(jsonBaseDir, 'update');
const errorOutputDir    = path.join(jsonBaseDir, 'error_records');
const infleetOutputDir  = path.join(jsonBaseDir, 'infleet_records');
const fleetOutputDir    = path.join(jsonBaseDir, 'fleet_records');
const historyOutputDir  = path.join(jsonBaseDir, 'history_records');


// Splits CSV text into an array of trimmed lines, dropping blank lines and pure-number lines.
function parseCsvRecords(text) {
  return text
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line !== '' && !/^[0-9]+$/.test(line));
}


// Generate `count` rows of synthetic fleet data.
function generateSFleetData(count) {
  const arr = [];
  for (let i = 1; i <= count; i++) {
    arr.push({
      brand: ['Enterprise','Alamo','National'][Math.floor(Math.random()*3)],
      ody_vehicle_id_number: generateOdyVehicleId(i),
      license_plate_number: generateRandomLicensePlate(i),
      license_plate_state: usStates[(i - 1) % usStates.length],
      year: Math.floor(Math.random() * (2026 - 2023 + 1) + 2023),
      make: faker.vehicle.manufacturer(),
      model: faker.vehicle.model(),
      color: faker.color.human(),
      vin: faker.vehicle.vin(),
      location_group: `GroupFR${String(i).padStart(5,'0')}`,
      location_code: locationCodes[Math.floor(Math.random()*locationCodes.length)],
      location_name: faker.location.city(),
      location_1: faker.location.streetAddress(),
      location_2: `Block ${String(i).padStart(6,'0')}`,
      city: faker.location.city(),
      state: faker.helpers.arrayElement(usStates),
      zip: faker.number.int({ min:30000, max:39999 }),
      phone_number: faker.phone.number({ style: 'national' }),
      vehicle_erac: generateEracVehicleId(i),
      transponder_number: ""
    });
  }

  // Inject two “blanked” fields in the last two records if length ≥ 10
  if (arr.length >= 10) {
    const last = arr.length - 1;
    arr[last].license_plate_number = '';
    arr[last - 1].license_plate_state = '';
  }

  return arr;
}

// SFTP helpers
async function getYesterdayRemoteFile(sftpClient, remoteDir, fmt) {
  const key     = DateTime.now().minus({ days: 1 }).toFormat('MM-dd-yyyy');
  const list    = await sftpClient.list(remoteDir);
  const regex   = new RegExp(`^em-fleet-${key}-\\d{2}-\\d{2}\\.${fmt}$`);
  const matches = list.filter(f => regex.test(f.name));
  if (!matches.length) return null;
  matches.sort((a, b) => b.name.localeCompare(a.name));
  return path.posix.join(remoteDir, matches[0].name);
}

async function downloadYesterdayFile(localPath, fmt) {
  try {
    await sftp.connect(sftpConfig);
    const remote = await getYesterdayRemoteFile(sftp, sftpRemoteDir, fmt);
    if (!remote) return null;
    await sftp.get(remote, localPath);
    return localPath;
  } finally {
    sftp.end();
  }
}

// Save JSON helper
function saveJson(folder, name, ts, data) {
  fs.mkdirSync(folder, { recursive: true });
  fs.writeFileSync(
    path.join(folder, `${name}.json`),
    JSON.stringify(data, null, 2),
    'utf-8'
  );
}

async function generateAndMerge(count, fmt) {
  console.log(' Step 1: Generate synthetic fleet data');
  const sfleet_data = generateSFleetData(count);

  console.log(' Step 2: Render today\'s CSV from Liquid template');
  const tpl      = fs.readFileSync(path.join(__dirname,'fleettemplete.liquid'),'utf-8');
  const todayCsv = await new Liquid({ greedy: true }).parseAndRender(tpl, { sfleet_data });

  console.log(' Step 3: Save today\'s raw CSV');
  const ts       = DateTime.now().toFormat('MM-dd-yyyy-HH-mm');
  const fileName = `em-fleet-${ts}.${fmt}`;
  fs.mkdirSync(outputDir, { recursive: true });
  fs.writeFileSync(path.join(outputDir, fileName), todayCsv, 'utf-8');

  console.log(' Step 4: Download yesterday\'s file from SFTP');
  const tmpYest   = path.join(__dirname, `temp-yesterday.${fmt}`);
  const yestLocal = await downloadYesterdayFile(tmpYest, fmt);

  console.log(' Step 5: Load and clean yesterday\'s data');
  let oldRecords = [];
  if (yestLocal && fs.existsSync(yestLocal)) {
    const raw = fs.readFileSync(yestLocal,'utf-8')
      .split(/\r?\n/).map(l => l.trim()).filter(l => l !== '');
    raw.shift();              // drop old count line
    if (raw.length >= 2) raw.splice(-2, 2); // remove last 2 error rows
    oldRecords = raw;
    fs.unlinkSync(tmpYest);
  }

  console.log(' Step 6: Prepare defleet JSON');
  let defleetBatch = [];
  if (oldRecords.length >= 10) defleetBatch = oldRecords.splice(-10, 10);
  const defleetData = defleetBatch.map(line => {
    const cols = line.split(',');
    return {
      license_plate_number: cols[defleetIdx.license_plate_number],
      license_plate_state:  cols[defleetIdx.license_plate_state],
      year:                 cols[defleetIdx.year],
      make:                 cols[defleetIdx.make],
      model:                cols[defleetIdx.model],
      color:                cols[defleetIdx.color],
      vin:                  cols[defleetIdx.vin]
    };
  });
  if (defleetData.length) {
    saveJson(defleetOutputDir, 'defleet', ts, defleetData);
    // console.log(`    Defleet JSON saved (${defleetData.length} records)`);
  }

  console.log(' Step 7: Prepare update JSON');
  const updateStart = oldRecords.length - 10;
  const updateBatch = oldRecords.slice(updateStart);
  const updateData  = updateBatch.map((line, i) => {
    const cols = line.split(',');
    return {
      license_plate_number: cols[defleetIdx.license_plate_number],
      license_plate_state:  cols[defleetIdx.license_plate_state],
      year:                 cols[defleetIdx.year],
      make:                 cols[defleetIdx.make],
      model:                cols[defleetIdx.model],
      color:                commonWords[i % commonWords.length],
      vin:                  cols[defleetIdx.vin]
    };
  });
  if (updateData.length) 
    saveJson(updateOutputDir, 'update', ts, updateData);
    

  console.log(' Step 8: Parse today\'s new records');
  const newRecords = parseCsvRecords(todayCsv);

  //  Apply updates to oldRecords before merging
  const updateMap = new Map(updateData.map(r => [`${r.license_plate_number}_${r.license_plate_state}`, r]));
  oldRecords = oldRecords.map(line => {
    const cols = line.split(',');
    const key = `${cols[defleetIdx.license_plate_number]}_${cols[defleetIdx.license_plate_state]}`;
    if (updateMap.has(key)) {
      const upd = updateMap.get(key);
      cols[defleetIdx.color] = upd.color;
      cols[defleetIdx.vin]   = upd.vin;
      cols[defleetIdx.year]  = upd.year;
      cols[defleetIdx.make]  = upd.make;
      cols[defleetIdx.model] = upd.model;
    }
    return cols.join(',');
  });

  console.log(' Step 9: Generate error, infleet, and fleet JSONs');
  const errorRecords = newRecords.slice(-2).map(line => {
    const cols = line.split(',');
    const obj = {};
    for (const [key, idx] of Object.entries(errorColumnMapping)) {
      obj[key] = cols[idx] || '';
    }
    return obj;
  });

  const infleetRecords = newRecords.slice(0, -2).map(line => {
    const cols = line.split(',');
    return {
      license_plate_number: cols[defleetIdx.license_plate_number],
      license_plate_state:  cols[defleetIdx.license_plate_state],
      year:                 cols[defleetIdx.year],
      make:                 cols[defleetIdx.make],
      model:                cols[defleetIdx.model],
      color:                cols[defleetIdx.color],
      vin:                  cols[defleetIdx.vin]
    };
  });

  saveJson(errorOutputDir, 'error_records', ts, errorRecords);
  saveJson(infleetOutputDir, 'infleet_records', ts, infleetRecords);
  saveJson(fleetOutputDir, 'fleet_records', ts, infleetRecords.map(r => ({
    license_plate_number: r.license_plate_number,
    license_plate_state:  r.license_plate_state
  })));

  console.log(' Step 10: Save history JSON');
  saveJson(historyOutputDir, 'history_records', ts, [...infleetRecords, ...updateData]);

  console.log(' Step 11: Save s_fleet JSON');
  const sFleetRecords = [...oldRecords, ...newRecords.slice(0, -2)].map(line => {
    const cols = line.split(',');
    const obj = {};
    for (const [key, idx] of Object.entries(s_fleetColumnMapping)) {
      obj[key] = cols[idx] || '';
    }
    return obj;
  });
  saveJson(sFleetOutputDir, 's_fleet', ts, sFleetRecords);

  console.log(' Step 12: Generated a full fleet file');
  const total  = oldRecords.length + newRecords.length;
  const merged = [ total.toString(), ...oldRecords, ...newRecords ];
  fs.mkdirSync(mergedOutputDir, { recursive: true });
  fs.writeFileSync(
    path.join(mergedOutputDir, fileName),
    merged.join('\n'),
    'utf-8'
  );

  console.log(' CSV and JSON files generated successfully.');
}

// CLI Entrypoint
const [rawCount, rawFmt] = process.argv.slice(2);
const count = parseInt(rawCount, 10);
const fmt   = (rawFmt || 'csv').toLowerCase();
if (isNaN(count) || !['csv','txt'].includes(fmt)) {
  console.error('Usage: node merge-fleet-records.js <count> <csv|txt>');
  process.exit(1);
}
generateAndMerge(count, fmt);