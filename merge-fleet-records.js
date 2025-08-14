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


// Ten fixed words used to overwrite the `color` field in the update JSON
const commonWords = ['delta'];

// Output directories for JSON files
const defleetOutputDir   = path.join(__dirname, 'defleet');
const updateOutputDir    = path.join(__dirname, 'update');
const errorOutputDir     = path.join(__dirname, 'error_records');
const infleetOutputDir   = path.join(__dirname, 'infleet_records');
const fleetOutputDir     = path.join(__dirname, 'fleet_records');
const historyOutputDir   = path.join(__dirname, 'history_records');

// Splits CSV text into an array of trimmed lines, dropping blank lines and pure-number lines.
function parseCsvRecords(text) {
  return text
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line !== '' && !/^[0-9]+$/.test(line));
}

// Generate IDs and plates
function generateOdyVehicleId(i) {
  const plate = faker.helpers.arrayElement(['ADN']);
  return `ODY-${plate}${String(i).padStart(4, '0')}`;
}

function generateEracVehicleId(i) {
  const plate = faker.helpers.arrayElement(['ADN']);
  return `ERAC-${plate}${String(i).padStart(4, '0')}`;
}

function generateRandomLicensePlate(i) {
  const plate = faker.helpers.arrayElement(['ADN']);
  return `${plate}${String(i).padStart(4, '0')}`;
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
      vehicle_erac: generateEracVehicleId(i)
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
    path.join(folder, `${name}-${ts}.json`),
    JSON.stringify(data, null, 2),
    'utf-8'
  );
}

async function generateAndMerge(count, fmt) {
  // 1. Render today's CSV
  const sfleet_data = generateSFleetData(count);
  const tpl         = fs.readFileSync(path.join(__dirname,'fleettemplete.liquid'),'utf-8');
  const todayCsv    = await new Liquid({ greedy: true })
    .parseAndRender(tpl, { sfleet_data });

  // 2. Save raw today
  const ts       = DateTime.now().toFormat('MM-dd-yyyy-HH-mm');
  const fileName = `em-fleet-${ts}.${fmt}`;
  fs.mkdirSync(outputDir, { recursive: true });
  fs.writeFileSync(path.join(outputDir, fileName), todayCsv, 'utf-8');

  // 3. Download yesterday's file
  const tmpYest   = path.join(__dirname, `temp-yesterday.${fmt}`);
  const yestLocal = await downloadYesterdayFile(tmpYest, fmt);

  // 4. Load & clean yesterday’s data
  let oldRecords = [];
  if (yestLocal && fs.existsSync(yestLocal)) {
    const raw = fs.readFileSync(yestLocal,'utf-8')
      .split(/\r?\n/).map(l => l.trim()).filter(l => l !== '');
    raw.shift();              // drop old count line
    if (raw.length >= 2) {
      raw.splice(-2, 2);      // delete last 2 error rows
    }
    oldRecords = raw;
    fs.unlinkSync(tmpYest);
  }

  // 5. Defleet
  let defleetBatch = [];
  if (oldRecords.length >= 10) {
    defleetBatch = oldRecords.splice(-10, 10);
  }
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
  if (defleetData.length) saveJson(defleetOutputDir, 'defleet', ts, defleetData);

  // 6. Update
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
  if (updateData.length) saveJson(updateOutputDir, 'update', ts, updateData);

  // Apply same color override in CSV
  for (let i = 0; i < updateBatch.length; i++) {
    const idx = updateStart + i;
    const cols = oldRecords[idx].split(',');
    cols[defleetIdx.color] = commonWords[i % commonWords.length];
    oldRecords[idx] = cols.join(',');
  }

  // 7. Parse today's new records
  const newRecords = parseCsvRecords(todayCsv);

  // 8. Generate error + infleet + fleet JSONs
  const errorRecords = newRecords.slice(-2).map(line => {
    const cols = line.split(',');
    const obj = {};
    for (const [key, idx] of Object.entries(errorColumnMapping)) {
      obj[key] = cols[idx] || '';
    }
    return obj;
  });

  // INFLEET = all new records except last two
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

  // FLEET = only LPN + LPS from infleet
  const fleetRecords = infleetRecords.map(r => ({
    license_plate_number: r.license_plate_number,
    license_plate_state:  r.license_plate_state
  }));

  saveJson(errorOutputDir, 'error_records', ts, errorRecords);
  saveJson(infleetOutputDir, 'infleet_records', ts, infleetRecords);
  saveJson(fleetOutputDir, 'fleet_records', ts, fleetRecords);

  // 9. History = infleet + update
  const historyRecords = [...infleetRecords, ...updateData];
  saveJson(historyOutputDir, 'history_records', ts, historyRecords);

  // 10. Merge CSV
  const total  = oldRecords.length + newRecords.length;
  const merged = [ total.toString(), ...oldRecords, ...newRecords ];
  fs.mkdirSync(mergedOutputDir, { recursive: true });
  fs.writeFileSync(
    path.join(mergedOutputDir, fileName),
    merged.join('\n'),
    'utf-8'
  );

  console.log('✅ All CSV and JSON files written successfully.');
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
