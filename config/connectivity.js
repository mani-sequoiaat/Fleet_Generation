// config/connectivity.js
const { sftpConfig } = require('./credentials');
const path = require('path');

module.exports = {
  sftpConfig,
  sftpRemoteDir: '/em/fleet/fleet-archive-files',
  outputDir: path.join(__dirname, '../Fleet-sampledata'),
  mergedOutputDir: path.join(__dirname, '../../generated-fleet-files')
};
