// config/connectivity.js
const { sftpConfig } = require('./credentials');

module.exports = {
  sftpConfig,
  sftpRemoteDir: '/em/fleet/fleet-archive-files',
  outputDir: 'D:/sampledata',
  mergedOutputDir: 'D:/mergeddata'
};
