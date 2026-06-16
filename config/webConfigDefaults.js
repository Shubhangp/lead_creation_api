const { buildLenderList } = require('./lenderCatalog');

function buildDefaultWebConfig() {
  return {
    redirectToSuccess: true,
    formMode: 'full',
    lenders: buildLenderList(),
  };
}

module.exports = { buildDefaultWebConfig };
