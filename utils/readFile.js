const xlsx = require('xlsx');
const fs = require('fs');

const parseFileInChunks = async (filePath, chunkSize = 500, onChunk) => {
  const workbook = xlsx.readFile(filePath, {
    dense: true,
    cellFormula: false,
    cellStyles: false,
    cellHTML: false,
  });

  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];

  const range = xlsx.utils.decode_range(sheet['!ref']);
  const totalRows = range.e.r;

  const headers = [];
  for (let col = range.s.c; col <= range.e.c; col++) {
    const cellAddress = xlsx.utils.encode_cell({ r: 0, c: col });
    const cell = sheet[cellAddress];
    headers.push(cell ? String(cell.v).trim() : `col_${col}`);
  }

  let chunk = [];
  let totalProcessed = 0;

  for (let row = 1; row <= totalRows; row++) {
    const rowData = {};
    let hasData = false;

    for (let col = range.s.c; col <= range.e.c; col++) {
      const cellAddress = xlsx.utils.encode_cell({ r: row, c: col });
      const cell = sheet[cellAddress];
      if (cell !== undefined) {
        rowData[headers[col]] = cell.v;
        hasData = true;
      }
    }

    if (!hasData) continue;

    chunk.push(rowData);

    if (chunk.length >= chunkSize) {
      await onChunk([...chunk]);
      totalProcessed += chunk.length;
      chunk = [];
    }
  }

  if (chunk.length > 0) {
    await onChunk([...chunk]);
    totalProcessed += chunk.length;
    chunk = [];
  }

  return totalProcessed;
};


const readFile = (filePath) => {
  const workbook = xlsx.readFile(filePath, {
    cellFormula: false,
    cellStyles: false,
    cellHTML: false,
  });
  const sheetName = workbook.SheetNames[0];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
};

const deleteFile = (filePath) => {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log(`🗑️  Deleted temp file: ${filePath}`);
    }
  } catch (err) {
    console.error('Error deleting file:', err);
  }
};

module.exports = { parseFileInChunks, readFile, deleteFile };