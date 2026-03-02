// utils/readFile.js
const xlsx = require('xlsx');
const fs = require('fs');

const parseFileInChunks = async (filePath, chunkSize = 500, onChunk) => {
  const workbook = xlsx.readFile(filePath, {
    cellFormula: false,
    cellStyles:  false,
    cellHTML:    false,
    cellDates:   true,
    raw:         false,
  });

  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];

  if (!sheet || !sheet['!ref']) {
    console.warn('⚠️  Sheet is empty or has no ref range');
    return 0;
  }

  const range = xlsx.utils.decode_range(sheet['!ref']);
  console.log(`📋 Sheet: "${sheetName}" | Rows: ${range.e.r} | Cols: ${range.e.c + 1}`);

  // Extract headers from row 0
  const headers = [];
  for (let col = range.s.c; col <= range.e.c; col++) {
    const cellAddr = xlsx.utils.encode_cell({ r: 0, c: col });
    const cell = sheet[cellAddr];
    headers[col] = cell ? String(cell.v).trim() : `col_${col}`;
  }

  console.log(`📑 Headers found: ${headers.filter(Boolean).join(', ')}`);

  let chunk = [];
  let totalProcessed = 0;

  for (let row = 1; row <= range.e.r; row++) {
    const rowData = {};
    let hasData = false;

    for (let col = range.s.c; col <= range.e.c; col++) {
      const cellAddr = xlsx.utils.encode_cell({ r: row, c: col });
      const cell = sheet[cellAddr];
      if (cell !== undefined && cell.v !== undefined && cell.v !== '') {
        rowData[headers[col]] = cell.v;
        hasData = true;
      }
    }

    if (!hasData) continue;

    chunk.push(rowData);

    if (chunk.length >= chunkSize) {
      await onChunk([...chunk]);
      totalProcessed += chunk.length;
      console.log(`  ↳ Chunk flushed: ${totalProcessed} rows processed so far`);
      chunk = []; // free memory
    }
  }

  // Flush remaining rows
  if (chunk.length > 0) {
    await onChunk([...chunk]);
    totalProcessed += chunk.length;
  }

  return totalProcessed;
};

// ─── One-shot reader (small files / backward compat) ──────────────────────────
const readFile = (filePath) => {
  const workbook = xlsx.readFile(filePath, {
    cellFormula: false,
    cellStyles:  false,
    cellHTML:    false,
    cellDates:   true,
    raw:         false,
  });
  const sheetName = workbook.SheetNames[0];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
};

// ─── Synchronous delete ────────────────────────────────────────────────────────
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