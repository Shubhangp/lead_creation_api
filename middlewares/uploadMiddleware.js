const multer = require("multer");
const path = require("path");

// Configure storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/");
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    cb(null, `leads-${uniqueSuffix}${path.extname(file.originalname)}`);
  },
});

// File Filter
const fileFilter = (req, file, cb) => {
  const allowedExtensions = [".xlsx", '.xls', ".csv"];
  const ext = path.extname(file.originalname).toLowerCase();
  if (allowedExtensions.includes(ext)) {
    cb(null, true);
  } else {
    cb(new Error("Only .xlsx or .csv files are allowed!"), false);
  }
};

const upload = multer({ 
  storage,
  limits: {
    fileSize: 100 * 1024 * 1024, //100MB
  },
  fileFilter
});

module.exports = upload;