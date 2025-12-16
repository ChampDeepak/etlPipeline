/**
 * GOOGLE APPS SCRIPT FOR DATA VALIDATION & AUTOMATION
 * FIXED: Respects '🚀 ADDED' status & GitHub Permissions
 */

// ==========================================
// CONFIGURATION & CREDENTIALS
// ==========================================
// ⚠️ RE-PASTE YOUR NEW CLASSIC TOKEN HERE
const GITHUB_TOKEN = '';
const GITHUB_REPO = 'champdeepak/etlpipeline'; 

const SHEET_NAME = 'netflix';
const COL_SHOW_ID = 1;       // Column A
const COL_TYPE = 2;          // Column B
const COL_TITLE = 3;         // Column C
const COL_YEAR = 8;          // Column H
const COL_STATUS = 13;       // Column M

// ==========================================
// 1. GITHUB TRIGGER
// ==========================================
function triggerGithubAction() {
  const url = `https://api.github.com/repos/${GITHUB_REPO}/dispatches`;
  
  const payload = {
    "event_type": "sheet_update" 
  };
  
  const options = {
    "method": "post",
    "contentType": "application/json",
    "headers": {
      "Authorization": "Bearer " + GITHUB_TOKEN,
      "Accept": "application/vnd.github.v3+json"
    },
    "payload": JSON.stringify(payload),
    "muteHttpExceptions": true // Get better error messages
  };
  
  try {
    const response = UrlFetchApp.fetch(url, options);
    const code = response.getResponseCode();
    if (code >= 200 && code < 300) {
       console.log("✅ GitHub Action Triggered Successfully!");
    } else {
       console.error("❌ GitHub Error (" + code + "): " + response.getContentText());
    }
  } catch (e) {
    console.error("❌ Request Failed: " + e.toString());
  }
}

// ==========================================
// 2. AUTOMATIC TRIGGER (Single Row Edit)
// ==========================================
function handleEdit(e) {
  if (!e) return; // Safety check if run manually without event
  const sheet = e.source.getActiveSheet();
  const range = e.range;

  // 1. Basic Checks
  if (sheet.getName() !== SHEET_NAME || range.getRow() === 1) return;

  // 2. CRITICAL FIX: If user is manually changing the STATUS column, IGNORE IT.
  // This prevents the script from overwriting your manual '🚀 ADDED' entry.
  if (range.getColumn() === COL_STATUS) return;

  // Validate ONLY the row that was edited
  validateSingleRow(sheet, range.getRow());
}

function validateSingleRow(sheet, rowNum) {
  const dataRange = sheet.getRange(rowNum, 1, 1, COL_STATUS);
  const values = dataRange.getValues()[0];
  
  const showId = values[COL_SHOW_ID - 1];
  const type = values[COL_TYPE - 1];
  const title = values[COL_TITLE - 1];
  const year = values[COL_YEAR - 1];
  const currentStatus = values[COL_STATUS - 1]; // Get current status

  // OPTIONAL: If it's already ADDED, do we want to re-validate on edit?
  // Usually YES (if you edit data, it needs to be synced again).
  // So we proceed with validation.
  
  let errors = getErrors(showId, type, title, year);
  
  const statusCell = sheet.getRange(rowNum, COL_STATUS);
  
  if (errors.length === 0) {
    // Only update and trigger if it's NOT ALREADY Ready
    // (This prevents infinite loops if you type in a cell)
    if (currentStatus !== "✅ READY") { 
        statusCell.setValue("✅ READY").setBackground("#d9ead3");
        
        // TRIGGER AUTOMATION
        if (GITHUB_TOKEN && GITHUB_TOKEN !== 'your_new_classic_token_here') {
           triggerGithubAction();
           sheet.getRange(rowNum, COL_STATUS).setNote("Sync triggered at " + new Date());
        }
    }
  } else {
    statusCell.setValue("❌ " + errors.join(", ")).setBackground("#f4cccc");
    statusCell.setNote(null);
  }
}

// ==========================================
// 3. BATCH VALIDATION (Menu Click)
// ==========================================
function validateAllRowsBatch() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  
  const range = sheet.getRange(2, 1, lastRow - 1, COL_STATUS);
  const values = range.getValues(); 
  let statusUpdates = []; 
  
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const currentStatus = row[COL_STATUS - 1];

    // --- CRITICAL FIX: PRESERVE 'ADDED' ROWS ---
    if (currentStatus === "🚀 ADDED") {
      statusUpdates.push(["🚀 ADDED"]); // Keep it exactly as is
      continue; // Skip validation for this row
    }

    // Otherwise, validate normally
    let errors = getErrors(row[COL_SHOW_ID - 1], row[COL_TYPE - 1], row[COL_TITLE - 1], row[COL_YEAR - 1]);

    if (errors.length === 0) {
      statusUpdates.push(["✅ READY"]);
    } else {
      statusUpdates.push(["❌ " + errors.join(", ")]);
    }
  }
  
  const statusRange = sheet.getRange(2, COL_STATUS, statusUpdates.length, 1);
  statusRange.setValues(statusUpdates);
  SpreadsheetApp.getUi().alert('✅ Batch Validation Complete! (Preserved existing ADDED rows)');
}

// ... (getErrors, onOpen, exportToJSON remain the same) ...
function getErrors(showId, type, title, year) {
  let errors = [];
  if (!showId || showId.toString().trim() === "") errors.push("Missing Show ID");
  if (!title || title.toString().trim() === "") errors.push("Missing Title");
  if (type !== "Movie" && type !== "TV Show") errors.push("Invalid Type");
  if (isNaN(year) || year < 1900 || year > 2030) errors.push("Invalid Year");
  return errors;
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🚀 Data Engineering')
      .addItem('Validate All Rows', 'validateAllRowsBatch')
      .addItem('Export Valid Rows to JSON', 'exportToJSON')
      .addToUi();
}

function exportToJSON() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const rows = data.slice(1);
  let exportData = [];
  rows.forEach(row => {
    if (row[COL_STATUS - 1] === "✅ READY") {
      let obj = {};
      headers.forEach((header, index) => {
        if (index < COL_STATUS - 1) obj[header] = row[index];
      });
      exportData.push(obj);
    }
  });
  const jsonString = JSON.stringify(exportData, null, 2);
  const htmlOutput = HtmlService.createHtmlOutput('<textarea style="width:100%; height:300px;">' + jsonString + '</textarea>').setWidth(600).setHeight(400);
  SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'Exported JSON Data');
}