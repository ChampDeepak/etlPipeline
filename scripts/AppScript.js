/**
 * GOOGLE APPS SCRIPT FOR DATA VALIDATION & AUTOMATION
 * Task 6: Validate entries, Visual Feedback, and JSON Export
 */

// ==========================================
// CONFIGURATION & CREDENTIALS
// ==========================================
// ⚠️ IMPORTANT: Replace 'your_token_here' with your actual GitHub PAT
const GITHUB_TOKEN = 'your_github_personal_access_token_here';
const GITHUB_REPO = 'champdeepak/etlpipeline'; 

const SHEET_NAME = 'netflix';
const COL_SHOW_ID = 1; // Column A is the Show ID 
const COL_TYPE = 2;          // Column B
const COL_TITLE = 3;         // Column C
const COL_YEAR = 8;          // Column H
const COL_STATUS = 13;       // Column M

// ==========================================
// 1. GITHUB TRIGGER (The "Bridge")
// ==========================================
function triggerGithubAction() {
  const url = `https://api.github.com/repos/${GITHUB_REPO}/dispatches`;
  
  const payload = {
    "event_type": "sheet_update" // Matches the type in your .yml file
  };
  
  const options = {
    "method": "post",
    "contentType": "application/json",
    "headers": {
      "Authorization": "Bearer " + GITHUB_TOKEN,
      "Accept": "application/vnd.github.v3+json"
    },
    "payload": JSON.stringify(payload)
  };
  
  try {
    UrlFetchApp.fetch(url, options);
    console.log("✅ GitHub Action Triggered Successfully!");
  } catch (e) {
    console.error("❌ Failed to trigger GitHub Action: " + e.toString());
  }
}

// ==========================================
// 2. AUTOMATIC TRIGGER (Runs on Single Row Edit)
// ==========================================
function onEdit(e) {
  const sheet = e.source.getActiveSheet();
  
  if (sheet.getName() !== SHEET_NAME || e.range.getRow() === 1) {
    return;
  }

  // Validate ONLY the row that was edited
  validateSingleRow(sheet, e.range.getRow());
}



/**
 * Validates a single row (Helper function for onEdit)
 */
function validateSingleRow(sheet, rowNum) {
  const dataRange = sheet.getRange(rowNum, 1, 1, COL_STATUS);
  const values = dataRange.getValues()[0];
  
  const showId = values[COL_SHOW_ID - 1]; // Get Show ID from Column A
  const type = values[COL_TYPE - 1];
  const title = values[COL_TITLE - 1];
  const year = values[COL_YEAR - 1];
  
  let errors = getErrors(showId, type, title, year); // Pass showId to validator
  
  const statusCell = sheet.getRange(rowNum, COL_STATUS);
  
  if (errors.length === 0) {
    // 1. Update UI to Green
    statusCell.setValue("✅ READY").setBackground("#d9ead3");
    
    // 2. TRIGGER AUTOMATION
    if (GITHUB_TOKEN && GITHUB_TOKEN !== 'your_github_personal_access_token_here') {
       triggerGithubAction();
       sheet.getRange(rowNum, COL_STATUS).setNote("Sync triggered at " + new Date());
    }
    
  } else {
    // FAILURE: Mark as Invalid and DO NOT trigger GitHub
    statusCell.setValue("❌ " + errors.join(", ")).setBackground("#f4cccc");
    statusCell.setNote(null); // Clear previous notes
  }
}

/**
 * Update the Batch function as well to include the new column
 */
function validateAllRowsBatch() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  
  const range = sheet.getRange(2, 1, lastRow - 1, COL_STATUS);
  const values = range.getValues(); 
  let statusUpdates = []; 
  
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    // Pass Column A, B, C, H
    let errors = getErrors(row[COL_SHOW_ID - 1], row[COL_TYPE - 1], row[COL_TITLE - 1], row[COL_YEAR - 1]);

    if (errors.length === 0) {
      statusUpdates.push(["✅ READY"]);
    } else {
      statusUpdates.push(["❌ " + errors.join(", ")]);
    }
  }
  
  const statusRange = sheet.getRange(2, COL_STATUS, statusUpdates.length, 1);
  statusRange.setValues(statusUpdates);
  SpreadsheetApp.getUi().alert('✅ Batch Validation Complete!');
}

function getErrors(showId, type, title, year) {
  let errors = [];
  
  // NEW: Mandatory Show ID Check
  if (!showId || showId.toString().trim() === "") {
    errors.push("Missing Show ID");
  }
  
  if (!title || title.toString().trim() === "") {
    errors.push("Missing Title");
  }
  
  if (type !== "Movie" && type !== "TV Show") {
    errors.push("Invalid Type");
  }
  
  if (isNaN(year) || year < 1900 || year > 2030) {
    errors.push("Invalid Year");
  }
  
  return errors;
}

// ==========================================
// 4. MENU & EXPORT
// ==========================================
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
  const htmlOutput = HtmlService.createHtmlOutput('<textarea style="width:100%; height:300px;">' + jsonString + '</textarea>')
      .setWidth(600).setHeight(400);
  SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'Exported JSON Data');
}