import { google } from 'googleapis';

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(200).end();
  }

  res.setHeader('Access-Control-Allow-Origin', '*');

  try {
    const { action, sheetId, sheetName, data, id } = req.body || {};
    if (!action) return res.status(400).json({ ok: false, error: 'Missing action' });

    const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    if (!credsJson) return res.status(500).json({ ok: false, error: 'Missing GOOGLE_SERVICE_ACCOUNT_JSON' });
    const creds = JSON.parse(credsJson);

    const jwt = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );

    await jwt.authorize();
    const sheets = google.sheets({ version: 'v4', auth: jwt });

    if (action === 'update') {
      if (!sheetId || !sheetName || !data) {
        return res.status(400).json({ ok: false, error: 'Missing sheetId/sheetName/data' });
      }
      
      // Use ID from data or from separate id field
      const recordId = data.ID || data.id || id;
      if (!recordId) {
        return res.status(400).json({ ok: false, error: 'Missing ID field' });
      }

      // Read header
      const headerResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!1:1`,
      });
      const headers = headerResp.data.values?.[0] || [];
      const normMap = {}; // normalized header -> real header
      headers.forEach(h => { normMap[norm(h)] = h; });
      function valueByHeader(h, source) {
        // try exact header first, then normalized key match
        if (source.hasOwnProperty(h)) return source[h];
        const n = norm(h);
        const hit = Object.keys(source).find(k => norm(k) === n);
        return hit ? source[hit] : '';
      }

      // Find row by ID (col A)
      const idResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:A`,
      });
      const colA = idResp.data.values?.map(r => r[0]) || [];
      let rowIndex = colA.findIndex((v, i) => i > 0 && String(v || '').trim().toLowerCase() === String(recordId).trim().toLowerCase());
      if (rowIndex === -1) {
        // append
        const row = headers.map(h => valueByHeader(h, data));
        row[0] = recordId; // ensure ID at col A
        await sheets.spreadsheets.values.append({
          spreadsheetId: sheetId,
          range: sheetName,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [row] },
        });
        return res.status(200).json({ ok: true, created: true });
      } else {
        // rowIndex is 0-based; header at 0 -> +1 for 1-based row
        const targetRow = rowIndex + 1;
        // Read current row to avoid wiping unspecified fields
        const currRange = `${sheetName}!A${targetRow}:${columnLetter(headers.length)}${targetRow}`;
        const existingResp = await sheets.spreadsheets.values.get({
          spreadsheetId: sheetId,
          range: currRange,
        });
        const existingRow = existingResp.data.values?.[0] || Array(headers.length).fill('');
        // Build a header->value map from existing row
        const currMap = {};
        headers.forEach((h, i) => { currMap[h] = existingRow[i] ?? ''; });
        // Merge with incoming data (only override provided keys)
        Object.keys(data).forEach(k => {
          const targetHeader = normMap[norm(k)] || k;
          currMap[targetHeader] = data[k];
        });
        // ensure ID in first column
        currMap[headers[0]] = recordId;
        // Reconstruct row in header order
        const row = headers.map(h => currMap[h] ?? valueByHeader(h, data) ?? '');
        const range = currRange;
        await sheets.spreadsheets.values.update({
          spreadsheetId: sheetId,
          range,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [row] },
        });
        return res.status(200).json({ ok: true, updated: true });
      }
    }

    if (action === 'delete') {
      if (!sheetId || !sheetName || !id) {
        return res.status(400).json({ ok: false, error: 'Missing sheetId/sheetName/id' });
      }
      const idResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:A`,
      });
      const colA = idResp.data.values?.map(r => r[0]) || [];
      const rowIndex = colA.findIndex((v, i) => i > 0 && String(v || '').trim().toLowerCase() === String(id).trim().toLowerCase());
      if (rowIndex === -1) return res.status(404).json({ ok: false, error: 'ID not found' });
      const targetRow = rowIndex + 1;
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: sheetId,
        requestBody: {
          requests: [{ deleteDimension: { range: { sheetId: await getSheetGid(sheets, sheetId, sheetName), dimension: 'ROWS', startIndex: targetRow - 1, endIndex: targetRow } } }],
        },
      });
      return res.status(200).json({ ok: true, deleted: true });
    }

    if (action === 'updateBillSTT') {
      const { billNumber, stt } = req.body;
      if (!sheetId || !sheetName || !billNumber || !stt) {
        return res.status(400).json({ ok: false, error: 'Missing sheetId/sheetName/billNumber/stt' });
      }

      // Read header to find STT column
      const headerResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!1:1`,
      });
      const headers = headerResp.data.values?.[0] || [];
      const sttColIndex = headers.findIndex(h => norm(h) === 'stt');
      if (sttColIndex === -1) {
        return res.status(400).json({ ok: false, error: 'STT column not found' });
      }

      // Find row by Bill number (assuming it's in column B or find by "Số Bill")
      const billColIndex = headers.findIndex(h => norm(h).includes('so_bill') || norm(h).includes('bill'));
      if (billColIndex === -1) {
        return res.status(400).json({ ok: false, error: 'Bill number column not found' });
      }

      const billResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!${columnLetter(billColIndex + 1)}:${columnLetter(billColIndex + 1)}`,
      });
      const billCol = billResp.data.values?.map(r => r[0]) || [];
      const rowIndex = billCol.findIndex((v, i) => i > 0 && String(v || '').trim() === String(billNumber).trim());
      
      if (rowIndex === -1) {
        return res.status(404).json({ ok: false, error: 'Bill number not found' });
      }

      const targetRow = rowIndex + 1;
      const sttColumn = columnLetter(sttColIndex + 1);
      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${sheetName}!${sttColumn}${targetRow}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[stt]] },
      });

      return res.status(200).json({ ok: true, updated: true });
    }

    if (action === 'updateAllSTT') {
      const { bills } = req.body;
      if (!sheetId || !sheetName || !bills || !Array.isArray(bills)) {
        return res.status(400).json({ ok: false, error: 'Missing sheetId/sheetName/bills' });
      }

      // Read header to find STT and Bill columns
      const headerResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!1:1`,
      });
      const headers = headerResp.data.values?.[0] || [];
      const sttColIndex = headers.findIndex(h => norm(h) === 'stt');
      const billColIndex = headers.findIndex(h => norm(h).includes('so_bill') || norm(h).includes('bill'));
      
      if (sttColIndex === -1 || billColIndex === -1) {
        return res.status(400).json({ ok: false, error: 'STT or Bill column not found' });
      }

      // Prepare batch update
      const requests = [];
      const sttColumn = columnLetter(sttColIndex + 1);
      const billColumn = columnLetter(billColIndex + 1);

      // Get all current data to find matching rows
      const allDataResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:Z`,
      });
      const allData = allDataResp.data.values || [];

      bills.forEach(bill => {
        const billNumber = bill['Số Bill'];
        const stt = bill['STT'];
        
        // Find row by bill number
        for (let i = 1; i < allData.length; i++) {
          if (allData[i][billColIndex] && String(allData[i][billColIndex]).trim() === String(billNumber).trim()) {
            requests.push({
              range: `${sheetName}!${sttColumn}${i + 1}`,
              values: [[stt]]
            });
            break;
          }
        }
      });

      if (requests.length > 0) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: sheetId,
          valueInputOption: 'USER_ENTERED',
          requestBody: { valueInputOption: 'USER_ENTERED', data: requests },
        });
      }

      return res.status(200).json({ ok: true, updated: requests.length });
    }

    return res.status(400).json({ ok: false, error: 'Unknown action' });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
}

function columnLetter(n) {
  let s = '';
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function getSheetGid(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const sh = meta.data.sheets?.find(s => s.properties?.title === sheetName);
  return sh?.properties?.sheetId;
}

function norm(s) {
  return String(s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
}


