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
      if (!sheetId || !sheetName || !data || !data.id) {
        return res.status(400).json({ ok: false, error: 'Missing sheetId/sheetName/data.id' });
      }

      // Read header
      const headerResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!1:1`,
      });
      const headers = headerResp.data.values?.[0] || [];

      // Find row by ID (col A)
      const idResp = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:A`,
      });
      const colA = idResp.data.values?.map(r => r[0]) || [];
      let rowIndex = colA.findIndex((v, i) => i > 0 && String(v || '').trim().toLowerCase() === String(data.id).trim().toLowerCase());
      if (rowIndex === -1) {
        // append
        const row = headers.map(h => data[h] ?? '');
        row[0] = data.id; // ensure ID at col A
        await sheets.spreadsheets.values.append({
          spreadsheetId: sheetId,
          range: sheetName,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [row] },
        });
        return res.status(200).json({ ok: true, created: true });
      } else {
        // rowIndex is 0-based; header at 0 -> add 1 for 1-based row, +1 again to skip header
        const targetRow = rowIndex + 1; // already accounts header because findIndex starts at >0
        // Build update range/values for entire row based on headers
        const row = headers.map(h => data[h] ?? '');
        row[0] = data.id;
        const range = `${sheetName}!A${targetRow}:${columnLetter(headers.length)}${targetRow}`;
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


