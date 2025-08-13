// sync.js — Cloud SQL Postgres, resilient JSONB + resumable cursor

const { Pool } = require('pg');
const fetch = require('node-fetch');
const crypto = require('crypto');

/* ----------------------------- knobs via env ----------------------------- */
const JOBS_ENDPOINT = process.env.JOBS_ENDPOINT || 'https://api.fantastic.jobs/active-ats-6m';
const API_LIMIT     = Number(process.env.PAGE_LIMIT ?? 200);   // API page size
const BATCH         = Number(process.env.BATCH ?? 50);         // rows per UPSERT
const PAGE_PAUSE_MS = Number(process.env.PAGE_PAUSE_MS ?? 100);
const JOB_KEY       = process.env.JOB_KEY || '6m_backfill';
const START_CURSOR  = process.env.START_CURSOR ? Number(process.env.START_CURSOR) : undefined;
/* ------------------------------------------------------------------------ */

// Use libpq env vars set in Cloud Run Job (PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT)
const pool = new Pool({
  statement_timeout: 30_000,
  query_timeout: 30_000,
  max: 10,
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withExpoRetry(fn, label, maxRetries = 3, startDelay = 500) {
  let delay = startDelay;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try { return await fn(); }
    catch (e) {
      if (attempt === maxRetries) throw e;
      console.warn(`⚠️  ${label} failed (attempt ${attempt}/${maxRetries}): ${e.message}`);
      await sleep(delay);
      delay = Math.min(delay * 2, 16_000);
    }
  }
}

/* -------------------------- JSONB safety helpers ------------------------- */
function nz(x) { return x === undefined ? null : x; }

function parsePgArrayLiteral(s) {
  // s like: {a,b,"c,d",NULL}
  const inner = s.slice(1, -1); // remove outer {}
  const out = [];
  let cur = '';
  let inQuotes = false;
  let esc = false;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (esc) { cur += ch; esc = false; continue; }
    if (ch === '\\') { cur += ch; esc = true; continue; }
    if (ch === '"') { inQuotes = !inQuotes; cur += ch; continue; }
    if (ch === ',' && !inQuotes) { out.push(cur.trim()); cur = ''; continue; }
    cur += ch;
  }
  if (cur.length) out.push(cur.trim());

  return out.map(p => {
    if (p === '' || p.toUpperCase() === 'NULL') return null;
    if (p.startsWith('"') && p.endsWith('"')) {
      // unquote via JSON.parse to handle escapes correctly
      try { return JSON.parse(p); } catch { return p.slice(1, -1); }
    }
    return p;
  });
}

function asJsonbArray(x, fallback = []) {
  if (x == null) return fallback;
  if (Array.isArray(x)) return x;

  if (typeof x === 'string') {
    const s = x.trim();
    if (!s) return fallback;

    // Already valid JSON?
    try {
      const v = JSON.parse(s);
      return Array.isArray(v) ? v : [v];
    } catch { /* not JSON string */ }

    // Postgres array-literal?  { ... }
    if (s.startsWith('{') && s.endsWith('}')) {
      return parsePgArrayLiteral(s);
    }

    // Fallback: single string → [string]
    return [s];
  }

  // numbers/booleans/objects -> wrap
  return [x];
}

function asJsonbObject(x, fallback = {}) {
  if (x == null) return fallback;
  if (typeof x === 'object' && !Array.isArray(x)) return x;
  if (typeof x === 'string') {
    const s = x.trim();
    if (!s) return fallback;
    try {
      const v = JSON.parse(s);
      return (v && typeof v === 'object' && !Array.isArray(v)) ? v : { value: v };
    } catch { return { value: s }; }
  }
  return { value: x };
}


function asJsonbObject(x, fallback = {}) {
  if (x == null) return fallback;
  if (typeof x === 'object' && !Array.isArray(x)) return x;
  if (typeof x === 'string') {
    const s = x.trim();
    if (!s) return fallback;
    try {
      const v = JSON.parse(s);
      return (v && typeof v === 'object' && !Array.isArray(v)) ? v : { value: v };
    } catch { return { value: s }; }
  }
  return { value: x };
}

/* ----------------------------- schema setup ----------------------------- */
const CREATE_BASE_TABLES_SQL = `
create table if not exists jobs (
  id                bigint primary key,
  title             text,
  organization      text,
  organization_url  text,
  organization_logo text,
  date_posted       timestamptz,
  date_created      timestamptz,
  date_validthrough timestamptz,

  locations_raw     text,
  locations_alt_raw jsonb default '[]',
  locations_derived jsonb default '[]',
  location_type     text,
  location_requirements_raw text,
  salary_raw        text,
  employment_type   jsonb default '[]',

  url               text,
  source            text,
  source_type       text,
  source_domain     text,

  description_text  text,
  description_html  text,

  cities_derived    jsonb default '[]',
  regions_derived   jsonb default '[]',
  countries_derived jsonb default '[]',
  timezones_derived jsonb default '[]',
  lats_derived      jsonb default '[]',
  lngs_derived      jsonb default '[]',
  remote_derived    boolean,
  domain_derived    text,

  li_payload        jsonb default '{}'::jsonb,
  ai_payload        jsonb default '{}'::jsonb,
  body_hash         text,
  updated_at        timestamptz not null default now()
);

-- keep cursor here so the job can resume between runs
create table if not exists sync_state (
  job_key text primary key,
  last_id bigint not null
);

create index if not exists idx_jobs_date_posted on jobs(date_posted desc);
`;

const ADD_MISSING_COLUMNS_SQL = `
-- If you used an older 33-col schema, these keep you moving without a separate migration step
alter table jobs
  add column if not exists locations_alt_raw jsonb default '[]',
  add column if not exists locations_derived  jsonb default '[]',
  add column if not exists location_requirements_raw text,
  add column if not exists employment_type    jsonb default '[]',
  add column if not exists cities_derived     jsonb default '[]',
  add column if not exists regions_derived    jsonb default '[]',
  add column if not exists countries_derived  jsonb default '[]',
  add column if not exists timezones_derived  jsonb default '[]',
  add column if not exists lats_derived       jsonb default '[]',
  add column if not exists lngs_derived       jsonb default '[]',
  add column if not exists li_payload         jsonb default '{}'::jsonb,
  add column if not exists ai_payload         jsonb default '{}'::jsonb,
  add column if not exists body_hash          text,
  add column if not exists updated_at         timestamptz not null default now();
`;

async function ensureSchema(client) {
  await client.query(CREATE_BASE_TABLES_SQL);
  await client.query(ADD_MISSING_COLUMNS_SQL);
}

/* ----------------------------- transform row ---------------------------- */
function toRow(job) {
  const li_payload = asJsonbObject({
    linkedin_org_employees: nz(job.linkedin_org_employees),
    linkedin_org_url: nz(job.linkedin_org_url),
    linkedin_org_size: nz(job.linkedin_org_size),
    linkedin_org_slogan: nz(job.linkedin_org_slogan),
    linkedin_org_industry: nz(job.linkedin_org_industry),
    linkedin_org_followers: nz(job.linkedin_org_followers),
    linkedin_org_headquarters: nz(job.linkedin_org_headquarters),
    linkedin_org_type: nz(job.linkedin_org_type),
    linkedin_org_foundeddate: nz(job.linkedin_org_foundeddate),
    linkedin_org_specialties: asJsonbArray(job.linkedin_org_specialties),
    linkedin_org_locations: asJsonbArray(job.linkedin_org_locations),
    linkedin_org_description: nz(job.linkedin_org_description),
    linkedin_org_recruitment_agency_derived: nz(job.linkedin_org_recruitment_agency_derived),
    linkedin_org_recruitment_agency_derived_2: nz(job.linkedin_org_recruitment_agency_derived_2),
    linkedin_org_slug: nz(job.linkedin_org_slug),
    seniority: nz(job.seniority),
    directapply: nz(job.directapply),
    recruiter_name: nz(job.recruiter_name),
    recruiter_title: nz(job.recruiter_title),
    recruiter_url: nz(job.recruiter_url),
    external_apply_url: nz(job.external_apply_url),
    no_jb_schema: nz(job.no_jb_schema),
  });

  const ai_payload = asJsonbObject({
    ai_salary_currency: nz(job.ai_salary_currency),
    ai_salary_value: nz(job.ai_salary_value),
    ai_salary_minvalue: nz(job.ai_salary_minvalue),
    ai_salary_maxvalue: nz(job.ai_salary_maxvalue),
    ai_salary_unittext: nz(job.ai_salary_unittext),
    ai_benefits: asJsonbArray(job.ai_benefits),
    ai_experience_level: nz(job.ai_experience_level),
    ai_work_arrangement: nz(job.ai_work_arrangement),
    ai_work_arrangement_office_days: nz(job.ai_work_arrangement_office_days),
    ai_remote_location: asJsonbArray(job.ai_remote_location),
    ai_remote_location_derived: asJsonbArray(job.ai_remote_location_derived),
    ai_key_skills: asJsonbArray(job.ai_key_skills),
    ai_hiring_manager_name: nz(job.ai_hiring_manager_name),
    ai_hiring_manager_email_address: nz(job.ai_hiring_manager_email_address),
    ai_core_responsibilities: nz(job.ai_core_responsibilities),
    ai_requirements_summary: nz(job.ai_requirements_summary),
    ai_working_hours: nz(job.ai_working_hours),
    ai_employment_type: asJsonbArray(job.ai_employment_type),
    ai_job_language: nz(job.ai_job_language),
    ai_visa_sponsorship: nz(job.ai_visa_sponsorship),
  });

  const body = `${job.description_html || ''}|${job.description_text || ''}`;
  const body_hash = crypto.createHash('sha256').update(body).digest('hex');

  return {
    id: job.id,
    title: nz(job.title),
    organization: nz(job.organization),
    organization_url: nz(job.organization_url),
    organization_logo: nz(job.organization_logo),
    date_posted: job.date_posted ? new Date(job.date_posted) : null,
    date_created: job.date_created ? new Date(job.date_created) : null,
    date_validthrough: job.date_validthrough ? new Date(job.date_validthrough) : null,

    locations_raw: nz(job.locations_raw),
    locations_alt_raw: asJsonbArray(job.locations_alt_raw),
    locations_derived: asJsonbArray(job.locations_derived),
    location_type: nz(job.location_type),
    location_requirements_raw: nz(job.location_requirements_raw),
    salary_raw: nz(job.salary_raw),
    employment_type: asJsonbArray(job.employment_type),

    url: nz(job.url),
    source: nz(job.source),
    source_type: nz(job.source_type),
    source_domain: nz(job.source_domain),

    description_text: nz(job.description_text),
    description_html: nz(job.description_html),

    cities_derived: asJsonbArray(job.cities_derived),
    regions_derived: asJsonbArray(job.regions_derived),
    countries_derived: asJsonbArray(job.countries_derived),
    timezones_derived: asJsonbArray(job.timezones_derived),
    lats_derived: asJsonbArray(job.lats_derived),
    lngs_derived: asJsonbArray(job.lngs_derived),
    remote_derived: !!job.remote_derived,
    domain_derived: nz(job.domain_derived),

    li_payload,
    ai_payload,
    body_hash
  };
}

/* -------------------------- cursor read / write ------------------------- */
async function getResumeCursor(client) {
  if (START_CURSOR) {
    console.log(`🧭 Using START_CURSOR=${START_CURSOR}`);
    return START_CURSOR;
  }
  const s = await client.query('select last_id from sync_state where job_key = $1 limit 1', [JOB_KEY]);
  if (s.rows.length) {
    const c = Number(s.rows[0].last_id);
    console.log(`🧭 Resuming from sync_state: ${c}`);
    return c;
  }
  const m = await client.query('select max(id) as max_id from jobs');
  const c = m.rows[0].max_id ? Number(m.rows[0].max_id) : 1;
  console.log(`🧭 Resuming from jobs max(id): ${c}`);
  return c;
}

async function saveCursor(client, cursor) {
  await client.query(`
    insert into sync_state(job_key, last_id)
    values ($1, $2)
    on conflict (job_key) do update set last_id = excluded.last_id
  `, [JOB_KEY, cursor]);
}

/* ------------------------------ upsert batch ---------------------------- */
async function upsertBatch(client, rows) {
  if (!rows.length) return;

    const jsonbArrayFields = [
    'locations_alt_raw','locations_derived','employment_type',
    'cities_derived','regions_derived','countries_derived',
    'timezones_derived','lats_derived','lngs_derived'
  ];

  for (const r of rows) {
    for (const f of jsonbArrayFields) {
      r[f] = asJsonbArray(r[f]);   // <- guarantees proper JSON arrays
    }
    r.li_payload = asJsonbObject(r.li_payload); // <- guarantees proper JSON object
    r.ai_payload = asJsonbObject(r.ai_payload);
  }

  // (optional but helpful) preflight to pinpoint bad shapes early
  for (const r of rows) {
    try {
      jsonbArrayFields.forEach(f => JSON.stringify(r[f]));
      JSON.stringify(r.li_payload);
      JSON.stringify(r.ai_payload);
    } catch (e) {
      console.error('🚨 malformed jsonb before insert for id=', r.id, e.message);
      throw e;
    }
  }
  
  const cols = [
    'id','title','organization','organization_url','organization_logo',
    'date_posted','date_created','date_validthrough',
    'locations_raw','locations_alt_raw','locations_derived','location_type','location_requirements_raw','salary_raw','employment_type',
    'url','source','source_type','source_domain',
    'description_text','description_html',
    'cities_derived','regions_derived','countries_derived','timezones_derived','lats_derived','lngs_derived',
    'remote_derived','domain_derived',
    'li_payload','ai_payload','body_hash'
  ];

  const values = [];
  const placeholders = rows.map((r, i) => {
    const base = i * cols.length;
    values.push(
      r.id, r.title, r.organization, r.organization_url, r.organization_logo,
      r.date_posted, r.date_created, r.date_validthrough,
      r.locations_raw, r.locations_alt_raw, r.locations_derived, r.location_type, r.location_requirements_raw, r.salary_raw, r.employment_type,
      r.url, r.source, r.source_type, r.source_domain,
      r.description_text, r.description_html,
      r.cities_derived, r.regions_derived, r.countries_derived, r.timezones_derived, r.lats_derived, r.lngs_derived,
      r.remote_derived, r.domain_derived,
      r.li_payload, r.ai_payload, r.body_hash
    );
    const idx = Array.from({ length: cols.length }, (_, k) => `$${base + k + 1}`);
    return `(${idx.join(',')})`;
  }).join(',');

  const setList = `
    title = EXCLUDED.title,
    organization = EXCLUDED.organization,
    organization_url = EXCLUDED.organization_url,
    organization_logo = EXCLUDED.organization_logo,
    date_posted = EXCLUDED.date_posted,
    date_created = EXCLUDED.date_created,
    date_validthrough = EXCLUDED.date_validthrough,
    locations_raw = EXCLUDED.locations_raw,
    locations_alt_raw = EXCLUDED.locations_alt_raw,
    locations_derived = EXCLUDED.locations_derived,
    location_type = EXCLUDED.location_type,
    location_requirements_raw = EXCLUDED.location_requirements_raw,
    salary_raw = EXCLUDED.salary_raw,
    employment_type = EXCLUDED.employment_type,
    url = EXCLUDED.url,
    source = EXCLUDED.source,
    source_type = EXCLUDED.source_type,
    source_domain = EXCLUDED.source_domain,
    description_text = EXCLUDED.description_text,
    description_html = EXCLUDED.description_html,
    cities_derived = EXCLUDED.cities_derived,
    regions_derived = EXCLUDED.regions_derived,
    countries_derived = EXCLUDED.countries_derived,
    timezones_derived = EXCLUDED.timezones_derived,
    lats_derived = EXCLUDED.lats_derived,
    lngs_derived = EXCLUDED.lngs_derived,
    remote_derived = EXCLUDED.remote_derived,
    domain_derived = EXCLUDED.domain_derived,
    li_payload = EXCLUDED.li_payload,
    ai_payload = EXCLUDED.ai_payload,
    body_hash = EXCLUDED.body_hash,
    updated_at = now()
  `;

  const sql = `
    insert into jobs (${cols.join(',')})
    values ${placeholders}
    on conflict (id) do update
      set ${setList}
      where jobs.body_hash is distinct from EXCLUDED.body_hash
  `;

  try {
    await client.query(sql, values);
  } catch (e) {
    // help identify the offending shape fast
    console.error('❗ Upsert failed; sample row:', rows[0]);
    throw e;
  }
}

/* ---------------------------------- run --------------------------------- */
async function run() {
  const apiKey = process.env.FANTASTIC_JOBS_API_KEY;
  if (!apiKey) throw new Error('Missing FANTASTIC_JOBS_API_KEY');

  const client = await pool.connect();
  try {
    await client.query(`set statement_timeout = '30s'`);
    await ensureSchema(client);

    let cursor = await getResumeCursor(client);
    let totalFetched = 0;
    let totalProcessed = 0;

    while (true) {
      const url = `${JOBS_ENDPOINT}?description_type=text&include_description=true&include_li=true&include_ai=true&limit=${API_LIMIT}&cursor=${cursor}`;

      const jobs = await withExpoRetry(async () => {
        const res = await fetch(url, {
          headers: { Accept: 'application/json', 'User-Agent': 'JobSync/1.0', 'X-API-Key': apiKey }
        });
        if (!res.ok) {
          const body = await res.text().catch(() => '');
          throw new Error(`API ${res.status} ${res.statusText}: ${body}`);
        }
        return res.json();
      }, 'fetch page', 4, 1000);

      if (!Array.isArray(jobs) || jobs.length === 0) {
        console.log('✅ No more jobs; finished paging.');
        break;
      }

      totalFetched += jobs.length;

      // transform + intra-page dedupe
      const mapped = jobs.map(toRow);
      const seen = new Set();
      const rows = mapped.filter(r => r.id && !seen.has(r.id) && seen.add(r.id));
      if (rows.length !== mapped.length) {
        console.log(`🧹 Deduped ${mapped.length - rows.length} intra-page duplicates`);
      }

      // chunked upsert with retries
      for (let i = 0; i < rows.length; i += BATCH) {
        const chunk = rows.slice(i, i + BATCH);
        await withExpoRetry(() => upsertBatch(client, chunk), `upsert ${chunk.length}`, 3, 500);
        totalProcessed += chunk.length;
      }

      // advance cursor & persist
      cursor = jobs[jobs.length - 1].id;
      await saveCursor(client, cursor);
      console.log(`✅ Page done. nextCursor=${cursor} | totals fetched=${totalFetched}, processed=${totalProcessed}`);

      if (jobs.length < API_LIMIT) {
        console.log('🎉 Final page fetched for this run.');
        break;
      }
      await sleep(PAGE_PAUSE_MS);
    }

    console.log(`🎉 Sync complete. fetched=${totalFetched}, processed=${totalProcessed}`);
  } finally {
    client.release();
    await pool.end();
  }
}

// Entry (Cloud Run Job)
if (require.main === module) {
  run().then(() => process.exit(0)).catch(e => {
    console.error('💥 Fatal:', e);
    process.exit(1);
  });
}

module.exports = { run };
