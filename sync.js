// sync.js — simplified: let libpq env vars drive the connection
const { Pool } = require('pg');
const fetch = require('node-fetch');
const crypto = require('crypto');

/* ----------------------- knobs you can tune via env ----------------------- */
const JOBS_ENDPOINT   = process.env.JOBS_ENDPOINT || 'https://api.fantastic.jobs/active-ats-6m';
const API_LIMIT       = Number(process.env.PAGE_LIMIT ?? 200);  // API page size
const BATCH           = Number(process.env.BATCH ?? 50);        // per-UPSERT rows
const PAGE_PAUSE_MS   = Number(process.env.PAGE_PAUSE_MS ?? 100);
const JOB_KEY         = process.env.JOB_KEY || '6m_backfill';
const START_CURSOR    = process.env.START_CURSOR ? Number(process.env.START_CURSOR) : undefined;
/* ------------------------------------------------------------------------- */

// no INSTANCE or host in code:
const pool = new Pool({
  statement_timeout: 30000,
  query_timeout: 30000,
  max: 10,
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withExpoRetry(fn, label, maxRetries = 4, startDelay = 1000) {
  let delay = startDelay;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (attempt === maxRetries) throw e;
      console.warn(`⚠️  ${label} failed (attempt ${attempt}/${maxRetries}): ${e.message}`);
      await sleep(delay);
      delay = Math.min(delay * 2, 16000);
    }
  }
}

function toRow(job) {
  const li_payload = {
    linkedin_org_employees: job.linkedin_org_employees,
    linkedin_org_url: job.linkedin_org_url,
    linkedin_org_size: job.linkedin_org_size,
    linkedin_org_slogan: job.linkedin_org_slogan,
    linkedin_org_industry: job.linkedin_org_industry,
    linkedin_org_followers: job.linkedin_org_followers,
    linkedin_org_headquarters: job.linkedin_org_headquarters,
    linkedin_org_type: job.linkedin_org_type,
    linkedin_org_foundeddate: job.linkedin_org_foundeddate,
    linkedin_org_specialties: job.linkedin_org_specialties,
    linkedin_org_locations: job.linkedin_org_locations,
    linkedin_org_description: job.linkedin_org_description,
    linkedin_org_recruitment_agency_derived: job.linkedin_org_recruitment_agency_derived,
    linkedin_org_recruitment_agency_derived_2: job.linkedin_org_recruitment_agency_derived_2,
    linkedin_org_slug: job.linkedin_org_slug,
    seniority: job.seniority,
    directapply: job.directapply,
    recruiter_name: job.recruiter_name,
    recruiter_title: job.recruiter_title,
    recruiter_url: job.recruiter_url,
    external_apply_url: job.external_apply_url,
    no_jb_schema: job.no_jb_schema
  };
  const ai_payload = {
    ai_salary_currency: job.ai_salary_currency,
    ai_salary_value: job.ai_salary_value,
    ai_salary_minvalue: job.ai_salary_minvalue,
    ai_salary_maxvalue: job.ai_salary_maxvalue,
    ai_salary_unittext: job.ai_salary_unittext,
    ai_benefits: job.ai_benefits,
    ai_experience_level: job.ai_experience_level,
    ai_work_arrangement: job.ai_work_arrangement,
    ai_work_arrangement_office_days: job.ai_work_arrangement_office_days,
    ai_remote_location: job.ai_remote_location,
    ai_remote_location_derived: job.ai_remote_location_derived,
    ai_key_skills: job.ai_key_skills,
    ai_hiring_manager_name: job.ai_hiring_manager_name,
    ai_hiring_manager_email_address: job.ai_hiring_manager_email_address,
    ai_core_responsibilities: job.ai_core_responsibilities,
    ai_requirements_summary: job.ai_requirements_summary,
    ai_working_hours: job.ai_working_hours,
    ai_employment_type: job.ai_employment_type,
    ai_job_language: job.ai_job_language,
    ai_visa_sponsorship: job.ai_visa_sponsorship
  };

  const body = `${job.description_html || ''}|${job.description_text || ''}`;
  const body_hash = crypto.createHash('sha256').update(body).digest('hex');

  return {
    id: job.id,
    title: job.title || null,
    organization: job.organization || null,
    organization_url: job.organization_url || null,
    organization_logo: job.organization_logo || null,
    date_posted: job.date_posted ? new Date(job.date_posted) : null,
    date_created: job.date_created ? new Date(job.date_created) : null,
    date_validthrough: job.date_validthrough ? new Date(job.date_validthrough) : null,

    locations_raw: job.locations_raw || null,
    locations_alt_raw: job.locations_alt_raw || [],
    locations_derived: job.locations_derived || [],
    location_type: job.location_type || null,
    location_requirements_raw: job.location_requirements_raw || null,
    salary_raw: job.salary_raw || null,
    employment_type: job.employment_type || [],

    url: job.url || null,
    source: job.source || null,
    source_type: job.source_type || null,
    source_domain: job.source_domain || null,

    description_text: job.description_text || null,
    description_html: job.description_html || null,

    cities_derived: job.cities_derived || [],
    regions_derived: job.regions_derived || [],
    countries_derived: job.countries_derived || [],
    timezones_derived: job.timezones_derived || [],
    lats_derived: job.lats_derived || [],
    lngs_derived: job.lngs_derived || [],
    remote_derived: !!job.remote_derived,
    domain_derived: job.domain_derived || null,

    li_payload,
    ai_payload,
    body_hash
  };
}

async function getResumeCursor(client) {
  if (START_CURSOR) {
    console.log(`🧭 Using START_CURSOR=${START_CURSOR}`);
    return START_CURSOR;
  }

  // Try sync_state first
  const q1 = await client.query(
    'select last_id from sync_state where job_key = $1 limit 1',
    [JOB_KEY]
  );
  if (q1.rows.length) {
    const c = Number(q1.rows[0].last_id);
    console.log(`🧭 Resuming from sync_state: ${c}`);
    return c;
  }

  // Fallback to max(id) in jobs
  const q2 = await client.query('select max(id) as max_id from jobs');
  const c = q2.rows[0].max_id ? Number(q2.rows[0].max_id) : 1;
  console.log(`🧭 Resuming from jobs max(id): ${c}`);
  return c;
}

async function saveCursor(client, cursor) {
  await client.query(
    `insert into sync_state(job_key, last_id)
     values ($1, $2)
     on conflict (job_key) do update set last_id = excluded.last_id`,
    [JOB_KEY, cursor]
  );
}

async function upsertBatch(client, rows) {
  if (!rows.length) return;

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
    const idxs = Array.from({ length: cols.length }, (_, k) => `$${base + k + 1}`);
    return `(${idxs.join(',')})`;
  }).join(',');

  // Conditional update to skip unchanged rows
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

  await client.query(sql, values);
}

async function run() {
  const apiKey = process.env.FANTASTIC_JOBS_API_KEY;
  if (!apiKey) throw new Error('Missing FANTASTIC_JOBS_API_KEY');

  const client = await pool.connect();
  try {
    await client.query(`set statement_timeout = '30s'`);

    // ensure tables exist (idempotent)
    await client.query(`
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
        li_payload        jsonb,
        ai_payload        jsonb,
        body_hash         text,
        updated_at        timestamptz not null default now()
      );
      create index if not exists idx_jobs_date_posted on jobs(date_posted desc);
      create table if not exists sync_state (
        job_key text primary key,
        last_id bigint not null
      );
    `);

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

      // upsert in small chunks
      for (let i = 0; i < rows.length; i += BATCH) {
        const chunk = rows.slice(i, i + BATCH);
        await withExpoRetry(() => upsertBatch(client, chunk), `upsert ${chunk.length}`, 3, 500);
        totalProcessed += chunk.length;
      }

      // advance cursor and persist atomically
      cursor = jobs[jobs.length - 1].id;
      await saveCursor(client, cursor);
      console.log(`✅ Page done. nextCursor=${cursor} | totals fetched=${totalFetched}, processed=${totalProcessed}`);

      // small breather
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

// Entry for Cloud Run Job
if (require.main === module) {
  run().then(() => process.exit(0)).catch((e) => {
    console.error('💥 Fatal:', e);
    process.exit(1);
  });
}

module.exports = { run };
