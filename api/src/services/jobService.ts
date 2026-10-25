import { CronExpressionParser } from 'cron-parser';
import { pool } from '../db';

export interface Job {
  id: string;
  name: string;
  cron_expr: string;
  timezone: string;
  payload: Record<string, unknown>;
  priority: number;
  max_retries: number;
  timeout_seconds: number;
  enabled: boolean;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface CreateJobInput {
  name: string;
  cron_expr: string;
  timezone?: string;
  payload: Record<string, unknown>;
  priority?: number;
  max_retries?: number;
  timeout_seconds?: number;
}

export interface ListJobsQuery {
  limit?: number;
  offset?: number;
  enabled?: boolean;
}

export class ValidationError extends Error {
  statusCode = 400;
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class NotFoundError extends Error {
  statusCode = 404;
  constructor(message: string) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class ConflictError extends Error {
  statusCode = 409;
  constructor(message: string) {
    super(message);
    this.name = 'ConflictError';
  }
}

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function validateUUID(id: string): void {
  if (!UUID_REGEX.test(id)) {
    throw new ValidationError(`Invalid job ID format: ${id}`);
  }
}

export function isValidTimezone(tz: string): boolean {
  try {
    Intl.DateTimeFormat(undefined, { timeZone: tz });
    return true;
  } catch {
    return false;
  }
}

export function computeNextRun(expr: string, timezone: string = 'UTC', fromDate: Date = new Date()): Date {
  if (!isValidTimezone(timezone)) {
    throw new ValidationError(`Invalid IANA timezone: ${timezone}`);
  }
  try {
    const interval = CronExpressionParser.parse(expr, { tz: timezone, currentDate: fromDate });
    return interval.next().toDate();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new ValidationError(`Invalid cron expression: ${msg}`);
  }
}

export async function createJob(input: CreateJobInput): Promise<Job> {
  if (!input.name || typeof input.name !== 'string' || input.name.trim() === '') {
    throw new ValidationError('Job name is required');
  }
  if (!input.cron_expr || typeof input.cron_expr !== 'string' || input.cron_expr.trim() === '') {
    throw new ValidationError('cron_expr is required');
  }
  if (input.payload === undefined || input.payload === null || typeof input.payload !== 'object' || Array.isArray(input.payload)) {
    throw new ValidationError('payload must be a valid JSON object');
  }

  const timezone = input.timezone?.trim() || 'UTC';
  const priority = input.priority !== undefined ? Number(input.priority) : 5;
  const maxRetries = input.max_retries !== undefined ? Number(input.max_retries) : 3;
  const timeoutSeconds = input.timeout_seconds !== undefined ? Number(input.timeout_seconds) : 60;

  if (Number.isNaN(priority) || priority < 0 || priority > 100) {
    throw new ValidationError('priority must be an integer between 0 and 100');
  }
  if (Number.isNaN(maxRetries) || maxRetries < 0) {
    throw new ValidationError('max_retries must be a non-negative integer');
  }
  if (Number.isNaN(timeoutSeconds) || timeoutSeconds <= 0) {
    throw new ValidationError('timeout_seconds must be a positive integer');
  }

  const nextRunAt = computeNextRun(input.cron_expr, timezone, new Date());

  try {
    const result = await pool.query<Job>(
      `INSERT INTO jobs (name, cron_expr, timezone, payload, priority, max_retries, timeout_seconds, next_run_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       RETURNING *`,
      [
        input.name.trim(),
        input.cron_expr.trim(),
        timezone,
        JSON.stringify(input.payload),
        priority,
        maxRetries,
        timeoutSeconds,
        nextRunAt.toISOString(),
      ]
    );

    const createdJob = result.rows[0];
    if (!createdJob) {
      throw new Error('Database insert succeeded but returned no row');
    }
    return createdJob;
  } catch (err: any) {
    if (err.code === '23505') {
      throw new ConflictError(`Job with name "${input.name.trim()}" already exists`);
    }
    throw err;
  }
}

export async function listJobs(query: ListJobsQuery): Promise<{ data: Job[]; total: number }> {
  const limit = Math.min(Math.max(Number(query.limit) || 20, 1), 100);
  const offset = Math.max(Number(query.offset) || 0, 0);
  const enabled = query.enabled !== undefined ? query.enabled : null;

  const countResult = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM jobs
     WHERE deleted_at IS NULL
       AND ($1::boolean IS NULL OR enabled = $1)`,
    [enabled]
  );
  const total = parseInt(countResult.rows[0]?.count || '0', 10);

  const result = await pool.query<Job>(
    `SELECT * FROM jobs
     WHERE deleted_at IS NULL
       AND ($1::boolean IS NULL OR enabled = $1)
     ORDER BY created_at DESC
     LIMIT $2 OFFSET $3`,
    [enabled, limit, offset]
  );

  return { data: result.rows, total };
}

export async function getJobById(id: string): Promise<Job> {
  validateUUID(id);

  const result = await pool.query<Job>(
    `SELECT * FROM jobs
     WHERE id = $1 AND deleted_at IS NULL`,
    [id]
  );

  const job = result.rows[0];
  if (!job) {
    throw new NotFoundError(`Job not found with id ${id}`);
  }

  return job;
}

export async function pauseJob(id: string): Promise<Job> {
  validateUUID(id);

  const result = await pool.query<Job>(
    `UPDATE jobs
     SET enabled = false, updated_at = now()
     WHERE id = $1 AND deleted_at IS NULL
     RETURNING *`,
    [id]
  );

  const job = result.rows[0];
  if (!job) {
    throw new NotFoundError(`Job not found with id ${id}`);
  }

  return job;
}

export async function resumeJob(id: string): Promise<Job> {
  validateUUID(id);

  const existing = await getJobById(id);
  const nextRunAt = computeNextRun(existing.cron_expr, existing.timezone, new Date());

  const result = await pool.query<Job>(
    `UPDATE jobs
     SET enabled = true, next_run_at = $2, updated_at = now()
     WHERE id = $1 AND deleted_at IS NULL
     RETURNING *`,
    [id, nextRunAt.toISOString()]
  );

  const job = result.rows[0];
  if (!job) {
    throw new NotFoundError(`Job not found with id ${id}`);
  }

  return job;
}

export async function deleteJob(id: string): Promise<{ id: string }> {
  validateUUID(id);

  const result = await pool.query(
    `UPDATE jobs
     SET deleted_at = now(), enabled = false, updated_at = now()
     WHERE id = $1 AND deleted_at IS NULL
     RETURNING id`,
    [id]
  );

  if (result.rowCount === 0) {
    throw new NotFoundError(`Job not found with id ${id}`);
  }

  return { id };
}
