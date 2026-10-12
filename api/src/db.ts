import { Pool } from 'pg';
import { config } from './config';

const pool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  user: config.postgres.user,
  password: config.postgres.password,
  database: config.postgres.database,
  ssl: config.postgres.ssl ? { rejectUnauthorized: false } : false,
  connectionTimeoutMillis: 3000,
  idleTimeoutMillis: 10000,
  max: 10,
});

// Suppress unhandled error crashes on idle clients
pool.on('error', (err) => {
  console.error(JSON.stringify({ level: 'error', message: 'Unexpected error on idle DB client', error: err.message }));
});


async function checkDbHealth(timeoutMs: number = 2000): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  try {
    const timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('Database health check timed out')), timeoutMs);
    });

    await Promise.race([
      pool.query('SELECT 1'),
      timeoutPromise,
    ]);

    return true;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(JSON.stringify({ level: 'error', message: 'Database health check failed', error: message }));
    return false;
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function closeDb(): Promise<void> {
  await pool.end();
}

export {closeDb,checkDbHealth,pool};